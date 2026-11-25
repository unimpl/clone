package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	openviking "github.com/volcengine/OpenViking/sdk/go"
)

const defaultOpenVikingEndpoint = "https://api.vikingdb.cn-beijing.volces.com/openviking"

type openVikingConfig struct {
	Endpoint string   `json:"endpoint"`
	APIKey   string   `json:"api_key"`
	AgentID  string   `json:"agent_id"`
	GroupIDs []string `json:"group_ids"`
}

type watchArchive struct {
	client   *openviking.Client
	db       *sql.DB
	lock     *os.File
	appID    string
	endpoint string
	excluded map[string]bool
	groups   map[string]bool
	source   atomic.Value
	cancel   context.CancelFunc
	done     chan struct{}
}

func loadOpenVikingConfig(dir string) (openVikingConfig, map[string]bool, error) {
	path := filepath.Join(dir, "openviking.json")
	var config openVikingConfig
	err := readPrivateJSON(path, &config)
	legacy := errors.Is(err, os.ErrNotExist)
	if err != nil && !legacy {
		return config, nil, fmt.Errorf("读取 OpenViking 配置 %s: %w", path, err)
	}
	if key := strings.TrimSpace(os.Getenv("OPENVIKING_API_KEY")); key != "" {
		config.APIKey = key
	}
	if agentID := strings.TrimSpace(os.Getenv("OPENVIKING_AGENT_ID")); agentID != "" {
		config.AgentID = agentID
	}
	config.APIKey = strings.TrimSpace(config.APIKey)
	config.AgentID = strings.TrimSpace(config.AgentID)
	if config.APIKey == "" {
		if legacy {
			return config, nil, nil
		}
		return config, nil, errors.New("openviking.json 缺少 api_key")
	}
	groupFile := "openviking.json 的 group_ids"
	groups := make(map[string]bool, len(config.GroupIDs))
	if legacy {
		config.Endpoint = defaultOpenVikingEndpoint
		groupFile = "openviking-groups.txt"
		groups, err = loadWatchChatIDs(dir, groupFile)
		if err != nil {
			return config, nil, err
		}
	} else {
		for _, id := range config.GroupIDs {
			groups[strings.TrimSpace(id)] = true
		}
	}
	config.Endpoint = strings.TrimRight(strings.TrimSpace(config.Endpoint), "/")
	endpoint, err := url.Parse(config.Endpoint)
	if err != nil || (endpoint.Scheme != "https" && endpoint.Scheme != "http") ||
		endpoint.Hostname() == "" || endpoint.User != nil || endpoint.ForceQuery || endpoint.RawQuery != "" ||
		endpoint.Fragment != "" {
		return config, nil, errors.New("openviking.json 的 endpoint 需要完整的 HTTP(S) 基础地址")
	}
	config.Endpoint = endpoint.String()
	for id := range groups {
		if !strings.HasPrefix(id, "oc_") || len(id) <= len("oc_") || strings.ContainsAny(id, ",， \t\r\n") {
			return config, nil, fmt.Errorf("%s 需要填入 oc_ 开头的群聊 ID", groupFile)
		}
	}
	return config, groups, nil
}

func newWatchArchive(ctx context.Context, dir, appID string, excluded map[string]bool) (*watchArchive, error) {
	config, groups, err := loadOpenVikingConfig(dir)
	if err != nil || config.APIKey == "" {
		return nil, err
	}
	client, err := newOpenVikingClient(config.Endpoint, config.APIKey, config.AgentID)
	if err != nil {
		return nil, err
	}
	db, lock, err := openArchiveDB(dir)
	if err != nil {
		client.CloseIdleConnections()
		return nil, fmt.Errorf("打开 OpenViking 入库队列: %w", err)
	}
	worker, cancel := context.WithCancel(ctx)
	archive := &watchArchive{
		client: client, db: db, lock: lock, appID: appID, endpoint: config.Endpoint,
		excluded: excluded, groups: groups, cancel: cancel, done: make(chan struct{}),
	}
	go archive.upload(worker)
	fmt.Fprintf(os.Stderr, "OpenViking 入库已启用，包含私聊和名单中的 %d 个群聊。\n", len(groups))
	return archive, nil
}

func newOpenVikingClient(baseURL, key, agentID string) (*openviking.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	headers := map[string]string{"Authorization": "Bearer " + key}
	if agentID != "" {
		headers["X-OpenViking-Agent"] = agentID
	}
	return openviking.NewClient(openviking.Config{
		BaseURL: baseURL, ExtraHeaders: headers,
		HTTPClient: &http.Client{
			Transport: transport, Timeout: 15 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	})
}

func (archive *watchArchive) includes(c chat) bool {
	return !archive.excluded[c.id] && (c.mode == "p2p" || archive.groups[c.id])
}

func stableFeishuID(prefix, value string) string {
	digest := sha256.Sum256([]byte(value))
	return prefix + hex.EncodeToString(digest[:16])
}

func (archive *watchArchive) setIdentity(identity watchIdentity) {
	source := archive.appID + "\x00" + identity.OpenID
	if archive.endpoint != "" && archive.endpoint != defaultOpenVikingEndpoint {
		source += "\x00" + archive.endpoint
	}
	archive.source.Store(stableFeishuID("", source))
}

func (archive *watchArchive) close() {
	archive.cancel()
	<-archive.done
	archive.client.CloseIdleConnections()
	archive.db.Close()
	archive.lock.Close()
}

func (archive *watchArchive) upload(ctx context.Context) {
	defer close(archive.done)
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	sessions := make(map[string]bool)
	var lastError string
	for ctx.Err() == nil {
		source, _ := archive.source.Load().(string)
		if source != "" {
			item, err := archive.nextDelivery(ctx, source)
			if err == nil && item != nil {
				err = archive.deliver(ctx, item, sessions)
				if err == nil {
					lastError = ""
					continue
				}
			}
			if err != nil && ctx.Err() == nil && lastError != err.Error() {
				lastError = err.Error()
				fmt.Fprintln(os.Stderr, "OpenViking 本地队列读写失败："+terminalText(lastError, 200))
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (archive *watchArchive) deliver(ctx context.Context, item *archiveDelivery, sessions map[string]bool) error {
	sessionID := item.sessionID()
	if item.state == "sending" {
		result, err := archive.client.GetSessionContext(ctx, sessionID, 0)
		if err == nil && hasArchiveMessage(result, item.messageID) {
			return archive.updateDelivery(ctx, item, "sent", 0)
		}
		return archive.deferDelivery(ctx, item, "sending", "写入结果待核对", err)
	}

	if !sessions[sessionID] {
		_, err := archive.client.CreateSession(ctx, &openviking.CreateSessionOptions{
			SessionID: sessionID, DisableAutoCommit: true,
		})
		if err != nil && !openviking.IsCode(err, "ALREADY_EXISTS") && openVikingStatus(err) != http.StatusConflict {
			return archive.deferDelivery(ctx, item, "pending", "创建会话失败", err)
		}
		sessions[sessionID] = true
	}
	if err := archive.updateDelivery(ctx, item, "sending", 0); err != nil {
		return err
	}
	_, err := archive.client.AddMessage(ctx, sessionID, "user", openviking.AddMessageOptions{
		Content: &item.content, PeerID: item.peerID,
		CreatedAt: time.UnixMilli(item.createdAt).UTC().Format(time.RFC3339Nano),
	})
	if err == nil {
		return archive.updateDelivery(ctx, item, "sent", 0)
	}
	status := openVikingStatus(err)
	switch {
	case status == http.StatusNotFound:
		delete(sessions, sessionID)
		return archive.deferDelivery(ctx, item, "pending", "写入会话不存在", err)
	case status >= 400 && status < 500 && status != http.StatusRequestTimeout && status != http.StatusConflict:
		return archive.deferDelivery(ctx, item, "pending", "追加消息被拒绝", err)
	default:
		return archive.deferDelivery(ctx, item, "sending", "写入结果待核对", err)
	}
}

func (archive *watchArchive) deferDelivery(ctx context.Context, item *archiveDelivery, state, reason string, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if status := openVikingStatus(err); status > 0 {
		fmt.Fprintf(os.Stderr, "OpenViking %s，消息 %s 已留在本地队列（HTTP %d）。\n", reason, item.messageID, status)
	} else if state != item.state || item.state != "sending" {
		fmt.Fprintf(os.Stderr, "OpenViking %s，消息 %s 已留在本地队列。\n", reason, item.messageID)
	}
	return archive.updateDelivery(ctx, item, state, time.Now().Add(time.Minute).UnixMilli())
}

func openVikingStatus(err error) int {
	var apiErr *openviking.Error
	if errors.As(err, &apiErr) {
		return apiErr.StatusCode
	}
	return 0
}

func hasArchiveMessage(result map[string]any, id string) bool {
	messages, _ := result["messages"].([]any)
	for _, value := range messages {
		message, _ := value.(map[string]any)
		if sourceID, ok := message["content"].(string); ok && isArchiveMessage(sourceID, id) {
			return true
		}
		parts, _ := message["parts"].([]any)
		for _, value := range parts {
			part, _ := value.(map[string]any)
			if content, ok := part["text"].(string); ok && isArchiveMessage(content, id) {
				return true
			}
		}
	}
	return false
}

func isArchiveMessage(content, id string) bool {
	var record struct {
		MessageID string `json:"feishu_message_id"`
	}
	return json.Unmarshal([]byte(content), &record) == nil && record.MessageID == id
}
