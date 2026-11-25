package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

const (
	watchPreferenceTTL   = 5 * time.Minute
	watchPreferenceRetry = time.Minute
)

type cachedMute struct {
	muted     bool
	known     bool
	refreshAt time.Time
}

type watchPreferences struct {
	excluded      map[string]bool
	pinned        []chat
	pinsRefreshAt time.Time
	mutes         map[string]cachedMute
	muteRetryAt   time.Time
	lastError     map[string]string
}

func newWatchPreferences(excluded map[string]bool) watchPreferences {
	return watchPreferences{
		excluded: excluded, mutes: make(map[string]cachedMute), lastError: make(map[string]string),
	}
}

func loadWatchExclusions(dir string) (map[string]bool, error) {
	return loadWatchChatIDs(dir, "watch-exclude.txt")
}

func loadWatchChatIDs(dir, filename string) (map[string]bool, error) {
	path := filepath.Join(dir, filename)
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("读取会话名单 %s: %w", path, err)
	}
	defer file.Close()
	ids := make(map[string]bool)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		value, _, _ := strings.Cut(strings.TrimPrefix(scanner.Text(), "\uFEFF"), "#")
		if id := strings.TrimSpace(value); id != "" {
			ids[id] = true
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("读取会话名单 %s: %w", path, err)
	}
	return ids, nil
}

func (prefs *watchPreferences) refreshPinned(ctx context.Context, client *imClient, now time.Time) {
	if now.Before(prefs.pinsRefreshAt) {
		return
	}
	ids, err := client.pinnedChatIDs(ctx)
	if err != nil {
		prefs.pinsRefreshAt = now.Add(watchPreferenceRetry)
		prefs.report(ctx, "置顶会话", err)
		return
	}
	prefs.report(ctx, "置顶会话", nil)
	prefs.pinsRefreshAt = now.Add(watchPreferenceTTL)
	var pinned []chat
	known := make(map[string]chat, len(prefs.pinned))
	for _, c := range prefs.pinned {
		known[c.id] = c
	}
	var lookup []string
	for _, id := range ids {
		if !prefs.excluded[id] {
			lookup = append(lookup, id)
		}
	}
	details, err := client.chatDetails(ctx, lookup)
	if err != nil {
		prefs.pinsRefreshAt = now.Add(watchPreferenceRetry)
	}
	prefs.report(ctx, "置顶会话详情", err)
	for _, id := range lookup {
		c, ok := details[id]
		if !ok {
			c = known[id]
		}
		if c.id == "" {
			c = chat{id: id, name: id}
		}
		pinned = append(pinned, c)
	}
	prefs.pinned = pinned
}

func (prefs *watchPreferences) eligible(ctx context.Context, client *imClient, candidates []chat, checked map[string]bool, now time.Time) []chat {
	var chats []chat
	var pending []string
	seen := make(map[string]bool, len(candidates))
	for _, c := range candidates {
		if prefs.excluded[c.id] || checked[c.id] || seen[c.id] {
			continue
		}
		seen[c.id] = true
		chats = append(chats, c)
		if !now.Before(prefs.mutes[c.id].refreshAt) {
			pending = append(pending, c.id)
		}
	}
	if !now.Before(prefs.muteRetryAt) {
		prefs.refreshMutes(ctx, client, pending, now)
	}
	return chats
}

func (prefs *watchPreferences) mentionsOnly(c chat) bool {
	status := prefs.mutes[c.id]
	return status.muted || (!status.known && c.mode != "p2p")
}

func (prefs *watchPreferences) refreshMutes(ctx context.Context, client *imClient, ids []string, now time.Time) {
	for batch := range slices.Chunk(ids, 100) {
		statuses, err := client.muteStatuses(ctx, batch)
		if err != nil {
			prefs.muteRetryAt = now.Add(watchPreferenceRetry)
			prefs.report(ctx, "会话免打扰状态", err)
			return
		}
		var unknown int
		for _, id := range batch {
			entry := prefs.mutes[id]
			if muted, ok := statuses[id]; ok {
				entry.muted = muted
				entry.known = true
			} else {
				unknown++
			}
			entry.refreshAt = now.Add(watchPreferenceTTL)
			prefs.mutes[id] = entry
		}
		if unknown != 0 {
			prefs.report(ctx, "会话免打扰状态", errors.New("部分会话没有返回状态"))
		} else {
			prefs.report(ctx, "会话免打扰状态", nil)
		}
	}
}

type watchIdentity struct {
	OpenID    string `json:"open_id"`
	UnionID   string `json:"union_id"`
	UserID    string `json:"user_id"`
	TenantKey string `json:"tenant_key"`
}

func (client *imClient) currentWatchIdentity(ctx context.Context) (watchIdentity, error) {
	var identity watchIdentity
	err := client.watchRequest(ctx, http.MethodGet, "/open-apis/authen/v1/user_info", nil, nil, &identity)
	if err == nil && identity.OpenID == "" {
		err = errors.New("飞书响应缺少当前账号的 open_id")
	}
	return identity, err
}

func (identity watchIdentity) directlyMentioned(m message) bool {
	if m.raw == nil {
		return false
	}
	for _, mention := range m.raw.Mentions {
		if mention != nil && identity.matchesMention(get(mention.Id), get(mention.IdType), get(mention.TenantKey)) {
			return true
		}
	}
	if get(m.raw.MsgType) == "post" && m.raw.Body != nil {
		post := decodePost([]byte(get(m.raw.Body.Content)))
		for _, paragraphs := range [][][]postNode{post.Content, post.ContentV2} {
			for _, paragraph := range paragraphs {
				for _, node := range paragraph {
					if node.Tag == "at" && identity.matchesMention(node.UserID, "", "") {
						return true
					}
				}
			}
		}
	}
	return false
}

func (identity watchIdentity) matchesMention(id, idType, tenantKey string) bool {
	if id == "" || id == "all" {
		return false
	}
	switch idType {
	case "open_id":
		return id == identity.OpenID
	case "union_id":
		return id == identity.UnionID
	case "user_id":
		return id == identity.UserID && (tenantKey == "" || tenantKey == identity.TenantKey)
	case "":
		return id == identity.OpenID || id == identity.UnionID ||
			(id == identity.UserID && (tenantKey == "" || tenantKey == identity.TenantKey))
	default:
		return false
	}
}

func (prefs *watchPreferences) report(ctx context.Context, operation string, err error) {
	if err == nil {
		delete(prefs.lastError, operation)
	} else if ctx.Err() == nil && prefs.lastError[operation] != err.Error() {
		fmt.Fprintf(os.Stderr, "刷新%s失败，将沿用已有状态并重试：%s\n", operation, terminalText(err.Error(), 400))
		prefs.lastError[operation] = err.Error()
	}
}

type chatIDsRequest struct {
	IDs []string `json:"chat_ids"`
}

func (client *imClient) pinnedChatIDs(ctx context.Context) ([]string, error) {
	var ids []string
	seenIDs := make(map[string]bool)
	seenPages := make(map[string]bool)
	query := make(larkcore.QueryParams)
	for {
		var data struct {
			Shortcuts []struct {
				ID   string `json:"feed_card_id"`
				Type int    `json:"type"`
			} `json:"shortcuts"`
			HasMore   bool   `json:"has_more"`
			PageToken string `json:"page_token"`
		}
		err := client.watchRequest(ctx, http.MethodGet, "/open-apis/im/v2/feed_shortcuts", query, nil, &data)
		if err != nil {
			return nil, err
		}
		for _, shortcut := range data.Shortcuts {
			if shortcut.Type == 1 && shortcut.ID != "" && !seenIDs[shortcut.ID] {
				ids = append(ids, shortcut.ID)
				seenIDs[shortcut.ID] = true
			}
		}
		if !data.HasMore {
			return ids, nil
		}
		if data.PageToken == "" || seenPages[data.PageToken] {
			return nil, errors.New("置顶会话分页游标无效")
		}
		seenPages[data.PageToken] = true
		query.Set("page_token", data.PageToken)
	}
}

func (client *imClient) chatDetails(ctx context.Context, ids []string) (map[string]chat, error) {
	details := make(map[string]chat, len(ids))
	for batch := range slices.Chunk(ids, 50) {
		var data struct {
			Items []*larkim.ListChat `json:"items"`
		}
		query := larkcore.QueryParams{"user_id_type": {"open_id"}}
		err := client.watchRequest(ctx, http.MethodPost, "/open-apis/im/v1/chats/batch_query", query, chatIDsRequest{IDs: batch}, &data)
		if err != nil {
			return details, err
		}
		for _, item := range data.Items {
			if item != nil && get(item.ChatId) != "" {
				details[get(item.ChatId)] = chatFromListItem(item)
			}
		}
	}
	return details, nil
}

func (client *imClient) muteStatuses(ctx context.Context, ids []string) (map[string]bool, error) {
	var data struct {
		Items []struct {
			ID    string `json:"chat_id"`
			Muted *bool  `json:"is_muted"`
		} `json:"items"`
	}
	err := client.watchRequest(ctx, http.MethodPost, "/open-apis/im/v1/chat_user_setting/batch_get_mute_status", nil, chatIDsRequest{IDs: ids}, &data)
	if err != nil {
		return nil, err
	}
	statuses := make(map[string]bool, len(data.Items))
	for _, item := range data.Items {
		if item.ID != "" && item.Muted != nil {
			statuses[item.ID] = *item.Muted
		}
	}
	return statuses, nil
}

func (client *imClient) watchRequest(ctx context.Context, method, path string, query larkcore.QueryParams, body, data any) error {
	request := &larkcore.ApiReq{
		HttpMethod: method, ApiPath: path, QueryParams: query, Body: body,
		SupportedAccessTokenTypes: []larkcore.AccessTokenType{larkcore.AccessTokenTypeUser},
	}
	for attempt := 0; ; attempt++ {
		if err := client.pace(ctx); err != nil {
			return err
		}
		response, err := larkcore.Request(ctx, request, client.config, larkcore.WithUserAccessToken(client.token))
		if err != nil {
			return err
		}
		var reply struct {
			Code    int             `json:"code"`
			Message string          `json:"msg"`
			Data    json.RawMessage `json:"data"`
		}
		decodeErr := json.Unmarshal(response.RawBody, &reply)
		if attempt < 4 && rateLimited(response.StatusCode, reply.Code) {
			client.pauseAfterLimit(response.Header, attempt)
			continue
		}
		if response.StatusCode != http.StatusOK || reply.Code != 0 {
			return apiError(response.StatusCode, reply.Code, reply.Message)
		}
		if decodeErr != nil {
			return decodeErr
		}
		if len(reply.Data) == 0 || string(reply.Data) == "null" {
			return errors.New("飞书响应缺少 data")
		}
		return json.Unmarshal(reply.Data, data)
	}
}
