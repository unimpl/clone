package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

const imBaseURL = "https://open.feishu.cn"

type imClient struct {
	api    *larkim.V1
	config *larkcore.Config
	mu     sync.Mutex
	next   time.Time
	token  string
}

func newIMClient(httpClient *http.Client, app credentials) *imClient {
	config := &larkcore.Config{
		BaseUrl: imBaseURL, AppId: app.AppID, AppSecret: app.AppSecret,
		AppType: larkcore.AppTypeSelfBuilt, HttpClient: httpClient, LogLevel: larkcore.LogLevelError,
	}
	larkcore.NewLogger(config)
	larkcore.NewSerialization(config)
	return &imClient{api: larkim.New(config), config: config}
}

func (client *imClient) pace(ctx context.Context) error {
	for {
		client.mu.Lock()
		delay := time.Until(client.next)
		if delay <= 0 {
			client.next = time.Now().Add(125 * time.Millisecond)
		}
		client.mu.Unlock()
		if delay <= 0 {
			return ctx.Err()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

func (client *imClient) pauseAfterLimit(header http.Header, attempt int) {
	seconds := 5 << attempt
	for _, name := range []string{"Retry-After", "X-Ogw-Ratelimit-Reset"} {
		if value, err := strconv.Atoi(header.Get(name)); err == nil && value > 0 {
			seconds = max(seconds, value)
		}
	}
	client.mu.Lock()
	client.next = latest(client.next, time.Now().Add(time.Duration(min(seconds, 60))*time.Second))
	client.mu.Unlock()
}

type chat struct {
	id   string
	name string
	mode string
}

func (c chat) label() string {
	if c.mode == "p2p" {
		return "私聊"
	}
	return "群聊"
}

func (client *imClient) chats(ctx context.Context) ([]chat, error) {
	return client.chatsByOrder(ctx, "ByActiveTimeDesc")
}

func (client *imClient) chatsByOrder(ctx context.Context, order string) ([]chat, error) {
	var chats []chat
	unique := make(map[string]bool)
	for page := ""; ; {
		batch, next, err := client.chatPage(ctx, page, 100, order)
		if err != nil {
			return nil, err
		}
		for _, c := range batch {
			if !unique[c.id] {
				unique[c.id] = true
				chats = append(chats, c)
			}
		}
		if next == "" {
			return chats, nil
		}
		page = next
	}
}

func (client *imClient) chatPage(ctx context.Context, page string, size int, order string) ([]chat, string, error) {
	builder := larkim.NewListChatReqBuilder().Types("p2p,group").SortType(order).PageSize(size)
	if page != "" {
		builder.PageToken(page)
	}
	var response *larkim.ListChatResp
	for attempt := 0; ; attempt++ {
		if err := client.pace(ctx); err != nil {
			return nil, "", err
		}
		var err error
		response, err = client.api.Chat.List(ctx, builder.Build(), larkcore.WithUserAccessToken(client.token))
		if err != nil {
			return nil, "", err
		}
		if attempt >= 4 || !rateLimited(response.StatusCode, response.Code) {
			break
		}
		client.pauseAfterLimit(response.Header, attempt)
	}
	if !response.Success() || response.StatusCode != http.StatusOK {
		return nil, "", apiError(response.StatusCode, response.Code, response.Msg)
	}
	if response.Data == nil {
		return nil, "", errors.New("会话列表响应缺少 data")
	}
	var chats []chat
	for _, item := range response.Data.Items {
		if item == nil || get(item.ChatId) == "" {
			continue
		}
		chats = append(chats, chatFromListItem(item))
	}
	if !get(response.Data.HasMore) {
		return chats, "", nil
	}
	next := get(response.Data.PageToken)
	if next == "" || next == page {
		return nil, "", errors.New("会话列表分页游标无效")
	}
	return chats, next, nil
}

func chatFromListItem(item *larkim.ListChat) chat {
	id, name := get(item.ChatId), get(item.Name)
	if name == "" {
		name = id
	}
	return chat{id: id, name: name, mode: get(item.ChatMode)}
}

type message struct {
	id     string
	at     time.Time
	thread string
	raw    *larkim.Message
}

func (client *imClient) messages(ctx context.Context, kind, id string, since, until time.Time) ([]message, error) {
	var messages []message
	var limitAttempts int
	for page := ""; ; {
		builder := larkim.NewListMessageReqBuilder().ContainerIdType(kind).ContainerId(id).
			SortType("ByCreateTimeDesc").PageSize(50).WithSenderName(true)
		if kind == "chat" {
			builder.StartTime(strconv.FormatInt(since.Unix(), 10)).EndTime(strconv.FormatInt(until.Unix()+1, 10)).
				OnlyThreadRootMessages(false)
		}
		if page != "" {
			builder.PageToken(page)
		}
		if err := client.pace(ctx); err != nil {
			return nil, err
		}
		response, err := client.api.Message.List(ctx, builder.Build(), larkcore.WithUserAccessToken(client.token))
		if err != nil {
			return nil, err
		}
		if limitAttempts < 4 && rateLimited(response.StatusCode, response.Code) {
			client.pauseAfterLimit(response.Header, limitAttempts)
			limitAttempts++
			continue
		}
		limitAttempts = 0
		if !response.Success() || response.StatusCode != http.StatusOK {
			return nil, apiError(response.StatusCode, response.Code, response.Msg)
		}
		if response.Data == nil {
			return nil, errors.New("消息列表响应缺少 data")
		}
		reachedStart := false
		for _, item := range response.Data.Items {
			if item == nil || get(item.MessageId) == "" {
				continue
			}
			milliseconds, err := strconv.ParseInt(get(item.CreateTime), 10, 64)
			if err != nil {
				return nil, fmt.Errorf("消息 %s 的创建时间无效", get(item.MessageId))
			}
			at := time.UnixMilli(milliseconds)
			if at.Before(since) {
				reachedStart = true
				continue
			}
			if !at.Before(until) {
				continue
			}
			messages = append(messages, message{id: get(item.MessageId), at: at, thread: get(item.ThreadId), raw: item})
		}
		if reachedStart || !get(response.Data.HasMore) {
			return messages, nil
		}
		next := get(response.Data.PageToken)
		if next == "" || next == page {
			return nil, errors.New("消息列表分页游标无效")
		}
		page = next
	}
}

type imAPIError struct {
	status  int
	code    int
	message string
}

func (err imAPIError) Error() string {
	return fmt.Sprintf("飞书 API HTTP %d，code=%d，%s", err.status, err.code, terminalText(err.message, 200))
}

func apiError(status, code int, message string) error {
	return imAPIError{status: status, code: code, message: message}
}

func rateLimited(status, code int) bool {
	return status == http.StatusTooManyRequests || code == 99991400
}

func get[T any](value *T) T {
	if value != nil {
		return *value
	}
	var zero T
	return zero
}
