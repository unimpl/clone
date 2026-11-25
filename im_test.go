package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

type fakeTransport func(*http.Request) (*http.Response, error)

func (transport fakeTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

func TestIMUsesUserIdentityAndPaginates(t *testing.T) {
	var calls int
	client := newIMClient(&http.Client{Transport: fakeTransport(func(request *http.Request) (*http.Response, error) {
		calls++
		if got := request.Header.Get("Authorization"); got != "Bearer user-token" {
			t.Fatalf("expected user identity, got %q", got)
		}
		query := request.URL.Query()
		var body string
		switch request.URL.Path {
		case "/open-apis/im/v1/chats":
			if query.Get("types") != "p2p,group" {
				t.Fatalf("p2p conversations missing from request: %v", query)
			}
			body = `{"code":0,"data":{"items":[{"chat_id":"oc_dm","chat_mode":"p2p"},{"chat_id":"oc_group","chat_mode":"group"}]}}`
		case "/open-apis/im/v1/messages":
			if query.Get("container_id") != "oc_dm" || query.Get("start_time") != "10" {
				t.Fatalf("unexpected message query: %v", query)
			}
			switch query.Get("page_token") {
			case "":
				body = `{"code":0,"data":{"has_more":true,"page_token":"next","items":[{"message_id":"om_late","create_time":"12000"},{"message_id":"om_first","create_time":"11500"}]}}`
			case "next":
				body = `{"code":0,"data":{"items":[{"message_id":"om_second","create_time":"11000"},{"message_id":"om_early","create_time":"10000"}]}}`
			default:
				t.Fatalf("unexpected page token: %v", query)
			}
		default:
			t.Fatalf("unexpected endpoint: %v", request.URL)
		}
		return jsonResponse(body), nil
	})}, credentials{AppID: "cli_test", AppSecret: "app-secret"})
	client.token = "user-token"
	ctx := context.Background()
	chats, err := client.chats(ctx)
	if err != nil || len(chats) != 2 || chats[0].label() != "私聊" || chats[1].label() != "群聊" {
		t.Fatalf("chats = %v, error = %v", chats, err)
	}
	messages, err := client.messages(ctx, "chat", chats[0].id, time.UnixMilli(10500), time.Unix(12, 0))
	if err != nil || len(messages) != 2 || messages[0].id != "om_first" || messages[1].id != "om_second" {
		t.Fatalf("messages = %v, error = %v", messages, err)
	}
	if calls != 3 {
		t.Fatalf("requests = %d, want 3", calls)
	}
}

func TestWatchSkipsHistoryAndDuplicates(t *testing.T) {
	started := time.Unix(110, 0)
	state := watchChat{cursor: started, seen: make(map[string]time.Time)}
	var requests int
	client := newIMClient(&http.Client{Transport: fakeTransport(func(request *http.Request) (*http.Response, error) {
		requests++
		if request.URL.Query().Get("container_id") != "oc_dm" {
			t.Fatalf("unexpected container: %v", request.URL)
		}
		if requests == 1 {
			return jsonResponse(`{"code":0,"data":{"items":[{"message_id":"second","create_time":"112000"},{"message_id":"first","create_time":"111000"},{"message_id":"history","create_time":"108000"}]}}`), nil
		}
		return jsonResponse(`{"code":0,"data":{"items":[{"message_id":"third","create_time":"113000"},{"message_id":"second","create_time":"112000"},{"message_id":"first","create_time":"111000"}]}}`), nil
	})}, credentials{AppID: "cli_test", AppSecret: "app-secret"})
	client.token = "user-token"
	c := chat{id: "oc_dm", mode: "p2p"}
	var emitted []string
	emit := func(m message) { emitted = append(emitted, m.id) }
	state.poll(context.Background(), client, c, started, time.UnixMilli(112500), time.Hour, emit, nil)
	state.poll(context.Background(), client, c, started, time.UnixMilli(113500), time.Hour, emit, nil)
	if got := strings.Join(emitted, ","); got != "first,second,third" {
		t.Fatalf("notifications = %s", got)
	}
}

func TestWatchPreview(t *testing.T) {
	const imageURL = "https://open.feishu.cn/open-apis/im/v1/messages/om_example/resources/img_v3_example?type=image"
	const sourceURL = "https://applink.feishu.cn/client/chat/open?openChatId=oc_example"
	tests := []struct {
		name string
		kind string
		body string
		link string
		want string
	}{
		{
			name: "received rich text",
			kind: "post",
			body: `{"title":"发布通知","content":[[{"tag":"text","text":"请查看 "},{"tag":"a","text":"更新文档","href":"https://example.com/changes?a=1&b=2"},{"tag":"text","text":" "},{"tag":"at","user_id":"ou_example"}],[{"tag":"img","image_key":"img_v3_example"}]]}`,
			want: "发布通知 请查看 更新文档 @小林 [图片] | 链接 https://example.com/changes?a=1&b=2 | 图片资源 API " + imageURL,
		},
		{
			name: "localized content and terminal controls",
			kind: "post",
			body: `{"zh_cn":{"title":"进度","content_v2":[[{"tag":"code_block","text":"第一行\n第二行\u001b"}]]},"en_us":{"title":"Progress"}}`,
			want: "进度 第一行 第二行",
		},
		{
			name: "image and original message",
			kind: "image",
			body: `{"image_key":"img_v3_example"}`,
			link: sourceURL,
			want: "[图片] | 图片资源 API " + imageURL + " | 飞书原消息 " + sourceURL,
		},
		{
			name: "image address survives shortened body",
			kind: "post",
			body: `{"title":"` + strings.Repeat("字", 205) + `","content":[[{"tag":"img","image_key":"img_v3_example"}]]}`,
			want: strings.Repeat("字", 200) + "… | 图片资源 API " + imageURL,
		},
		{name: "invalid post", kind: "post", body: "{", want: "[post]"},
		{name: "missing image key", kind: "image", body: `{}`, want: "[图片]"},
		{name: "plain text mention", kind: "text", body: `{"text":"你好 @_user_1"}`, want: "你好 @小林"},
		{name: "unsupported type", kind: "file", body: `{}`, want: "[file]"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var raw larkim.Message
			err := json.Unmarshal([]byte(`{"message_id":"om_example","mentions":[{"key":"@_user_1","id":"ou_example","name":"小林"}]}`), &raw)
			if err != nil {
				t.Fatal(err)
			}
			raw.MsgType = &test.kind
			raw.Body = &larkim.MessageBody{Content: &test.body}
			raw.MessageAppLink = &test.link
			if got := preview(message{id: get(raw.MessageId), raw: &raw}); got != test.want {
				t.Errorf("preview = %q, want %q", got, test.want)
			}
		})
	}
}

func jsonResponse(body string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)),
		Header: http.Header{"Content-Type": {"application/json"}},
	}
}
