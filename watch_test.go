package main

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
)

func TestWatchRoundPrioritizesAndCachesPreferences(t *testing.T) {
	var messageMu sync.Mutex
	var messages []string
	pinCalls, muteCalls, detailCalls, identityCalls := 0, 0, 0, 0
	client := newIMClient(&http.Client{Transport: fakeTransport(func(request *http.Request) (*http.Response, error) {
		if request.Header.Get("Authorization") != "Bearer user-token" {
			t.Error("watch must use the user's authorization")
		}
		switch request.URL.Path {
		case "/open-apis/authen/v1/user_info":
			identityCalls++
			return jsonResponse(`{"code":0,"data":{"open_id":"ou_current"}}`), nil
		case "/open-apis/im/v2/feed_shortcuts":
			pinCalls++
			if request.URL.Query().Get("page_token") == "pin-next" {
				return jsonResponse(`{"code":0,"data":{"shortcuts":[{"feed_card_id":"oc_skip","type":1},{"feed_card_id":"oc_other","type":2}],"has_more":false}}`), nil
			}
			return jsonResponse(`{"code":0,"data":{"shortcuts":[{"feed_card_id":"oc_pinned","type":1},{"feed_card_id":"oc_muted","type":1}],"has_more":true,"page_token":"pin-next"}}`), nil
		case "/open-apis/im/v1/chats/batch_query":
			detailCalls++
			checkWatchChatIDs(t, request, []string{"oc_pinned", "oc_muted"})
			return jsonResponse(`{"code":0,"data":{"items":[{"chat_id":"oc_pinned","chat_mode":"group","external":true},{"chat_id":"oc_muted","chat_mode":"p2p"}]}}`), nil
		case "/open-apis/im/v1/chat_user_setting/batch_get_mute_status":
			muteCalls++
			switch muteCalls {
			case 1:
				checkWatchChatIDs(t, request, []string{"oc_pinned", "oc_muted"})
				return jsonResponse(`{"code":0,"data":{"items":[{"chat_id":"oc_pinned","is_muted":false},{"chat_id":"oc_muted","is_muted":true}]}}`), nil
			case 2:
				checkWatchChatIDs(t, request, []string{"oc_hot"})
				return jsonResponse(`{"code":0,"data":{"items":[{"chat_id":"oc_hot","is_muted":false}]}}`), nil
			case 3:
				checkWatchChatIDs(t, request, []string{"oc_background"})
				return jsonResponse(`{"code":0,"data":{"items":[{"chat_id":"oc_background","is_muted":false}]}}`), nil
			default:
				t.Error("looked up mute preferences again before the cache expired")
			}
		case "/open-apis/im/v1/chats":
			switch request.URL.Query().Get("sort_type") {
			case "ByActiveTimeDesc":
				if request.URL.Query().Get("page_token") == "hot-next" {
					return jsonResponse(`{"code":0,"data":{"items":[{"chat_id":"oc_hot","chat_mode":"p2p"}]}}`), nil
				}
				return jsonResponse(`{"code":0,"data":{"items":[{"chat_id":"oc_pinned","chat_mode":"p2p"},{"chat_id":"oc_muted","chat_mode":"p2p"}],"has_more":true,"page_token":"hot-next"}}`), nil
			case "ByCreateTimeAsc":
				return jsonResponse(`{"code":0,"data":{"items":[{"chat_id":"oc_hot","chat_mode":"p2p"},{"chat_id":"oc_skip","chat_mode":"p2p"},{"chat_id":"oc_muted","chat_mode":"p2p"},{"chat_id":"oc_background","chat_mode":"p2p"}]}}`), nil
			}
		case "/open-apis/im/v1/messages":
			messageMu.Lock()
			messages = append(messages, request.URL.Query().Get("container_id"))
			messageMu.Unlock()
			return jsonResponse(`{"code":0,"data":{"items":[]}}`), nil
		}
		t.Errorf("unexpected watch request %s", request.URL)
		return jsonResponse(`{"code":1,"msg":"unexpected request"}`), nil
	})}, credentials{AppID: "cli_test", AppSecret: "app-secret"})
	client.token = "user-token"
	schedule := watchSchedule{
		started: time.Now(), lookback: time.Hour, recentLimit: 2, backgroundBatch: 4,
		states: make(map[string]*watchChat),
		preferences: watchPreferences{
			excluded: map[string]bool{"oc_skip": true}, mutes: make(map[string]cachedMute), lastError: make(map[string]string),
		},
	}
	for range 2 {
		if err := schedule.pollRound(context.Background(), client); err != nil {
			t.Fatal(err)
		}
	}
	if pinned := schedule.preferences.pinned; len(pinned) == 0 || pinned[0].label() != "外部群聊" {
		t.Fatalf("pinned chat lost its external group label: %v", pinned)
	}
	want := []string{"oc_muted", "oc_pinned", "oc_hot", "oc_background", "oc_muted", "oc_pinned", "oc_hot", "oc_background"}
	if len(messages) == len(want) {
		slices.Sort(messages[:2])
		slices.Sort(messages[4:6])
	}
	if !reflect.DeepEqual(messages, want) {
		t.Errorf("message requests = %v, want %v", messages, want)
	}
	if pinCalls != 2 || muteCalls != 3 || detailCalls != 1 || identityCalls != 1 {
		t.Errorf("preference requests = %d pinned pages, %d mute, %d details, %d identity", pinCalls, muteCalls, detailCalls, identityCalls)
	}
}

func TestWatchStopsRecentScanAndVisitsRemainingChatsInBackground(t *testing.T) {
	var requests []string
	client := newIMClient(&http.Client{Transport: fakeTransport(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/open-apis/im/v1/chats":
			sort := request.URL.Query().Get("sort_type")
			requests = append(requests, sort)
			if sort == "ByActiveTimeDesc" && request.URL.Query().Get("page_token") == "" {
				return jsonResponse(`{"code":0,"data":{"items":[{"chat_id":"oc_quiet","chat_mode":"p2p"},{"chat_id":"oc_later","chat_mode":"p2p"}],"has_more":true,"page_token":"unused"}}`), nil
			}
			if sort == "ByCreateTimeAsc" {
				return jsonResponse(`{"code":0,"data":{"items":[{"chat_id":"oc_quiet","chat_mode":"p2p"},{"chat_id":"oc_later","chat_mode":"p2p"}]}}`), nil
			}
		case "/open-apis/im/v1/messages":
			requests = append(requests, request.URL.Query().Get("container_id"))
			return jsonResponse(`{"code":0,"data":{"items":[]}}`), nil
		}
		t.Errorf("unexpected watch request %s", request.URL)
		return jsonResponse(`{"code":1,"msg":"unexpected request"}`), nil
	})}, credentials{AppID: "cli_test", AppSecret: "app-secret"})
	client.token = "user-token"
	now := time.Now()
	schedule := watchSchedule{
		started: now.Add(-time.Minute), lastRecent: now.Add(-15 * time.Second),
		lookback: time.Hour, recentLimit: 2, backgroundBatch: 2,
		states: make(map[string]*watchChat), identity: watchIdentity{OpenID: "ou_current"}, identityToken: client.token,
		preferences: watchPreferences{
			pinsRefreshAt: now.Add(time.Minute),
			mutes: map[string]cachedMute{
				"oc_quiet": {known: true, refreshAt: now.Add(time.Minute)},
				"oc_later": {known: true, refreshAt: now.Add(time.Minute)},
			},
			lastError: make(map[string]string),
		},
	}
	if err := schedule.pollRound(context.Background(), client); err != nil {
		t.Fatal(err)
	}
	want := []string{"ByActiveTimeDesc", "oc_quiet", "ByCreateTimeAsc", "oc_later"}
	if !reflect.DeepEqual(requests, want) {
		t.Errorf("requests = %v, want %v", requests, want)
	}
}

func TestWatchQuietCheckNeedsCompleteCoverage(t *testing.T) {
	cutoff := time.Now()
	if (watchPollResult{since: cutoff, newest: cutoff, complete: true}).quietSince(cutoff) {
		t.Error("a message at the cutoff counts as activity")
	}
	if (watchPollResult{since: cutoff.Add(time.Second), complete: true}).quietSince(cutoff) {
		t.Error("a scan starting after the cutoff cannot establish inactivity")
	}
	if (watchPollResult{since: cutoff}).quietSince(cutoff) {
		t.Error("a failed scan cannot establish inactivity")
	}
}

func TestWatchKeepsMuteStateUntilRefreshSucceeds(t *testing.T) {
	calls := 0
	client := newIMClient(&http.Client{Transport: fakeTransport(func(request *http.Request) (*http.Response, error) {
		calls++
		checkWatchChatIDs(t, request, []string{"oc_example"})
		switch calls {
		case 1:
			return jsonResponse(`{"code":0,"data":{"items":[{"chat_id":"oc_example","is_muted":true}]}}`), nil
		case 2:
			return jsonResponse(`{"code":1,"msg":"lookup failed"}`), nil
		default:
			return jsonResponse(`{"code":0,"data":{"items":[{"chat_id":"oc_example","is_muted":false}]}}`), nil
		}
	})}, credentials{AppID: "cli_test", AppSecret: "app-secret"})
	client.token = "user-token"
	prefs := watchPreferences{mutes: make(map[string]cachedMute), lastError: make(map[string]string)}
	candidates := []chat{{id: "oc_example"}}
	now := time.Now()
	for _, offset := range []time.Duration{0, time.Minute, watchPreferenceTTL, watchPreferenceTTL + time.Second} {
		prefs.eligible(context.Background(), client, candidates, nil, now.Add(offset))
		if !prefs.mentionsOnly(candidates[0]) {
			t.Errorf("lost the muted chat's notification rule at %s", offset)
		}
	}
	if calls != 2 {
		t.Errorf("lookup count before retry = %d, want 2", calls)
	}
	prefs.eligible(context.Background(), client, candidates, nil, now.Add(watchPreferenceTTL+watchPreferenceRetry))
	if prefs.mentionsOnly(candidates[0]) || calls != 3 {
		t.Errorf("unmuted refresh still mentions-only = %t, lookup count = %d", prefs.mentionsOnly(candidates[0]), calls)
	}
}

func TestWatchReportsOnlyDirectMentionsInMutedChats(t *testing.T) {
	schedule := watchSchedule{
		identity: watchIdentity{OpenID: "ou_current", UnionID: "on_current", UserID: "employee", TenantKey: "tenant"},
		preferences: watchPreferences{mutes: map[string]cachedMute{
			"oc_muted": {known: true, muted: true},
			"oc_loud":  {known: true},
		}},
	}
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "ordinary", raw: `{"msg_type":"text","body":{"content":"{\"text\":\"@ou_current\"}"}}`},
		{name: "everyone", raw: `{"mentions":[{"id":"all","id_type":"open_id"}]}`},
		{name: "another user", raw: `{"mentions":[{"id":"ou_other","name":"same name"}]}`},
		{name: "direct", raw: `{"mentions":[{"id":"ou_current","id_type":"open_id"}]}`, want: true},
		{name: "direct plus everyone", raw: `{"mentions":[{"id":"all"},{"id":"on_current","id_type":"union_id"}]}`, want: true},
		{name: "tenant employee ID", raw: `{"mentions":[{"id":"employee","tenant_key":"tenant","id_type":"user_id"}]}`, want: true},
		{name: "employee ID in different tenant", raw: `{"mentions":[{"id":"employee","tenant_key":"other","id_type":"user_id"}]}`},
		{name: "rich text direct", raw: `{"msg_type":"post","body":{"content":"{\"content\":[[{\"tag\":\"at\",\"user_id\":\"ou_current\"}]]}"}}`, want: true},
		{name: "rich text everyone", raw: `{"msg_type":"post","body":{"content":"{\"content\":[[{\"tag\":\"at\",\"user_id\":\"all\"}]]}"}}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var raw larkim.Message
			if err := json.Unmarshal([]byte(test.raw), &raw); err != nil {
				t.Fatal(err)
			}
			m := message{raw: &raw}
			for _, id := range []string{"oc_muted", "oc_unknown"} {
				text, show := schedule.notification(chat{id: id, mode: "group"}, m)
				if show != test.want || strings.HasPrefix(text, "[@我] ") != test.want {
					t.Errorf("notification = %q, show = %t for %s, want direct = %t", text, show, id, test.want)
				}
			}
			text, show := schedule.notification(chat{id: "oc_loud", mode: "group"}, m)
			if !show || strings.HasPrefix(text, "[@我] ") != test.want {
				t.Errorf("unmuted notification = %q, show = %t, want direct = %t", text, show, test.want)
			}
		})
	}
}

func TestLoadWatchExclusions(t *testing.T) {
	dir := t.TempDir()
	ids, err := loadWatchExclusions(dir)
	if err != nil || len(ids) != 0 {
		t.Fatalf("missing optional exclusions file: %v, %v", ids, err)
	}
	path := filepath.Join(dir, "watch-exclude.txt")
	if err := os.WriteFile(path, []byte("\uFEFFoc_one # inline comment\r\n # comments\n\noc_two\noc_one\n"), 0600); err != nil {
		t.Fatal(err)
	}
	ids, err = loadWatchExclusions(dir)
	if err != nil || !reflect.DeepEqual(ids, map[string]bool{"oc_one": true, "oc_two": true}) {
		t.Fatalf("exclusions = %v, err = %v", ids, err)
	}
}

func checkWatchChatIDs(t *testing.T, request *http.Request, want []string) {
	t.Helper()
	var body chatIDsRequest
	if request.Method != http.MethodPost {
		t.Errorf("request method = %s, want POST", request.Method)
	}
	if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
		t.Errorf("decode chat IDs: %v", err)
	}
	if !reflect.DeepEqual(body.IDs, want) {
		t.Errorf("chat IDs = %s, want %s", strings.Join(body.IDs, ","), strings.Join(want, ","))
	}
}
