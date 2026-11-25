package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestArchiveWatchResumesAndKeepsRawTopicMessages(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "openviking.sqlite")
	db, err := openPrivateArchiveDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const chatID = "oc_selected"
	group := chat{id: chatID, mode: "group", name: "项目群"}
	archive := &watchArchive{db: db, appID: "cli_watch", groups: map[string]bool{chatID: true}}
	archive.setIdentity(watchIdentity{OpenID: "ou_watcher"})
	if !archive.includes(group) || !archive.includes(chat{id: "oc_private", mode: "p2p"}) ||
		archive.includes(chat{id: "oc_other", mode: "group"}) {
		t.Fatal("unexpected archive scope")
	}
	original := `{"title":"故障讨论","content":[[{"tag":"text","text":"` +
		strings.Repeat("测试", 220) + `"},{"tag":"img","image_key":"img_v3_example"}]]}`
	rawJSON, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	client := newIMClient(&http.Client{Transport: fakeTransport(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Query().Get("container_id_type") {
		case "chat":
			return jsonResponse(`{"code":0,"data":{"items":[
				{"message_id":"om_reply","create_time":"112000","root_id":"om_root","msg_type":"post",
				 "body":{"content":` + string(rawJSON) + `}},
				{"message_id":"om_root","thread_id":"omt_alert","create_time":"111000"},
				{"message_id":"om_history","create_time":"109000"}]}}`), nil
		case "thread":
			return jsonResponse(`{"code":0,"data":{"items":[
				{"message_id":"om_reply","create_time":"112000","root_id":"om_root","parent_id":"om_root","msg_type":"post",
				 "sender":{"id":"ou_sender","id_type":"open_id","sender_type":"user","sender_name":"小林"},
				 "body":{"content":` + string(rawJSON) + `}}]}}`), nil
		}
		t.Errorf("unexpected Feishu request %s", request.URL)
		return jsonResponse(`{"code":1}`), nil
	})}, credentials{AppID: "cli_watch"})
	client.token = "user-token"
	started, until := time.Unix(110, 0), time.Unix(113, 0)
	newState := func(started time.Time) *watchChat {
		return &watchChat{cursor: started.Add(-time.Hour), threads: make(map[string]*watchThread),
			seen: make(map[string]time.Time), errors: make(map[string]string)}
	}
	emit := func(message) {}
	if result := archive.poll(ctx, client, group, newState(started), started, until, time.Hour, emit); !result.complete {
		t.Fatal("first poll failed")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = openPrivateArchiveDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	archive.db = db
	restartedAt := time.Unix(114, 0)
	state := newState(restartedAt)
	if result := archive.poll(ctx, client, group, state, restartedAt, time.Unix(115, 0), time.Hour, emit); !result.complete {
		t.Fatal("resumed poll failed")
	}
	if !state.archiveStarted.Equal(started) || state.threads["omt_alert"] == nil {
		t.Fatalf("checkpoint lost after restart: %+v", state.archiveCheckpoint())
	}

	var count int
	if err := db.QueryRow(`SELECT count(*) FROM archive_messages`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("messages in queue = %d, %v", count, err)
	}
	var content string
	if err := db.QueryRow(`SELECT content FROM archive_messages WHERE message_id = 'om_reply'`).Scan(&content); err != nil {
		t.Fatal(err)
	}
	var record archiveMessage
	if err := json.Unmarshal([]byte(content), &record); err != nil {
		t.Fatal(err)
	}
	if record.ThreadID != "omt_alert" || record.RootID != "om_root" || record.ParentID != "om_root" ||
		get(record.Body) != original || record.Sender != "小林" || record.PeerID == "" || len(record.ImageURLs) != 1 {
		t.Fatalf("unexpected archived topic message: %+v", record)
	}
}

func TestOpenVikingDeliveryReconcilesAmbiguousAppend(t *testing.T) {
	ctx := context.Background()
	db, err := openPrivateArchiveDB(filepath.Join(t.TempDir(), "openviking.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	const source = "example_source"
	const sessionPath = "/openviking/api/v1/sessions/feishu_example_source_oc_private"
	var mu sync.Mutex
	var added []string
	var created int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if req.Header.Get("Authorization") != "Bearer test-key" || req.Header.Get("X-API-Key") != "" {
			t.Errorf("incorrect hosted authentication headers")
		}
		w.Header().Set("Content-Type", "application/json")
		var body map[string]any
		switch {
		case req.Method == http.MethodPost && req.URL.Path == "/openviking/api/v1/sessions":
			created++
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			autoCommit, present := body["auto_commit_policy"]
			if body["session_id"] != "feishu_example_source_oc_private" || !present || autoCommit != nil {
				t.Errorf("incorrect session parameters: %v", body)
			}
			w.Write([]byte(`{"status":"ok","result":{"session_id":"feishu_example_source_oc_private"}}`))
		case req.Method == http.MethodPost && req.URL.Path == sessionPath+"/messages":
			if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			content, _ := body["content"].(string)
			added = append(added, content)
			if body["role"] != "user" || body["peer_id"] != "feishu_peer_example" ||
				body["created_at"] != "1970-01-01T00:01:52Z" {
				t.Errorf("incorrect message parameters: %v", body)
			}
			if len(added) == 1 {
				w.WriteHeader(http.StatusBadGateway)
				w.Write([]byte(`{"status":"error","error":{"code":"SERVER_ERROR"}}`))
				return
			}
			w.Write([]byte(`{"status":"ok","result":{"message_count":2}}`))
		case req.Method == http.MethodGet && req.URL.Path == sessionPath+"/context":
			json.NewEncoder(w).Encode(map[string]any{"status": "ok", "result": map[string]any{
				"messages": []any{map[string]any{"parts": []any{map[string]any{"type": "text", "text": added[0]}}}},
			}})
		default:
			t.Errorf("unexpected OpenViking request %s %s", req.Method, req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	client, err := newOpenVikingClient(server.URL+"/openviking", "test-key", "")
	if err != nil {
		t.Fatal(err)
	}
	defer client.CloseIdleConnections()
	archive := &watchArchive{db: db, client: client}
	dm := chat{id: "oc_private", mode: "p2p"}
	for _, id := range []string{"om_first", "om_second"} {
		record := archiveMessage{MessageID: id, ChatID: dm.id, CreatedAt: time.Unix(112, 0), PeerID: "feishu_peer_example"}
		if err := archive.saveCheckpoint(ctx, source, dm, archiveCheckpoint{}, map[string]archiveMessage{id: record}); err != nil {
			t.Fatal(err)
		}
	}
	sessions := make(map[string]bool)
	deliverNext := func(wantID, wantState string) {
		t.Helper()
		item, err := archive.nextDelivery(ctx, source)
		if err != nil || item == nil || item.messageID != wantID || item.state != wantState {
			t.Fatalf("next delivery = %+v, %v", item, err)
		}
		if err := archive.deliver(ctx, item, sessions); err != nil {
			t.Fatal(err)
		}
	}
	deliverNext("om_first", "pending")
	if item, err := archive.nextDelivery(ctx, source); err != nil || item != nil {
		t.Fatalf("later message passed an unresolved append: %+v, %v", item, err)
	}
	if _, err := db.Exec(`UPDATE archive_messages SET retry_at=0 WHERE message_id='om_first'`); err != nil {
		t.Fatal(err)
	}
	deliverNext("om_first", "sending")
	deliverNext("om_second", "pending")
	mu.Lock()
	defer mu.Unlock()
	if created != 1 || len(added) != 2 || !isArchiveMessage(added[0], "om_first") ||
		!isArchiveMessage(added[1], "om_second") {
		t.Fatalf("created sessions = %d, appended messages = %v", created, added)
	}
}
