package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"time"

	_ "modernc.org/sqlite"
)

const archiveSchema = `
CREATE TABLE IF NOT EXISTS archive_checkpoints (
	source TEXT NOT NULL,
	chat_id TEXT NOT NULL,
	data TEXT NOT NULL,
	PRIMARY KEY (source, chat_id)
);
CREATE TABLE IF NOT EXISTS archive_messages (
	source TEXT NOT NULL,
	message_id TEXT NOT NULL,
	chat_id TEXT NOT NULL,
	chat_mode TEXT NOT NULL,
	created_at INTEGER NOT NULL,
	peer_id TEXT NOT NULL,
	content TEXT,
	state TEXT NOT NULL DEFAULT 'pending',
	retry_at INTEGER NOT NULL DEFAULT 0,
	PRIMARY KEY (source, message_id)
);
CREATE INDEX IF NOT EXISTS archive_pending ON archive_messages (source, chat_id, created_at, message_id)
	WHERE state != 'sent';
`

type archiveCheckpoint struct {
	Started time.Time                `json:"started"`
	Cursor  time.Time                `json:"cursor"`
	Threads map[string]archiveThread `json:"threads"`
}

type archiveThread struct {
	Cursor time.Time `json:"cursor"`
	Active time.Time `json:"active"`
}

func (state *watchChat) archiveCheckpoint() archiveCheckpoint {
	threads := make(map[string]archiveThread, len(state.threads))
	for id, thread := range state.threads {
		threads[id] = archiveThread{Cursor: thread.cursor, Active: thread.active}
	}
	return archiveCheckpoint{Started: state.archiveStarted, Cursor: state.cursor, Threads: threads}
}

func (state *watchChat) restoreArchive(checkpoint archiveCheckpoint) {
	state.archiveStarted = checkpoint.Started
	state.cursor = checkpoint.Cursor
	state.threads = make(map[string]*watchThread, len(checkpoint.Threads))
	for id, thread := range checkpoint.Threads {
		state.threads[id] = &watchThread{cursor: thread.Cursor, active: thread.Active}
	}
}

type archiveMessage struct {
	MessageID  string    `json:"feishu_message_id"`
	ChatID     string    `json:"chat_id"`
	ChatName   string    `json:"chat_name,omitempty"`
	ThreadID   string    `json:"thread_id,omitempty"`
	RootID     string    `json:"root_id,omitempty"`
	ParentID   string    `json:"parent_id,omitempty"`
	Type       string    `json:"message_type,omitempty"`
	SenderID   string    `json:"sender_id,omitempty"`
	SenderType string    `json:"sender_type,omitempty"`
	Sender     string    `json:"sender_name,omitempty"`
	PeerID     string    `json:"peer_id,omitempty"`
	Body       *string   `json:"body,omitempty"`
	ImageURLs  []string  `json:"image_urls,omitempty"`
	Link       string    `json:"message_link,omitempty"`
	Deleted    bool      `json:"deleted,omitempty"`
	CreatedAt  time.Time `json:"-"`
}

func makeArchiveMessage(c chat, m message, threadID string) archiveMessage {
	record := archiveMessage{MessageID: m.id, ChatID: c.id, ChatName: c.name, ThreadID: m.thread, CreatedAt: m.at}
	if record.ThreadID == "" {
		record.ThreadID = threadID
	}
	if m.raw == nil {
		return record
	}
	record.RootID = get(m.raw.RootId)
	record.ParentID = get(m.raw.ParentId)
	record.Type = get(m.raw.MsgType)
	record.Link = get(m.raw.MessageAppLink)
	record.Deleted = get(m.raw.Deleted)
	if m.raw.Body != nil {
		record.Body = m.raw.Body.Content
		record.ImageURLs = archiveImageURLs(record.Type, m.id, get(record.Body))
	}
	if sender := m.raw.Sender; sender != nil {
		record.SenderID = get(sender.Id)
		record.SenderType = get(sender.SenderType)
		record.Sender = get(sender.SenderName)
		record.PeerID = record.SenderID
	}
	return record
}

func archiveImageURLs(kind, id, body string) []string {
	var urls []string
	add := func(key string) {
		if address := messageResourceURL(id, key, "image"); address != "" && !slices.Contains(urls, address) {
			urls = append(urls, address)
		}
	}
	switch kind {
	case "image":
		var image struct {
			ImageKey string `json:"image_key"`
		}
		if json.Unmarshal([]byte(body), &image) == nil {
			add(image.ImageKey)
		}
	case "post":
		post := decodePost([]byte(body))
		for _, paragraphs := range [][][]postNode{post.Content, post.ContentV2} {
			for _, paragraph := range paragraphs {
				for _, node := range paragraph {
					if node.Tag == "img" {
						add(node.ImageKey)
					}
				}
			}
		}
	}
	return urls
}

func openArchiveDB(dir string) (*sql.DB, *os.File, error) {
	lock, err := os.OpenFile(filepath.Join(dir, "openviking.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, nil, errors.New("已有 watch 进程在使用 OpenViking 入库队列")
		}
		return nil, nil, err
	}
	db, err := openPrivateArchiveDB(filepath.Join(dir, "openviking.sqlite"))
	if err != nil {
		lock.Close()
		return nil, nil, err
	}
	return db, lock, nil
}

func openPrivateArchiveDB(path string) (*sql.DB, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	info, statErr := file.Stat()
	if err := errors.Join(statErr, file.Close()); err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, errors.New("OpenViking 入库队列必须是仅当前用户可读写的普通文件（chmod 600）")
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(archiveSchema); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (archive *watchArchive) poll(ctx context.Context, client *imClient, c chat, state *watchChat,
	started, until time.Time, lookback time.Duration, emit func(message)) watchPollResult {
	identity, _ := archive.identity.Load().(archiveIdentity)
	if state.archiveStarted.IsZero() {
		if err := archive.restoreCheckpoint(ctx, identity, c.id, state, started); err != nil {
			reportWatchError(ctx, c, "OpenViking 本地游标", "openviking", err, state.errors)
			return watchPollResult{}
		}
	}
	before := state.archiveCheckpoint()
	seenBefore := maps.Clone(state.seen)
	records := make(map[string]archiveMessage)
	result := state.poll(ctx, client, c, started, until, lookback, emit, func(messages []message, threadID string) {
		for _, m := range messages {
			if m.at.Before(state.archiveStarted) {
				continue
			}
			if previous, ok := records[m.id]; !ok || previous.ThreadID == "" {
				records[m.id] = makeArchiveMessage(c, m, threadID)
			}
		}
	})

	err := archive.saveCheckpoint(ctx, identity, c, state.archiveCheckpoint(), records)
	if err != nil {
		state.restoreArchive(before)
		maps.Copy(state.seen, seenBefore)
		result.complete = false
	}
	reportWatchError(ctx, c, "OpenViking 本地队列", "openviking", err, state.errors)
	return result
}

func (archive *watchArchive) restoreCheckpoint(ctx context.Context, identity archiveIdentity, chatID string, state *watchChat, started time.Time) error {
	var data []byte
	err := archive.db.QueryRowContext(ctx, `SELECT data FROM archive_checkpoints WHERE source IN (?, ?) AND chat_id = ?
		ORDER BY source = ? DESC LIMIT 1`, identity.source, identity.legacySource, chatID, identity.source).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		state.archiveStarted = started
		return nil
	}
	if err != nil {
		return err
	}
	var checkpoint archiveCheckpoint
	if err := json.Unmarshal(data, &checkpoint); err != nil {
		return err
	}
	state.restoreArchive(checkpoint)
	return nil
}

func (archive *watchArchive) saveCheckpoint(ctx context.Context, identity archiveIdentity, c chat, checkpoint archiveCheckpoint,
	records map[string]archiveMessage) error {
	data, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	tx, err := archive.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, message := range records {
		content, err := json.Marshal(message)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO archive_messages
			(source, message_id, chat_id, chat_mode, created_at, peer_id, content)
			SELECT ?, ?, ?, ?, ?, ?, ? WHERE NOT EXISTS (
				SELECT 1 FROM archive_messages WHERE source = ? AND message_id = ?
			) ON CONFLICT(source, message_id) DO NOTHING`,
			identity.source, message.MessageID, c.id, c.mode, message.CreatedAt.UnixMilli(), message.PeerID,
			string(content), identity.legacySource, message.MessageID)
		if err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO archive_checkpoints(source, chat_id, data) VALUES (?, ?, ?)
		ON CONFLICT(source, chat_id) DO UPDATE SET data = excluded.data`, identity.source, c.id, string(data))
	if err != nil {
		return err
	}
	return tx.Commit()
}

type archiveDelivery struct {
	source    string
	messageID string
	chatID    string
	createdAt int64
	peerID    string
	content   string
	state     string
	legacy    bool
}

func (item archiveDelivery) sessionID() string {
	id := fmt.Sprintf("feishu_%s_%s", item.source, item.chatID)
	// Preserve the original destination for legacy deliveries, including ambiguous writes.
	if !item.legacy && len(id) > 64 {
		return "feishu_" + archiveDigest(id)
	}
	return id
}

func (archive *watchArchive) nextDelivery(ctx context.Context, identity archiveIdentity) (*archiveDelivery, error) {
	rows, err := archive.db.QueryContext(ctx, `WITH first_in_chat AS (
		SELECT source, message_id, chat_id, chat_mode, created_at, peer_id, content, state, retry_at,
			ROW_NUMBER() OVER (PARTITION BY source, chat_id ORDER BY created_at, message_id) AS position
		FROM archive_messages WHERE source IN (?, ?) AND state != 'sent'
	)
	SELECT source, message_id, chat_id, chat_mode, created_at, peer_id, content, state
	FROM first_in_chat WHERE position = 1 AND retry_at <= ? ORDER BY created_at, message_id`,
		identity.source, identity.legacySource, time.Now().UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item archiveDelivery
		var mode string
		if err := rows.Scan(&item.source, &item.messageID, &item.chatID, &mode, &item.createdAt, &item.peerID, &item.content, &item.state); err != nil {
			return nil, err
		}
		item.legacy = item.source == identity.legacySource
		if archive.includes(chat{id: item.chatID, mode: mode}) {
			return &item, nil
		}
	}
	return nil, rows.Err()
}

func (archive *watchArchive) updateDelivery(ctx context.Context, item *archiveDelivery, state string, retryAt int64) error {
	_, err := archive.db.ExecContext(ctx, `UPDATE archive_messages SET state = ?, retry_at = ?,
		content = CASE WHEN ? = 'sent' THEN NULL ELSE content END
		WHERE source = ? AND message_id = ?`, state, retryAt, state, item.source, item.messageID)
	return err
}
