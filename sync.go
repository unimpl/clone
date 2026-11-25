package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode"
)

const watchOverlap = 90 * time.Second

func printStats(ctx context.Context, client *imClient, duration time.Duration, until time.Time) error {
	since := until.Add(-duration)
	chats, err := client.chats(ctx)
	if err != nil {
		return fmt.Errorf("列出飞书私聊和群聊: %w", err)
	}
	type count struct {
		chat
		messages   int
		failed     int
		restricted int
	}
	counts := make([]count, len(chats))
	fmt.Fprintf(os.Stderr, "正在统计 %d 个飞书会话。\n", len(chats))
	scanChats(ctx, len(chats), func(index int) {
		c := chats[index]
		messages, failed, restricted := countMessages(ctx, client, c, since, until)
		counts[index] = count{chat: c, messages: messages, failed: failed, restricted: restricted}
	})
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var privateChats, privateMessages, groupMessages, failed, restricted int
	for _, entry := range counts {
		if entry.mode == "p2p" {
			privateChats++
			privateMessages += entry.messages
		} else {
			groupMessages += entry.messages
		}
		failed += entry.failed
		restricted += entry.restricted
	}
	sort.Slice(counts, func(i, j int) bool {
		if counts[i].messages == counts[j].messages {
			return counts[i].id < counts[j].id
		}
		return counts[i].messages > counts[j].messages
	})
	fmt.Printf("飞书消息统计 %s 至 %s\n", since.Format(time.DateTime), until.Format(time.DateTime))
	fmt.Printf("私聊 %d 个，%d 条 | 群聊 %d 个，%d 条 | 合计 %d 条\n",
		privateChats, privateMessages, len(chats)-privateChats, groupMessages, privateMessages+groupMessages)
	for _, entry := range counts[:min(10, len(counts))] {
		if entry.messages == 0 {
			break
		}
		fmt.Printf("%6d  [%s] %s\n", entry.messages, entry.label(), terminalText(entry.name, 100))
	}
	if failed != 0 {
		return fmt.Errorf("统计不完整，有 %d 个会话或话题读取失败，其中 %d 个因飞书会话限制无法访问", failed, restricted)
	}
	return nil
}

func countMessages(ctx context.Context, client *imClient, c chat, since, until time.Time) (int, int, int) {
	messages, err := client.messages(ctx, "chat", c.id, since, until)
	if err != nil {
		if ctx.Err() == nil {
			warn(c, "会话消息", err)
		}
		return 0, 1, restrictedChat(err)
	}
	unique := make(map[string]bool, len(messages))
	threads := make(map[string]bool)
	for _, message := range messages {
		unique[message.id] = true
		if c.mode != "p2p" && message.thread != "" {
			threads[message.thread] = true
		}
	}
	var failed, restricted int
	for thread := range threads {
		replies, err := client.messages(ctx, "thread", thread, since, until)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			warn(c, "话题回复", err)
			failed++
			restricted += restrictedChat(err)
			continue
		}
		for _, reply := range replies {
			unique[reply.id] = true
		}
	}
	return len(unique), failed, restricted
}

func restrictedChat(err error) int {
	var response imAPIError
	if errors.As(err, &response) && response.code == 231203 {
		return 1
	}
	return 0
}

func scanChats(ctx context.Context, count int, scan func(int)) {
	jobs := make(chan int)
	var workers sync.WaitGroup
	for range min(count, 8) {
		workers.Go(func() {
			for index := range jobs {
				scan(index)
			}
		})
	}
sendJobs:
	for index := range count {
		select {
		case <-ctx.Done():
			break sendJobs
		case jobs <- index:
		}
	}
	close(jobs)
	workers.Wait()
}

type watchThread struct {
	cursor time.Time
	active time.Time
}

type watchChat struct {
	cursor         time.Time
	threads        map[string]*watchThread
	seen           map[string]time.Time
	errors         map[string]string
	archiveStarted time.Time
}

type watchSchedule struct {
	started         time.Time
	lookback        time.Duration
	recentLimit     int
	backgroundBatch int
	lastRecent      time.Time
	states          map[string]*watchChat
	backlog         []chat
	preferences     watchPreferences
	identity        watchIdentity
	identityToken   string
	archive         *watchArchive
}

func watch(ctx context.Context, client *imClient, auth authStore, interval, lookback time.Duration) error {
	excluded, err := loadWatchExclusions(auth.dir)
	if err != nil {
		return err
	}
	archive, err := newWatchArchive(ctx, auth.dir, auth.AppID, excluded)
	if err != nil {
		return err
	}
	if archive != nil {
		defer archive.close()
	}
	schedule := watchSchedule{
		started: time.Now(), lookback: lookback,
		recentLimit:     min(20, max(5, int(interval/time.Second)*3)),
		backgroundBatch: min(40, max(4, int(interval/time.Second)*2)),
		states:          make(map[string]*watchChat),
		preferences:     newWatchPreferences(excluded),
		archive:         archive,
	}
	var recentError string
	fmt.Printf("监听飞书私聊和群聊，轮询间隔 %s。按 Ctrl+C 停止。\n", interval)
	for {
		roundStart := time.Now()
		token, err := auth.accessToken(ctx)
		if err == nil {
			client.token = token
			err = schedule.pollRound(ctx, client)
		}
		if err == nil {
			recentError = ""
		} else if ctx.Err() == nil && recentError != err.Error() {
			fmt.Fprintf(os.Stderr, "飞书同步失败，将继续重试：%s\n", terminalText(err.Error(), 400))
			recentError = err.Error()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Until(roundStart.Add(interval))):
		}
	}
}

func (schedule *watchSchedule) pollRound(ctx context.Context, client *imClient) error {
	if err := schedule.refreshIdentity(ctx, client); err != nil {
		return err
	}
	checked := make(map[string]bool)
	schedule.preferences.refreshPinned(ctx, client, time.Now())
	schedule.pollBatch(ctx, client, schedule.preferences.pinned, checked)
	recentErr := schedule.pollRecent(ctx, client, checked)

	var backlogErr error
	if len(schedule.backlog) == 0 && ctx.Err() == nil {
		schedule.backlog, backlogErr = client.chatsByOrder(ctx, "ByCreateTimeAsc")
	}
	count := min(schedule.backgroundBatch, len(schedule.backlog))
	schedule.pollBatch(ctx, client, schedule.backlog[:count], checked)
	schedule.backlog = schedule.backlog[count:]
	return errors.Join(recentErr, backlogErr)
}

func (schedule *watchSchedule) refreshIdentity(ctx context.Context, client *imClient) error {
	if schedule.identity.OpenID != "" && schedule.identityToken == client.token {
		return nil
	}
	identity, err := client.currentWatchIdentity(ctx)
	if err != nil {
		return fmt.Errorf("识别飞书登录账号: %w", err)
	}
	if schedule.identity.OpenID != "" && schedule.identity.OpenID != identity.OpenID {
		schedule.started = time.Now()
		schedule.lastRecent = time.Time{}
		clear(schedule.states)
		schedule.backlog = nil
		schedule.preferences = newWatchPreferences(schedule.preferences.excluded)
	}
	schedule.identity = identity
	schedule.identityToken = client.token
	if schedule.archive != nil {
		schedule.archive.setIdentity(identity)
	}
	return nil
}

func (schedule *watchSchedule) pollRecent(ctx context.Context, client *imClient, checked map[string]bool) error {
	until := time.Now()
	complete := true
	scanned := 0
	page := ""
	seenPages := make(map[string]bool)
pages:
	for scanned < schedule.recentLimit {
		candidates, next, err := client.chatPage(ctx, page, schedule.recentLimit, "ByActiveTimeDesc")
		if err != nil {
			return err
		}
		for _, c := range schedule.preferences.eligible(ctx, client, candidates, checked, time.Now()) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			checked[c.id] = true
			scanned++
			result := schedule.pollChat(ctx, client, c, schedule.stateFor(c), until)
			complete = complete && result.complete
			if result.quietSince(schedule.lastRecent) || scanned == schedule.recentLimit {
				break pages
			}
		}
		if next == "" {
			break
		}
		if seenPages[next] {
			return errors.New("最近活跃会话分页游标重复")
		}
		seenPages[next] = true
		page = next
	}
	if complete {
		schedule.lastRecent = until
	}
	return nil
}

func (schedule *watchSchedule) pollBatch(ctx context.Context, client *imClient, candidates []chat, checked map[string]bool) {
	chats := schedule.preferences.eligible(ctx, client, candidates, checked, time.Now())
	states := make([]*watchChat, len(chats))
	for index, c := range chats {
		checked[c.id] = true
		states[index] = schedule.stateFor(c)
	}
	until := time.Now()
	scanChats(ctx, len(chats), func(index int) {
		schedule.pollChat(ctx, client, chats[index], states[index], until)
	})
}

func (schedule *watchSchedule) stateFor(c chat) *watchChat {
	if state := schedule.states[c.id]; state != nil {
		return state
	}
	cursor := schedule.started
	if c.mode != "p2p" {
		cursor = cursor.Add(-schedule.lookback)
	}
	state := &watchChat{
		cursor: cursor, threads: make(map[string]*watchThread),
		seen: make(map[string]time.Time), errors: make(map[string]string),
	}
	schedule.states[c.id] = state
	return state
}

func (schedule *watchSchedule) pollChat(ctx context.Context, client *imClient, c chat, state *watchChat, until time.Time) watchPollResult {
	emit := func(m message) {
		if text, show := schedule.notification(c, m); show {
			fmt.Printf("%s [%s·%s] %s：%s\n", m.at.Local().Format(time.DateTime), c.label(), terminalText(c.name, 80), senderName(m), text)
		}
	}
	if schedule.archive != nil && schedule.archive.includes(c) {
		return schedule.archive.poll(ctx, client, c, state, schedule.started, until, schedule.lookback, emit)
	}
	return state.poll(ctx, client, c, schedule.started, until, schedule.lookback, emit, nil)
}

func (schedule *watchSchedule) notification(c chat, m message) (string, bool) {
	direct := schedule.identity.directlyMentioned(m)
	if schedule.preferences.mentionsOnly(c) && !direct {
		return "", false
	}
	if direct {
		return "[@我] " + preview(m), true
	}
	return preview(m), true
}

type watchPollResult struct {
	since    time.Time
	newest   time.Time
	complete bool
}

func (result watchPollResult) quietSince(cutoff time.Time) bool {
	return result.complete && !cutoff.IsZero() && !cutoff.Before(result.since) && result.newest.Before(cutoff)
}

func (state *watchChat) poll(ctx context.Context, client *imClient, c chat, started, until time.Time, lookback time.Duration, emit func(message), collect func([]message, string)) watchPollResult {
	since := state.cursor.Add(-watchOverlap)
	messages, err := client.messages(ctx, "chat", c.id, since, until)
	result := watchPollResult{since: since, complete: err == nil}
	if err == nil {
		if collect != nil {
			collect(messages, "")
		}
		state.cursor = until
		result.newest = state.emitNew(messages, started, emit)
		if c.mode != "p2p" {
			for _, m := range messages {
				if m.thread == "" || m.at.Before(until.Add(-lookback)) {
					continue
				}
				thread := state.threads[m.thread]
				if thread == nil {
					thread = &watchThread{cursor: started}
					state.threads[m.thread] = thread
				}
				thread.active = latest(thread.active, m.at)
			}
		}
	}
	reportWatchError(ctx, c, "会话消息", c.id, err, state.errors)

	oldestCursor := state.cursor
	for id, thread := range state.threads {
		if thread.active.Before(until.Add(-lookback)) {
			delete(state.threads, id)
			delete(state.errors, id)
			continue
		}
		since := thread.cursor.Add(-watchOverlap)
		replies, err := client.messages(ctx, "thread", id, since, until)
		result.since = latest(result.since, since)
		result.complete = result.complete && err == nil
		if err == nil {
			if collect != nil {
				collect(replies, id)
			}
			thread.cursor = until
			for _, reply := range replies {
				thread.active = latest(thread.active, reply.at)
			}
			result.newest = latest(result.newest, state.emitNew(replies, started, emit))
		}
		reportWatchError(ctx, c, "话题回复", id, err, state.errors)
		if thread.cursor.Before(oldestCursor) {
			oldestCursor = thread.cursor
		}
	}
	for id, at := range state.seen {
		if at.Before(oldestCursor.Add(-watchOverlap)) {
			delete(state.seen, id)
		}
	}
	return result
}

func (state *watchChat) emitNew(messages []message, started time.Time, emit func(message)) time.Time {
	var newest time.Time
	for i := len(messages) - 1; i >= 0; i-- {
		m := messages[i]
		newest = latest(newest, m.at)
		if !m.at.Before(started) && state.seen[m.id].IsZero() {
			state.seen[m.id] = m.at
			emit(m)
		}
	}
	return newest
}

func latest(first, second time.Time) time.Time {
	if first.Before(second) {
		return second
	}
	return first
}

func reportWatchError(ctx context.Context, c chat, operation, id string, err error, recent map[string]string) {
	if err == nil {
		delete(recent, id)
	} else if ctx.Err() == nil && recent[id] != err.Error() {
		warn(c, operation, err)
		recent[id] = err.Error()
	}
}

func warn(c chat, operation string, err error) {
	fmt.Fprintf(os.Stderr, "[%s·%s] 读取%s失败：%s\n", c.label(), terminalText(c.name, 80), operation, terminalText(err.Error(), 400))
}

func senderName(m message) string {
	if m.raw.Sender != nil {
		for _, value := range []string{get(m.raw.Sender.SenderName), get(m.raw.Sender.Id)} {
			if name := terminalText(value, 80); name != "" {
				return name
			}
		}
	}
	return "未知发送者"
}

func terminalText(value string, limit int) string {
	value = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.Is(unicode.Cf, r) {
			return ' '
		}
		return r
	}, value)
	clean := []rune(strings.Join(strings.Fields(value), " "))
	if len(clean) > limit {
		return string(clean[:limit]) + "…"
	}
	return string(clean)
}
