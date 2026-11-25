package main

import (
	"encoding/json"
	"net/url"
	"slices"
	"strings"
	"unicode"
)

func preview(m message) string {
	if m.raw == nil {
		return "[消息]"
	}
	if get(m.raw.Deleted) {
		return "[已撤回]"
	}
	kind := terminalText(get(m.raw.MsgType), 30)
	if kind == "" {
		return "[消息]"
	}
	var body []byte
	if m.raw.Body != nil {
		body = []byte(get(m.raw.Body.Content))
	}
	switch kind {
	case "text":
		var content struct{ Text string }
		if json.Unmarshal(body, &content) == nil {
			if text := terminalText(m.replaceMentions(content.Text), 200); text != "" {
				return text
			}
		}
	case "post":
		return m.previewPost(decodePost(body))
	case "image":
		var content struct {
			ImageKey string `json:"image_key"`
		}
		_ = json.Unmarshal(body, &content)
		return m.previewWithLinks("[图片]", m.imageLink(content.ImageKey))
	}
	return "[" + kind + "]"
}

func (m message) replaceMentions(value string) string {
	for _, mention := range m.raw.Mentions {
		if mention != nil && get(mention.Key) != "" && get(mention.Name) != "" {
			value = strings.ReplaceAll(value, get(mention.Key), "@"+get(mention.Name))
		}
	}
	return value
}

type postNode struct {
	Tag      string `json:"tag"`
	Text     string `json:"text"`
	Href     string `json:"href"`
	UserID   string `json:"user_id"`
	UserName string `json:"user_name"`
	ImageKey string `json:"image_key"`
	Emoji    string `json:"emoji_type"`
}

type postContent struct {
	Title     string       `json:"title"`
	Content   [][]postNode `json:"content"`
	ContentV2 [][]postNode `json:"content_v2"`
}

func decodePost(body []byte) postContent {
	var post postContent
	if json.Unmarshal(body, &post) == nil && (post.Title != "" || len(post.Content)+len(post.ContentV2) > 0) {
		return post
	}
	var localized map[string]json.RawMessage
	if json.Unmarshal(body, &localized) != nil {
		return postContent{}
	}
	locales := []string{"zh_cn", "en_us", "ja_jp"}
	var others []string
	for locale := range localized {
		if !slices.Contains(locales, locale) {
			others = append(others, locale)
		}
	}
	slices.Sort(others)
	for _, locale := range append(locales, others...) {
		var translation postContent
		if json.Unmarshal(localized[locale], &translation) == nil &&
			(translation.Title != "" || len(translation.Content)+len(translation.ContentV2) > 0) {
			return translation
		}
	}
	return postContent{}
}

func (m message) previewPost(post postContent) string {
	paragraphs := post.Content
	if len(paragraphs) == 0 {
		paragraphs = post.ContentV2
	}
	var text strings.Builder
	text.WriteString(post.Title)
	var links []string
	for _, paragraph := range paragraphs {
		text.WriteByte('\n')
		for _, node := range paragraph {
			switch node.Tag {
			case "a":
				text.WriteString(node.Text)
				if address := httpLink(node.Href); address != "" {
					if node.Text == "" {
						text.WriteString("[链接]")
					}
					links = append(links, "链接 "+address)
				}
			case "at":
				text.WriteString(m.postMention(node))
			case "img":
				text.WriteString("[图片]")
				if link := m.imageLink(node.ImageKey); link != "" {
					links = append(links, link)
				}
			case "media":
				text.WriteString("[视频]")
			case "emotion":
				text.WriteString("[表情 " + node.Emoji + "]")
			case "hr":
				text.WriteString("—")
			default:
				text.WriteString(node.Text)
			}
		}
	}
	summary := terminalText(m.replaceMentions(text.String()), 200)
	if summary == "" {
		summary = "[post]"
	}
	return m.previewWithLinks(summary, links...)
}

func (m message) postMention(node postNode) string {
	if node.UserID == "all" {
		return "@所有人"
	}
	if node.UserName != "" {
		return "@" + strings.TrimPrefix(node.UserName, "@")
	}
	for _, mention := range m.raw.Mentions {
		if mention != nil && get(mention.Name) != "" && node.UserID != "" &&
			(node.UserID == get(mention.Id) || node.UserID == get(mention.Key)) {
			return "@" + get(mention.Name)
		}
	}
	for _, name := range []string{node.Text, node.UserID} {
		if name != "" {
			return "@" + strings.TrimPrefix(name, "@")
		}
	}
	return "[@提及]"
}

func (m message) imageLink(key string) string {
	address := imageResourceURL(m.id, key)
	if address == "" {
		return ""
	}
	return "图片资源 API " + address
}

func imageResourceURL(messageID, key string) string {
	if messageID == "" || key == "" {
		return ""
	}
	return imBaseURL + "/open-apis/im/v1/messages/" +
		url.PathEscape(messageID) + "/resources/" + url.PathEscape(key) + "?type=image"
}

func (m message) previewWithLinks(summary string, links ...string) string {
	parts := []string{summary}
	for _, link := range links {
		if link != "" && !slices.Contains(parts, link) {
			parts = append(parts, link)
		}
	}
	if address := httpLink(get(m.raw.MessageAppLink)); address != "" {
		parts = append(parts, "飞书原消息 "+address)
	}
	return strings.Join(parts, " | ")
}

func httpLink(value string) string {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") ||
		strings.ContainsFunc(value, func(r rune) bool {
			return unicode.IsSpace(r) || unicode.IsControl(r) || unicode.Is(unicode.Cf, r)
		}) {
		return ""
	}
	return parsed.String()
}
