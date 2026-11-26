package main

import (
	"encoding/json"
	"net/url"
	"regexp"
	"slices"
	"strconv"
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
	case "system":
		return previewSystem(body)
	case "file":
		return m.previewFile(body)
	case "image":
		var content struct {
			ImageKey string `json:"image_key"`
		}
		_ = json.Unmarshal(body, &content)
		return m.previewWithLinks("[图片]", m.imageLink(content.ImageKey))
	}
	return "[" + kind + "]"
}

func (m message) previewFile(body []byte) string {
	var file struct {
		Name     string          `json:"file_name"`
		Key      string          `json:"file_key"`
		Size     json.RawMessage `json:"file_size"`
		MIMEType string          `json:"mime_type"`
	}
	_ = json.Unmarshal(body, &file)

	parts := []string{"[文件]"}
	if name := terminalText(file.Name, 200); name != "" {
		parts = append(parts, name)
	}
	if size, err := strconv.ParseInt(strings.Trim(string(file.Size), `"`), 10, 64); err == nil && size >= 0 {
		parts = append(parts, strconv.FormatInt(size, 10)+" 字节")
	}
	if mimeType := terminalText(file.MIMEType, 80); mimeType != "" {
		parts = append(parts, mimeType)
	}
	summary := strings.Join(parts, " ")
	if address := messageResourceURL(m.id, file.Key, "file"); address != "" {
		return m.previewWithLinks(summary, "文件资源 API "+address)
	}
	if key := terminalText(file.Key, 100); key != "" {
		return m.previewWithLinks(summary, "文件标识 "+key)
	}
	return m.previewWithLinks(summary)
}

var systemPlaceholder = regexp.MustCompile(`\{([A-Za-z0-9_]+)\}`)

func previewSystem(body []byte) string {
	var fields map[string]any
	if json.Unmarshal(body, &fields) == nil {
		template := systemText(fields["template"])
		actor := systemText(fields["name"])
		if actor == "" {
			actor = systemText(fields["from_user"])
		}
		if strings.EqualFold(template, "{name} clipped content to top.") {
			if actor == "" {
				actor = "有人"
			}
			template = actor + "置顶了一条消息"
		} else {
			template = systemPlaceholder.ReplaceAllStringFunc(template, func(placeholder string) string {
				key := placeholder[1 : len(placeholder)-1]
				if value := systemText(fields[key]); value != "" {
					return value
				}
				if key == "name" && actor != "" {
					return actor
				}
				return placeholder
			})
		}
		for _, text := range []string{template, systemText(fields["text"]), systemText(fields["content"])} {
			if text = terminalText(text, 300); text != "" {
				return text
			}
		}
	}
	if text := terminalText(string(body), 300); text != "" && text != "{}" && text != "null" {
		return "[系统消息] " + text
	}
	return "[系统消息]"
}

func systemText(value any) string {
	switch value := value.(type) {
	case string:
		return strings.TrimSpace(value)
	case []any:
		var items []string
		for _, item := range value {
			if text := systemText(item); text != "" {
				items = append(items, text)
			}
		}
		return strings.Join(items, "、")
	case map[string]any:
		if translations, ok := value["i18n_text"].(map[string]any); ok {
			if text, ok := translations["zh_cn"].(string); ok && strings.TrimSpace(text) != "" {
				return strings.TrimSpace(text)
			}
		}
		if text, ok := value["text"].(string); ok {
			return strings.TrimSpace(text)
		}
	}
	return ""
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
	address := messageResourceURL(m.id, key, "image")
	if address == "" {
		return ""
	}
	return "图片资源 API " + address
}

func messageResourceURL(messageID, key, kind string) string {
	if messageID == "" || key == "" {
		return ""
	}
	return imBaseURL + "/open-apis/im/v1/messages/" +
		url.PathEscape(messageID) + "/resources/" + url.PathEscape(key) + "?type=" + url.QueryEscape(kind)
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
