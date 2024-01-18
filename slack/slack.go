package slack

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

// notification status
const (
	OK int = iota
	Warning
	Critical
	Other
)

type Slack struct {
	Channel string
	Prefix  string
	Url     string
	Title   string
}

// msg object is in the format for slack's hook api
// https://api.slack.com/reference/messaging/payload
type msg struct {
	Channel    string       `json:"channel,omitempty"`
	Text       string       `json:"text,omitempty"`
	Attchments []attachment `json:"attachments"`
}

// attachment object for slacks web hook api
// https://api.slack.com/reference/messaging/attachments
type attachment struct {
	Title      string `json:"title,omitempty"` // title
	Text       string `json:"text,omitempty"`  // Optional `text` that appears within the attachment
	Color      string `json:"color,omitempty"` // color #36a64f
	PreText    string `json:"pretext"`         // Optional pre-text that appears above the attachment block
	AuthorName string `json:"author_name"`     // author name
}

// Sends a slack message with a level indicator
func (s *Slack) Notify(message string, level int) error {
	color := "green"
	switch level {
	case OK:
		color = "green"
	case Warning:
		color = "yellow"
	case Critical:
		color = "red"
	case Other:
		color = "blue"
	}

	// always wait 1 second before sending a message
	// do this to keep from hitting slacks rate limits.
	time.Sleep(time.Second)

	if s.Prefix != "" {
		message = "[" + s.Prefix + "] " + message
	}

	b, _ := json.Marshal(s.newMessage(message, color))

	req, _ := http.NewRequest("POST", s.Url, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")
	c := http.DefaultClient
	c.Timeout = time.Minute
	resp, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("error on http do: %v", err)
	}

	if resp != nil {
		var b []byte
		if resp.StatusCode/100 != 2 {
			if resp.Body != nil {
				b, _ = io.ReadAll(resp.Body)
			}
			log.Printf("slack: %d %q", resp.StatusCode, string(b))
		}
	}

	return err
}

func (s *Slack) newMessage(message, color string) msg {
	m := msg{
		s.Channel,
		"",
		[]attachment{{
			Title:      s.Title,
			Text:       message,
			Color:      colorCode(color),
			PreText:    "task-tools",
			AuthorName: "",
		},
		},
	}
	return m
}

func colorCode(color string) string {
	switch color {
	case "red":
		return "#FF0000"
	case "yellow":
		return "#FFFF00"
	case "green":
		return "#00FF00"
	case "blue":
		return "#2210FF"
	case "purple":
		return "#D91BFF"
	default:
		return ""
	}
}

// ******************************************************************************
// New code for the slack blocks payload, recomended to switch from attachements
type Message struct {
	Channel string   `json:"channel,omitempty"`
	Text    string   `json:"text,omitempty"`
	Blocks  []*Block `json:"blocks,omitempty"`
}

type Block struct {
	Type       string `json:"type"`
	*Text      `json:"text,omitempty"`
	Elements   []*Element `json:"elements,omitempty"`
	*Accessory `json:"accessory,omitempty"`
}

type Text struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type Element struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type Accessory struct {
	Type    string    `json:"type"`
	Options []*Option `json:"options,omitempty"`
}

// on an overflow Option you cannot have more than 5 items
type Option struct {
	*Text `json:"text,omitempty"` // type on this text must always be "plain_text"
	Value string                  `json:"value"`
}

func (s *Slack) NewMessage(msg string) *Message {
	m := &Message{
		Channel: s.Channel,
		Blocks:  make([]*Block, 0),
	}

	b := &Block{
		Type: "section",
		Text: &Text{
			Type: "mrkdwn",
			Text: msg,
		},
	}

	m.Blocks = append(m.Blocks, b)

	return m
}

func (b *Block) AddOverflowOption(msg string) error {
	if b.Accessory == nil {
		b.Accessory = &Accessory{
			Type:    "overflow",
			Options: make([]*Option, 0),
		}
	}
	if len(b.Accessory.Options) >= 5 {
		return errors.New("cannot add more than 5 items to an overflow accessory")
	}

	b.Accessory.Options = append(b.Accessory.Options, &Option{
		Text: &Text{
			Type: "plain_text",
			Text: msg,
		},
		Value: fmt.Sprintf("value-%d", len(b.Accessory.Options)-1),
	})

	return nil
}

func (m *Message) AddElements(elements ...string) {
	l := make([]*Element, 0)
	for _, e := range elements {
		l = append(l, &Element{Type: "mrkdwn", Text: e})
	}

	m.Blocks = append(m.Blocks, &Block{
		Type:     "context",
		Elements: l,
	})
}

func (m *Message) AddBlockMsg(msg string) {
	m.Blocks = append(m.Blocks, &Block{
		Type: "divider",
	})

	m.Blocks = append(m.Blocks, &Block{
		Type: "section",
		Text: &Text{
			Type: "mrkdwn",
			Text: msg,
		},
	})
}

func (s *Slack) SendMessage(m *Message) error {
	// if there isn't a message just return
	if m == nil || s == nil {
		return nil
	}
	// always wait 1 second before sending a message
	// do this to keep from hitting slacks rate limits.
	time.Sleep(time.Second)

	b, _ := json.Marshal(m)
	req, _ := http.NewRequest("POST", s.Url, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")
	c := http.DefaultClient
	c.Timeout = time.Minute
	resp, err := c.Do(req)
	if err != nil {
		return fmt.Errorf("error on http do: %v", err)
	}

	if resp != nil {
		var b []byte
		if resp.StatusCode/100 != 2 {
			if resp.Body != nil {
				b, _ = io.ReadAll(resp.Body)
			}
			log.Println("slack request response not ok", resp.Status, string(b))
		}
	}

	return err
}

// Notify sends a text message to the slack url defined
// this is a very basic message without any formatting
func Notify(url string, text string) error {
	s := &Slack{Url: url}
	return s.Notify(text, Other)
}
