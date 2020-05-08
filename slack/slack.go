package slack

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	time.Sleep(time.Duration(time.Second))

	if s.Prefix != "" {
		message = "[" + s.Prefix + "] " + message
	}

	b, _ := json.Marshal(s.newMessage(message, color))

	req, _ := http.NewRequest("POST", s.Url, bytes.NewBuffer(b))
	req.Header.Set("Content-Type", "application/json")

	_, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error on http do: %v", err)
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
