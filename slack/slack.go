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

type msg struct {
	Channel    string       `json:"channel,omitempty"`
	Text       string       `json:"text,omitempty"`
	Attchments []attachment `json:"attachments"`
}

type attachment struct {
	Title string `json:"title,omitempty"`
	Text  string `json:"text,omitempty"`
	Color string `json:"color,omitempty"`
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
		[]attachment{
			{s.Title,
				message,
				colorCode(color)},
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
