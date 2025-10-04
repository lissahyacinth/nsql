package models

import (
	"fmt"
	"strings"
	"time"
)

type JoinEvent struct {
	Timestamp  time.Time
	LeftEvent  EventLike
	RightEvent EventLike
}

func NewJoinEvent(timestamp time.Time, leftEvent EventLike, rightEvent EventLike) JoinEvent {
	return JoinEvent{
		Timestamp:  timestamp,
		LeftEvent:  leftEvent,
		RightEvent: rightEvent,
	}
}

func (je JoinEvent) GetTimestamp() time.Time { return je.Timestamp }

func (je JoinEvent) GetString(fieldName string) string {
	// Handle dotted notation: "left.user_id", "right.amount"
	if strings.HasPrefix(fieldName, "left.") {
		return je.LeftEvent.GetString(strings.TrimPrefix(fieldName, "left."))
	}
	if strings.HasPrefix(fieldName, "right.") {
		return je.RightEvent.GetString(strings.TrimPrefix(fieldName, "right."))
	}
	return ""
}

func (je JoinEvent) GetField(fieldName string) interface{} {
	if strings.HasPrefix(fieldName, "left.") {
		return je.LeftEvent.GetField(strings.TrimPrefix(fieldName, "left."))
	}
	if strings.HasPrefix(fieldName, "right.") {
		return je.RightEvent.GetField(strings.TrimPrefix(fieldName, "right."))
	}
	return nil
}

func (je JoinEvent) String() string {
	return fmt.Sprintf("JoinEvent{%v, left=%s, right=%s}",
		je.Timestamp.Format(time.RFC3339),
		je.LeftEvent.String(),
		je.RightEvent.String())
}
