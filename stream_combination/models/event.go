package models

import (
	"encoding/json"
	"fmt"
	"time"
)

type EventLike interface {
	GetTimestamp() time.Time
	GetString(string) string
	GetField(string) interface{}
	fmt.Stringer
}

type Event struct {
	Timestamp time.Time
	data      map[string]interface{}
}

func (e Event) GetTimestamp() time.Time {
	return e.Timestamp
}

func (e Event) GetString(fieldName string) string {
	value, exists := e.data[fieldName]
	if !exists {
		return ""
	}
	return fmt.Sprintf("%v", value)
}

func (e Event) GetField(fieldName string) interface{} {
	value, exists := e.data[fieldName]
	if !exists {
		return nil
	}
	return value
}

func NewEventFromJson(timestamp time.Time, msgData []byte) (*Event, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(msgData, &data); err != nil {
		return nil, fmt.Errorf("error unmarshalling Event: %v", err)
	}
	return &Event{Timestamp: timestamp, data: data}, nil
}

func NewEvent(timestamp time.Time, data map[string]interface{}) *Event {
	return &Event{Timestamp: timestamp, data: data}
}

func (e Event) String() string {
	return fmt.Sprintf("Event{%v}", e.data)
}
