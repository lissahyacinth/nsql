package processor

import (
	"context"
	"stream_combination/models"

	"github.com/google/uuid"
)

// ColumnFilter Filter an event down to a fixed number of fields
type ColumnFilter struct {
	id        uuid.UUID
	fields    []string
	messageCh chan models.EventLike
}

func NewColumnFilter(fields []string, bufferSize int) (*ColumnFilter, error) {
	if bufferSize <= 0 {
		bufferSize = 50 // default
	}
	return &ColumnFilter{
		id:        uuid.New(),
		fields:    fields,
		messageCh: make(chan models.EventLike, bufferSize),
	}, nil
}

func (cf *ColumnFilter) ID() string {
	return cf.id.String()
}

func (cf *ColumnFilter) Add(ctx context.Context, event models.EventLike) error {
	data := make(map[string]interface{})
	for _, field := range cf.fields {
		fieldData := event.GetField(field)
		if fieldData == nil {
			continue
		}
		data[field] = fieldData
	}
	cf.messageCh <- models.NewEvent(event.GetTimestamp(), data)
	return nil
}

func (cf *ColumnFilter) Results(ctx context.Context, consumerID string, errorCh chan<- error) <-chan models.EventLike {
	return cf.messageCh
}

func (cf *ColumnFilter) Close() error {
	close(cf.messageCh)
	return nil
}
