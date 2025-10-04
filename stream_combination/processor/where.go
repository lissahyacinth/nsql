package processor

import (
	"context"
	"stream_combination/models"

	"github.com/google/uuid"
)

// WhereFilter - Filter to events that meet `WhereFilter.cond`
type WhereFilter struct {
	id        uuid.UUID
	cond      func(like models.EventLike) bool
	messageCh chan models.EventLike
}

func NewWhereFilter(cond func(like models.EventLike) bool, bufferSize int) (*WhereFilter, error) {
	if bufferSize <= 0 {
		bufferSize = 50 // default
	}
	return &WhereFilter{
		id:        uuid.New(),
		cond:      cond,
		messageCh: make(chan models.EventLike, bufferSize),
	}, nil
}

func (wf *WhereFilter) ID() string {
	return wf.id.String()
}

func (wf *WhereFilter) Add(ctx context.Context, event models.EventLike) error {
	if wf.cond(event) {
		wf.messageCh <- event
	}
	return nil
}

func (wf *WhereFilter) Results(ctx context.Context, consumerID string, errorCh chan<- error) <-chan models.EventLike {
	return wf.messageCh
}

func (wf *WhereFilter) Close() error {
	close(wf.messageCh)
	return nil
}
