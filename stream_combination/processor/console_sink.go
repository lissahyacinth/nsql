package processor

import (
	"context"
	"log"
	"stream_combination/models"

	"github.com/google/uuid"
)

type ConsoleSink struct {
	id uuid.UUID
}

func NewConsoleSink() ConsoleSink {
	return ConsoleSink{
		id: uuid.New(),
	}
}

func (cs *ConsoleSink) ID() string {
	return cs.id.String()
}

func (cs *ConsoleSink) Add(ctx context.Context, event models.EventLike) error {
	log.Println(event)
	return nil
}

func (cs *ConsoleSink) Results(ctx context.Context, consumerID string, errorCh chan<- error) <-chan models.EventLike {
	messageCh := make(chan models.EventLike)
	return messageCh
}

func (cs *ConsoleSink) Close() error {
	return nil
}
