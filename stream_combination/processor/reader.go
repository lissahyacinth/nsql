package processor

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"stream_combination/models"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
)

type SubjectReader struct {
	id      uuid.UUID
	js      jetstream.JetStream
	subject string
}

func NewSubjectReader(js jetstream.JetStream, subject string) (*SubjectReader, error) {
	return &SubjectReader{
		id:      uuid.New(),
		js:      js,
		subject: subject,
	}, nil
}

func (sr *SubjectReader) ID() string {
	return sr.id.String()
}

func (sr *SubjectReader) Add(ctx context.Context, event models.EventLike) error {
	return nil
}

func (sr *SubjectReader) Results(ctx context.Context, consumerID string, errorCh chan<- error) <-chan models.EventLike {
	messageCh := make(chan models.EventLike)

	go func() {
		defer close(messageCh)

		// Create consumer on-demand with unique ID
		consumer, err := sr.js.CreateOrUpdateConsumer(ctx, sr.subject, jetstream.ConsumerConfig{
			Name:      fmt.Sprintf("%s-%s-reader", sr.subject, consumerID),
			AckPolicy: jetstream.AckExplicitPolicy,
		})
		slog.Info("Consumer created for subject", "subject", sr.subject)
		if err != nil {
			slog.ErrorContext(ctx, "Error creating consumer", "error", err)
			select {
			case errorCh <- fmt.Errorf("failed to create consumer: %w", err):
			case <-ctx.Done():
			}
			return
		}

		iter, err := consumer.Consume(func(msg jetstream.Msg) {
			meta, err := msg.Metadata()
			if err != nil {
				log.Println("Error getting metadata:", err)
				msg.Ack()
				return
			}
			// TODO: Currently we assume all messages are JSON serialised
			event, err := models.NewEventFromJson(meta.Timestamp, msg.Data())
			if err != nil {
				log.Printf("error creating Event: %v", err)
				msg.Ack()
				return
			}
			msg.Ack()
			select {
			case messageCh <- event:
			case <-ctx.Done():
				return
			}
		})

		if err != nil {
			select {
			case errorCh <- fmt.Errorf("failed to consume: %w", err):
			case <-ctx.Done():
			}
			return
		}

		<-ctx.Done()
		iter.Stop()
	}()

	return messageCh
}

func (sr *SubjectReader) Close() error {
	// Cancellation happens within `Results`
	return nil
}
