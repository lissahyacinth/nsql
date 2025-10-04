package processor

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"stream_combination/models"

	"github.com/nats-io/nats.go/jetstream"
)

type Processor interface {
	ID() string
	Results(ctx context.Context, consumerID string, errorCh chan<- error) <-chan models.EventLike
	Close() error
}

type MessageProcessor interface {
	Processor
	Add(ctx context.Context, event models.EventLike) error
}

type DualInputProcessor interface {
	Processor
	AddLeft(ctx context.Context, event models.EventLike) error
	AddRight(ctx context.Context, event models.EventLike) error
}

type StreamProcessor struct {
	inputs     map[string][]<-chan models.EventLike
	Processors map[string]Processor
}

type ProcessorBuilder struct {
	JetStream  jetstream.JetStream
	aliases    map[string]string // Aliases => ProcessorID
	processors map[string]Processor
	edges      map[string][]string // processor_id -> [dependents]
}

func NewProcessorBuilder(js jetstream.JetStream) *ProcessorBuilder {
	return &ProcessorBuilder{
		JetStream:  js,
		aliases:    make(map[string]string), // Aliases => ProcessorID
		processors: make(map[string]Processor),
		edges:      make(map[string][]string),
	}
}

func (pb *ProcessorBuilder) AddAlias(alias string, processorId string) {
	pb.aliases[alias] = processorId
}

func (pb *ProcessorBuilder) AddProcessor(id string, processor MessageProcessor, dependencies ...string) {
	pb.processors[id] = processor
	for _, depID := range dependencies {
		pb.edges[depID] = append(pb.edges[depID], id)
	}
}

func (pb *ProcessorBuilder) AddDualProcessor(id string, dualProcessor DualInputProcessor, dependencies ...string) {
	pb.processors[id] = dualProcessor
	for _, depID := range dependencies {
		pb.edges[depID] = append(pb.edges[depID], id)
	}
}

func (pb *ProcessorBuilder) Build(ctx context.Context, errorCh chan<- error) (*StreamProcessor, error) {
	inputs := make(map[string][]<-chan models.EventLike)

	for fromID, dependentIDs := range pb.edges {
		if _, exists := pb.processors[fromID]; !exists {
			return nil, fmt.Errorf("processor %s not found", fromID)
		}
		fromProcessor := pb.processors[fromID]
		for _, toID := range dependentIDs {
			if _, exists := pb.processors[toID]; !exists {
				return nil, fmt.Errorf("dependent processor %s not found", toID)
			}
			consumerID := fmt.Sprintf("%s-to-%s", fromID, toID)
			resultsChan := fromProcessor.Results(ctx, consumerID, errorCh)
			inputs[toID] = append(inputs[toID], resultsChan)
		}
	}

	return &StreamProcessor{
		inputs:     inputs,
		Processors: pb.processors,
	}, nil
}

func (sp *StreamProcessor) Run(ctx context.Context) error {
	// Start consuming from inputs and feeding to processors
	for processorID, inputChannels := range sp.inputs {
		processor := sp.Processors[processorID]

		if dualProc, ok := processor.(DualInputProcessor); ok {
			slog.Info("Adding DualInputProcessor", "id", processorID)
			if len(inputChannels) != 2 && len(inputChannels) != 0 {
				return fmt.Errorf("dual processor %s requires exactly 2 inputs, got %d", processorID, len(inputChannels))
			}
			for i, inputChan := range inputChannels {
				slog.Info("Submitting processor", "inputChan", inputChan, "isLeft", i == 0)
				go func(isLeft bool, ch <-chan models.EventLike) {
					for event := range ch {
						if isLeft {
							if err := dualProc.AddLeft(ctx, event); err != nil {
								log.Printf("Error processing event: %v", err)
							}
						} else {
							if err := dualProc.AddRight(ctx, event); err != nil {
								log.Printf("Error processing event: %v", err)
							}
						}
					}
				}(i == 0, inputChan) // Initial input is 'left', second input is 'right'.
			}
		} else if singleProc, ok := processor.(MessageProcessor); ok {
			slog.Info("Adding SingleInputProcessor", "id", processorID)
			// Fan-in: merge all input channels for this processor
			go func(proc MessageProcessor, inputs []<-chan models.EventLike) {
				for _, inputChan := range inputs {
					go func(ch <-chan models.EventLike) {
						for event := range ch {
							if err := proc.Add(ctx, event); err != nil {
								log.Printf("Error processing event: %v", err)
							}
						}
					}(inputChan)
				}
			}(singleProc, inputChannels)
		}
	}

	// Keep running until context cancelled
	<-ctx.Done()
	return ctx.Err()
}
