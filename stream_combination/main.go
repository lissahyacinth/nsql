package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"stream_combination/parser"
	"stream_combination/processor"
	"time"

	"github.com/alecthomas/repr"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func publishExamples(js jetstream.JetStream, stream string) {
	ctx := context.Background()
	for i := 0; i < 20; i++ {
		CorrelationID := fmt.Sprintf("%v", i)
		dataA, _ := json.Marshal(map[string]interface{}{
			"CorrelationID": CorrelationID,
			"StringPayload": fmt.Sprintf("This is message %v", i),
			"BytePayload":   []byte(fmt.Sprintf("This is message %v", i)),
		})
		_, err := js.Publish(ctx, stream+".messages", dataA)
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func main() {
	query := "SELECT StringPayload FROM streamA WHERE CorrelationID = 1"
	result, err := parser.ParseSQL(query)
	repr.Println(result)

	nc, err := nats.Connect(nats.DefaultURL)

	if err != nil {
		log.Fatal(err)
	}

	// Handle draining and close at connection end
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		done := make(chan error, 1)
		go func() { done <- nc.Drain() }() // Drain in Go Routine

		select {
		case err := <-done:
			if err != nil {
				log.Printf("Drain failed, forcing close: %v", err)
				nc.Close()
			}
		case <-ctx.Done():
			log.Printf("Drain timed out, forcing close")
			nc.Close()
		}
		nc.Close()
	}()

	// Connect to JetStream
	js, _ := jetstream.New(nc)

	cfgA := jetstream.StreamConfig{
		Name:     "streamA",
		Subjects: []string{"streamA", "streamA.>"},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	js.CreateStream(ctx, cfgA)
	publishExamples(js, "streamA")

	builder := processor.NewProcessorBuilder(js)
	result.Visit(builder)

	// Build and run
	errorCh := make(chan error, 10)
	pipeline, err := builder.Build(ctx, errorCh)

	if err != nil {
		log.Fatal(err)
	}

	// Run pipeline in background and listen for errors
	go func() {
		if err := pipeline.Run(ctx); err != nil {
			log.Printf("Pipeline error: %v", err)
		}
	}()

	// Listen for errors or wait for completion
	select {
	case err := <-errorCh:
		log.Printf("Error: %v", err)
	case <-ctx.Done():
		log.Println("Deadline exceeded - shutting down")
	case <-time.After(30 * time.Second):
		log.Println("Test complete")
	}
}
