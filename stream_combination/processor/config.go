package processor

import (
	"github.com/nats-io/nats.go/jetstream"
	"stream_combination/models"
	"time"
)

type ProcessorConfig struct {
	Sources   []StreamSource           `yaml:"sources"`
	Selectors []models.SelectCondition `yaml:"selectors"`
	Output    StreamOutput             `yaml:"output"`
	Window    *WindowConfig            `yaml:"window,omitempty"`
	Join      *JoinConfig              `yaml:"join,omitempty"`
}

type StreamSource struct {
	Stream   string         `yaml:"stream"`  // NATS stream name
	Subject  string         `yaml:"subject"` // Subject pattern to subscribe to
	Consumer ConsumerConfig `yaml:"consumer"`
}

type ConsumerConfig struct {
	Name          string                  `yaml:"name"`
	Durable       bool                    `yaml:"durable"`
	DeliverPolicy jetstream.DeliverPolicy `yaml:"deliver_policy"` // All, Last, New, etc.
	AckPolicy     jetstream.AckPolicy     `yaml:"ack_policy"`     // Explicit, None, All
	MaxDeliver    int                     `yaml:"max_deliver,omitempty"`
	FilterSubject string                  `yaml:"filter_subject,omitempty"`
}

type StreamOutput struct {
	Stream  string            `yaml:"stream"`  // Target NATS stream
	Subject string            `yaml:"subject"` // Subject to publish to
	Headers map[string]string `yaml:"headers,omitempty"`
	MaxAge  time.Duration     `yaml:"max_age,omitempty"`
	MaxMsgs int64             `yaml:"max_msgs,omitempty"`
}

type WindowConfig struct {
	Type     WindowType    `yaml:"type"`
	Duration time.Duration `yaml:"duration"`
	Advance  time.Duration `yaml:"advance,omitempty"` // For sliding windows
}

type JoinConfig struct {
	Type      JoinType `yaml:"type"`
	OnFields  []string `yaml:"on_fields"`
	TimeField string   `yaml:"time_field,omitempty"`
}

type WindowType string

const (
	WindowTypeSliding  WindowType = "sliding"
	WindowTypeTumbling WindowType = "tumbling"
	WindowTypeSession  WindowType = "session"
)

type JoinType string

const (
	JoinTypeInner JoinType = "inner"
	JoinTypeLeft  JoinType = "left"
	JoinTypeRight JoinType = "right"
	JoinTypeOuter JoinType = "outer"
)
