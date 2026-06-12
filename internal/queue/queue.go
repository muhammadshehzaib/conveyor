// Package queue is the Kafka adapter: it knows how to create topics, publish job
// messages, and build consumer-group readers. The rest of the app talks jobs;
// only this package talks Kafka.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/aryan3650/conveyor/internal/job"
)

// Producer publishes job messages to Kafka. A single Producer is safe for
// concurrent use by many goroutines.
type Producer struct {
	w *kafka.Writer
}

// NewProducer creates a durable producer.
//   - Balancer Hash: messages with the same key (the job id) always land on the
//     same partition, preserving per-job ordering.
//   - RequireAll acks: the broker confirms the write to all in-sync replicas
//     before we consider it sent. This is the durability knob — we never lose an
//     acknowledged job.
func NewProducer(brokers []string) *Producer {
	return &Producer{
		w: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Balancer:     &kafka.Hash{},
			RequiredAcks: kafka.RequireAll,
			BatchTimeout: 10 * time.Millisecond,
			Async:        false,
		},
	}
}

// Close flushes and shuts down the producer.
func (p *Producer) Close() error { return p.w.Close() }

// Publish marshals m to JSON and writes it to topic, keyed by the job id.
func (p *Producer) Publish(ctx context.Context, topic string, m job.Message) error {
	value, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}
	return p.w.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   m.Key(),
		Value: value,
	})
}

// PublishRaw writes pre-encoded bytes to a topic. Used to dead-letter a message
// we couldn't even parse, preserving the original payload for inspection.
func (p *Producer) PublishRaw(ctx context.Context, topic string, key, value []byte) error {
	return p.w.WriteMessages(ctx, kafka.Message{Topic: topic, Key: key, Value: value})
}

// EnsureTopics creates each topic if it doesn't already exist. We do this in code
// (rather than relying on auto-creation) so partition counts and the topic set
// are explicit and version-controlled.
func EnsureTopics(ctx context.Context, brokers []string, numPartitions, replicationFactor int, topics ...string) error {
	client := &kafka.Client{Addr: kafka.TCP(brokers...)}

	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, t := range topics {
		configs = append(configs, kafka.TopicConfig{
			Topic:             t,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
		})
	}

	resp, err := client.CreateTopics(ctx, &kafka.CreateTopicsRequest{Topics: configs})
	if err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	for topic, topicErr := range resp.Errors {
		// "already exists" is fine — these calls are idempotent.
		if topicErr != nil && !errors.Is(topicErr, kafka.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", topic, topicErr)
		}
	}
	return nil
}

// NewReader builds a consumer-group reader for one topic. Every worker that joins
// the same group shares the topic's partitions, so adding workers = more
// throughput (up to the partition count). We commit offsets manually with
// CommitMessages so an offset only advances after a message is fully handled —
// the foundation of at-least-once delivery.
func NewReader(brokers []string, groupID, topic string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10 << 20, // 10MB
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0, // synchronous commits (we call CommitMessages ourselves)
		StartOffset:    kafka.FirstOffset,
	})
}
