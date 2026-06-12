// Package config loads all runtime settings from environment variables, with
// sensible defaults that match docker-compose.yml. Keeping config in one place
// (and reading it from the environment) is a 12-factor-app best practice: the
// same binary runs in dev, CI, and prod with no code changes.
package config

import (
	"os"
	"strconv"
	"strings"
)

// Config holds every setting the api and worker need.
type Config struct {
	// Kafka
	KafkaBrokers []string // e.g. ["localhost:9092"]
	JobsTopic    string   // main work topic
	RetryTopic   string   // delayed-retry topic (derived from JobsTopic)
	DLQTopic     string   // dead-letter topic (derived from JobsTopic)
	KafkaGroup   string   // consumer group id shared by all workers

	// Postgres
	DBDSN string

	// API server
	APIAddr string

	// Worker
	WorkerMetricsAddr string
	WorkerConcurrency int
	MaxRetries        int

	LogLevel string
}

// Load reads configuration from the environment.
func Load() Config {
	topic := getenv("CONVEYOR_KAFKA_TOPIC", "jobs")
	return Config{
		KafkaBrokers:      splitAndTrim(getenv("CONVEYOR_KAFKA_BROKERS", "localhost:9092")),
		JobsTopic:         topic,
		RetryTopic:        topic + ".retry",
		DLQTopic:          topic + ".dlq",
		KafkaGroup:        getenv("CONVEYOR_KAFKA_GROUP", "conveyor-workers"),
		DBDSN:             getenv("CONVEYOR_DB_DSN", "postgres://conveyor:conveyor@localhost:5432/conveyor?sslmode=disable"),
		APIAddr:           getenv("CONVEYOR_API_ADDR", ":8080"),
		WorkerMetricsAddr: getenv("CONVEYOR_WORKER_METRICS_ADDR", ":8081"),
		WorkerConcurrency: getenvInt("CONVEYOR_WORKER_CONCURRENCY", 8),
		MaxRetries:        getenvInt("CONVEYOR_MAX_RETRIES", 5),
		LogLevel:          getenv("CONVEYOR_LOG_LEVEL", "info"),
	}
}

func getenv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func splitAndTrim(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
