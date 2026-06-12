# Conveyor — common developer commands.
# Run `make help` to see everything.

.DEFAULT_GOAL := help
.PHONY: help up down logs ps api worker producer build test test-race tidy fmt vet lint load clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

up: ## Start the full infra stack (Kafka, Postgres, Prometheus, Grafana, Kafka-UI)
	docker compose up -d

down: ## Stop the stack (keeps data volume)
	docker compose down

clean: ## Stop the stack and delete all data
	docker compose down -v

logs: ## Tail logs from all containers
	docker compose logs -f

ps: ## Show container status
	docker compose ps

api: ## Run the API server on the host
	go run ./cmd/api

worker: ## Run a worker on the host
	go run ./cmd/worker

producer: ## Enqueue a batch of demo jobs (usage: make producer N=1000)
	go run ./cmd/producer -n $(or $(N),100)

build: ## Compile all binaries into ./bin
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker
	go build -o bin/producer ./cmd/producer

test: ## Run unit tests
	go test ./...

test-race: ## Run tests with the race detector
	go test -race ./...

tidy: ## Sync go.mod / go.sum
	go mod tidy

fmt: ## Format the code
	go fmt ./...

vet: ## Run go vet
	go vet ./...

load: ## Run a load test (usage: make load N=50000)
	go run ./cmd/producer -n $(or $(N),50000) -rate 0
