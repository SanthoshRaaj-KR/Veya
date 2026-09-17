.DEFAULT_GOAL := help

BIN       := bin
PKG       := ./...
VEYA_DSN  ?= postgres://veya:veya@localhost:5433/veya?sslmode=disable
VEYA_NATS ?= nats://127.0.0.1:4222

# The module cache is authoritative; nothing here reaches the network.
GO := GOFLAGS=-mod=mod go

.PHONY: help
help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Compile all binaries into ./bin
	$(GO) build -o $(BIN)/ ./cmd/...

.PHONY: test
test: ## Run unit tests (no Docker required)
	$(GO) test $(PKG)

.PHONY: test-race
test-race: ## Run unit tests under the race detector (needs a C toolchain)
	CGO_ENABLED=1 $(GO) test -race -count=2 $(PKG)

.PHONY: test-integration
test-integration: ## Run tests against live PostgreSQL and NATS (needs `make up`)
	VEYA_TEST_DSN="$(VEYA_DSN)" VEYA_TEST_NATS_URL="$(VEYA_NATS)" \
		$(GO) test -tags=integration -count=1 $(PKG)

.PHONY: vet
vet: ## Run go vet
	$(GO) vet $(PKG)

.PHONY: fmt
fmt: ## Format all Go source
	$(GO) fmt $(PKG)

.PHONY: migrate
migrate: ## Apply database migrations
	$(GO) run ./cmd/veya migrate --dsn "$(VEYA_DSN)"

.PHONY: up
up: ## Start PostgreSQL and NATS
	docker compose up -d
	@echo "waiting for postgres..."
	@until docker compose exec -T postgres pg_isready -U veya -d veya >/dev/null 2>&1; do sleep 1; done
	@echo "waiting for nats..."
	@until docker compose exec -T nats wget -q -O- http://localhost:8222/healthz >/dev/null 2>&1; do sleep 1; done
	@echo "ready. dsn: $(VEYA_DSN)  nats: $(VEYA_NATS)"

.PHONY: down
down: ## Stop containers and remove volumes
	docker compose down -v

.PHONY: demo
demo: ## Run the built-in agent end to end against PostgreSQL
	$(GO) run ./cmd/veya-runtime --dsn "$(VEYA_DSN)" --demo

.PHONY: demo-memory
demo-memory: ## Same demo, in-memory store, no Docker
	$(GO) run ./cmd/veya-runtime --store memory --demo

.PHONY: demo-jetstream
demo-jetstream: ## Same demo, delivered over NATS JetStream
	$(GO) run ./cmd/veya-runtime --dsn "$(VEYA_DSN)" --dispatch jetstream --nats "$(VEYA_NATS)" --demo

# The two halves of a distributed setup. Run `make runtime` in one terminal and
# `make worker` in another, then `veya run start` to give them something to do.
.PHONY: runtime
runtime: ## Engine only, no workers; execution is left to veya-worker
	$(GO) run ./cmd/veya-runtime --dsn "$(VEYA_DSN)" --dispatch postgres --workers 0

.PHONY: worker
worker: ## A standalone worker process
	$(GO) run ./cmd/veya-worker --dsn "$(VEYA_DSN)" --dispatch postgres --workers 4

.PHONY: clean
clean: ## Remove build artifacts
	rm -rf $(BIN)
