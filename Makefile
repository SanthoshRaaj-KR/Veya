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

# Code generation for the worker protocol.
#
# The generated code is committed. That is the decision .planning §12 left
# open, and the reason is that `go build ./...` and `make test` must work on a
# machine with no protoc, no protoc-gen-go and no Python — which is every
# machine that only wants to run the Go runtime. The cost is that a .proto
# change is two commits' worth of diff; the benefit is that the build has one
# prerequisite instead of four.
#
# `make proto-tools` installs what this needs. grpcio-tools bundles protoc
# itself, so there is no separate protoc to find.
PROTO_SRC := proto/veya/worker/v1/worker.proto
PROTO_PY  := sdk/python
GOBIN     := $(shell $(GO) env GOPATH)/bin

.PHONY: proto-tools
proto-tools: ## Install the protobuf code generators
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	python -m pip install --quiet grpcio-tools

.PHONY: proto
proto: ## Regenerate Go and Python stubs from the .proto
	PATH="$(GOBIN):$$PATH" python -m grpc_tools.protoc -Iproto 		--go_out=. --go_opt=module=github.com/SanthoshRaaj-KR/Veya 		--go-grpc_out=. --go-grpc_opt=module=github.com/SanthoshRaaj-KR/Veya 		--python_out=$(PROTO_PY) --pyi_out=$(PROTO_PY) --grpc_python_out=$(PROTO_PY) 		$(PROTO_SRC)
	@echo "generated: internal/sdk/workerpb, $(PROTO_PY)/veya/worker/v1"

.PHONY: proto-check
proto-check: ## Fail if the committed stubs are stale
	@$(MAKE) --no-print-directory proto
	@if ! git diff --quiet -- internal/sdk/workerpb $(PROTO_PY)/veya/worker; then 		echo "generated protocol code is out of date; run 'make proto' and commit"; 		git --no-pager diff --stat -- internal/sdk/workerpb $(PROTO_PY)/veya/worker; 		exit 1; 	fi

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

.PHONY: demo-python
demo-python: ## Run the Python refund agent end to end, no Docker
	@bash scripts/demo-python.sh

.PHONY: demo-onboarding
demo-onboarding: ## The Layer 5 example: fan-out, a timer and a human approval
	@bash scripts/demo-onboarding.sh

.PHONY: sdk-install
sdk-install: ## Install the Python SDK and its dev dependencies, editable
	python -m pip install -e "sdk/python[dev]"

.PHONY: sdk-test
sdk-test: ## Lint, type-check and test the Python SDK
	cd sdk/python && python -m ruff check . && python -m ruff format --check .
	cd sdk/python && python -m mypy
	cd sdk/python && python -m pytest -q

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
