.PHONY: help build build-debug test test-unit test-integration _test-integration test-integration-pathstyle _test-integration-pathstyle test-integration-awsstyle _test-integration-awsstyle test-integration-eventsource _test-integration-eventsource test-all fmt clippy lint clean run run-release audit all docker-build-debian docker-push-debian docker-build-alpine docker-push-alpine docker-build docker-build-multi docker-push docker-clean docker-buildx-setup docker-up docker-down docker-restart docker-logs docker-test wait-ready

.DEFAULT_GOAL := help

# Compose file variables
COMPOSE_TEST := docker compose -f docker-compose.test.yml
COMPOSE_AWSSTYLE := $(COMPOSE_TEST) -f docker-compose.test.awsstyle.yml

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "  Testing:"
	@echo "    make test                          - Run unit tests"
	@echo "    make test-integration              - Run simulated integration tests"
	@echo "    make test-integration-pathstyle    - Run path-style tests (real Lambda containers)"
	@echo "    make test-integration-awsstyle     - Run AWS-style vhost tests (in Docker with dnsmasq)"
	@echo "    make test-integration-eventsource  - Run SQS/SNS event source tests (local-sns + ElasticMQ)"
	@echo "    make test-all                      - Run all tests"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

all: fmt clippy build test ## Run fmt, clippy, build, and test

build: ## Build release binary
	cargo build --release

build-debug: ## Build debug binary
	cargo build

test: test-unit ## Run unit tests

test-unit: ## Run unit tests (alias for cargo test)
	cargo test

# All tests (unit + simulated + path-style + AWS-style)
# Docker image is built once; internal _targets skip the rebuild.
test-all: docker-build-debian ## Run all tests
	@exit_code=0; \
	$(MAKE) test-unit || exit_code=$$?; \
	$(MAKE) _test-integration || exit_code=$$?; \
	$(MAKE) _test-integration-eventsource || exit_code=$$?; \
	$(MAKE) _test-integration-pathstyle || exit_code=$$?; \
	$(MAKE) _test-integration-awsstyle || exit_code=$$?; \
	exit $$exit_code

fmt: ## Check code formatting
	cargo fmt --check

clippy: ## Run clippy lints
	cargo clippy -- -D warnings

lint: fmt clippy ## Run fmt and clippy

clean: ## Remove build artifacts
	cargo clean

audit: ## Run cargo audit
	cargo audit

run: ## Run server locally (debug)
	cargo run

run-release: ## Run server locally (release)
	cargo run --release

INVOKE_PORT := 9600
RUNTIME_PORT := 9601

wait-ready: ## Wait for server to be ready on ports 9600 and 9601
	@echo "Waiting for server to be ready on ports $(INVOKE_PORT) and $(RUNTIME_PORT)..."
	@for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30; do \
		if nc -z localhost $(INVOKE_PORT) 2>/dev/null && nc -z localhost $(RUNTIME_PORT) 2>/dev/null; then \
			echo "Server is ready!"; \
			break; \
		fi; \
		echo "Waiting... ($$i/30)"; \
		sleep 1; \
	done
	@if ! nc -z localhost $(INVOKE_PORT) 2>/dev/null || ! nc -z localhost $(RUNTIME_PORT) 2>/dev/null; then \
		echo "Error: Server did not become ready within 30 seconds"; \
		docker compose logs || true; \
		docker compose down; \
		exit 1; \
	fi

# Simulated integration tests (in-process servers with mock container runtimes)
test-integration: docker-build-debian _test-integration ## Run simulated integration tests

_test-integration: docker-up wait-ready
	@echo "=========================================="
	@echo "  SIMULATED integration tests"
	@echo "=========================================="
	@exit_code=0; \
	cargo test --test e2e_integration --test integration_awsstyle \
		--test bootstrap_failure_integration --test concurrent_invocation_integration \
		--test timeout_integration -- --ignored --nocapture || exit_code=$$?; \
	$(MAKE) docker-down; \
	exit $$exit_code

# Path-style integration tests (localfunctions in Docker, real Lambda containers)
test-integration-pathstyle: docker-build-debian _test-integration-pathstyle ## Run path-style integration tests with real Lambda containers

_test-integration-pathstyle:
	@echo "=========================================="
	@echo "  PATH-STYLE integration tests"
	@echo "=========================================="
	$(COMPOSE_TEST) up -d localfunctions
	@$(MAKE) wait-ready
	@exit_code=0; \
	cargo test --test docker_pathstyle_integration -- --ignored --nocapture --test-threads=1 || exit_code=$$?; \
	$(MAKE) docker-down; \
	exit $$exit_code

# AWS-style virtual host tests (localfunctions + dnsmasq in Docker, test-runner in Docker)
test-integration-awsstyle: docker-build-debian _test-integration-awsstyle ## Run AWS-style vhost integration tests (in Docker with dnsmasq)

_test-integration-awsstyle:
	@echo "=========================================="
	@echo "  AWS-STYLE VIRTUAL HOST integration tests"
	@echo "=========================================="
	@exit_code=0; \
	$(COMPOSE_AWSSTYLE) up -d localfunctions dns-awsstyle; \
	$(COMPOSE_AWSSTYLE) run --rm test-runner || exit_code=$$?; \
	$(MAKE) docker-down; \
	exit $$exit_code

# Event source integration tests (SQS + SNS with local-sns and ElasticMQ)
test-integration-eventsource: _test-integration-eventsource ## Run SQS/SNS event source integration tests

_test-integration-eventsource:
	@echo "=========================================="
	@echo "  EVENT SOURCE integration tests (SQS+SNS)"
	@echo "=========================================="
	$(COMPOSE_TEST) up -d sns sqs
	@echo "Waiting for SNS (local-sns) and SQS (ElasticMQ) to be ready..."
	@for i in 1 2 3 4 5 6 7 8 9 10; do \
		if nc -z localhost 9911 2>/dev/null && nc -z localhost 9324 2>/dev/null; then \
			echo "SNS and SQS are ready!"; \
			break; \
		fi; \
		echo "Waiting... ($$i/10)"; \
		sleep 1; \
	done
	@exit_code=0; \
	SQS_ENDPOINT=http://localhost:9324 SNS_ENDPOINT=http://localhost:9911 \
		cargo test --test sqs_integration --test sns_integration -- --ignored --nocapture --test-threads=1 || exit_code=$$?; \
	$(COMPOSE_TEST) stop sns sqs; \
	$(COMPOSE_TEST) rm -f sns sqs; \
	exit $$exit_code

docker-test: test-integration ## Run integration tests (alias)

VERSION := $(shell cat VERSION 2>/dev/null || echo "0.1.0")
IMAGE_NAME := localfunctions
REGISTRY_IMAGE := gandalfsievers/localfunctions
BUILDER_NAME := localfunctions-builder

# Auto-detect host architecture and map to Docker architecture names
UNAME_ARCH := $(shell uname -m)
ifeq ($(UNAME_ARCH),x86_64)
  HOST_ARCH := amd64
else ifeq ($(UNAME_ARCH),aarch64)
  HOST_ARCH := arm64
else ifeq ($(UNAME_ARCH),arm64)
  HOST_ARCH := arm64
else
  HOST_ARCH := $(UNAME_ARCH)
endif

docker-buildx-setup: ## Create/reuse named buildx builder
	@docker buildx inspect $(BUILDER_NAME) >/dev/null 2>&1 || \
		docker buildx create --name $(BUILDER_NAME) --use --platform linux/amd64,linux/arm64
	@docker buildx use $(BUILDER_NAME)

docker-build-debian: ## Build Debian image (native arch)
	@echo "Building Debian image (version: $(VERSION))..."
	docker build \
		-f docker/Dockerfile.debian \
		-t $(IMAGE_NAME):latest \
		-t $(IMAGE_NAME):debian \
		-t $(IMAGE_NAME):$(VERSION) \
		-t $(IMAGE_NAME):$(VERSION)-debian \
		-t $(REGISTRY_IMAGE):latest \
		-t $(REGISTRY_IMAGE):debian \
		-t $(REGISTRY_IMAGE):$(VERSION) \
		-t $(REGISTRY_IMAGE):$(VERSION)-debian \
		.

docker-build-alpine: ## Build Alpine image (native arch)
	@echo "Building Alpine image (version: $(VERSION))..."
	docker build \
		-f docker/Dockerfile.alpine \
		-t $(IMAGE_NAME):alpine \
		-t $(IMAGE_NAME):$(VERSION)-alpine \
		-t $(REGISTRY_IMAGE):alpine \
		-t $(REGISTRY_IMAGE):$(VERSION)-alpine \
		.

docker-build: docker-build-debian docker-build-alpine ## Build both images (native arch)
	@echo "Build complete! Images:"
	@docker images $(IMAGE_NAME) --format "table {{.Repository}}:{{.Tag}}\t{{.Size}}"

docker-build-multi: docker-buildx-setup ## Build multi-arch images (amd64 + arm64, cache only)
	@echo "Building multi-arch Debian image..."
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-f docker/Dockerfile.debian \
		-t $(IMAGE_NAME):latest \
		-t $(IMAGE_NAME):debian \
		-t $(IMAGE_NAME):$(VERSION) \
		-t $(IMAGE_NAME):$(VERSION)-debian \
		-t $(REGISTRY_IMAGE):latest \
		-t $(REGISTRY_IMAGE):debian \
		-t $(REGISTRY_IMAGE):$(VERSION) \
		-t $(REGISTRY_IMAGE):$(VERSION)-debian \
		.
	@echo "Building multi-arch Alpine image..."
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-f docker/Dockerfile.alpine \
		-t $(IMAGE_NAME):alpine \
		-t $(IMAGE_NAME):$(VERSION)-alpine \
		-t $(REGISTRY_IMAGE):alpine \
		-t $(REGISTRY_IMAGE):$(VERSION)-alpine \
		.

docker-push-debian: docker-buildx-setup ## Push multi-arch Debian image to registry
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-f docker/Dockerfile.debian \
		-t $(REGISTRY_IMAGE):latest \
		-t $(REGISTRY_IMAGE):debian \
		-t $(REGISTRY_IMAGE):$(VERSION) \
		-t $(REGISTRY_IMAGE):$(VERSION)-debian \
		--push \
		.

docker-push-alpine: docker-buildx-setup ## Push multi-arch Alpine image to registry
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		-f docker/Dockerfile.alpine \
		-t $(REGISTRY_IMAGE):alpine \
		-t $(REGISTRY_IMAGE):$(VERSION)-alpine \
		--push \
		.

docker-push: docker-push-debian docker-push-alpine ## Push both images to registry

docker-up: ## Start container via docker compose (detached, with build)
	docker compose up -d --build

docker-down: ## Stop and remove all test containers
	-$(COMPOSE_AWSSTYLE) down --remove-orphans 2>/dev/null
	-$(COMPOSE_TEST) down --remove-orphans 2>/dev/null
	-docker compose down --remove-orphans 2>/dev/null

docker-restart: docker-down docker-up ## Restart container (stop then start)

docker-logs: ## Tail container logs via docker compose
	docker compose logs -f

docker-clean: ## Remove Docker images and build artifacts
	@echo "Cleaning Docker artifacts..."
	-docker rmi $(IMAGE_NAME):latest $(IMAGE_NAME):debian $(IMAGE_NAME):alpine 2>/dev/null
	-docker rmi $(IMAGE_NAME):$(VERSION) $(IMAGE_NAME):$(VERSION)-debian $(IMAGE_NAME):$(VERSION)-alpine 2>/dev/null
	-docker rmi $(REGISTRY_IMAGE):latest $(REGISTRY_IMAGE):debian $(REGISTRY_IMAGE):alpine 2>/dev/null
	-docker rmi $(REGISTRY_IMAGE):$(VERSION) $(REGISTRY_IMAGE):$(VERSION)-debian $(REGISTRY_IMAGE):$(VERSION)-alpine 2>/dev/null
	-docker buildx rm $(BUILDER_NAME) 2>/dev/null
