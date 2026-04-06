.PHONY: help build test fmt clippy lint clean run run-release audit all docker-build-debian docker-push-debian docker-build-alpine docker-push-alpine docker-build docker-build-multi docker-push docker-clean docker-buildx-setup docker-up docker-down docker-restart docker-logs

.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

all: fmt clippy build test ## Run fmt, clippy, build, and test

build: ## Build the project (debug)
	cargo build

test: ## Run unit tests
	cargo test

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

docker-down: ## Stop and remove container via docker compose
	docker compose down

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
