.PHONY: help build test fmt clippy lint clean run run-release

.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-25s\033[0m %s\n", $$1, $$2}'

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

run: ## Run server locally (debug)
	cargo run

run-release: ## Run server locally (release)
	cargo run --release
