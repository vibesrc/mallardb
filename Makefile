# MallardDB Makefile
# Build, test, and run integration tests

.PHONY: all build build-release test test-unit test-integration test-ci \
        server-start server-stop run clean help

# Configuration
HOST ?= 127.0.0.1
PORT ?= 5432
PID_FILE := /tmp/mallardb.pid

# Default target
all: build test

# Build targets
build:
	cargo build

build-release:
	cargo build --release

# Test targets
test: test-unit

test-unit:
	cargo test --lib

test-integration:
	cargo test --test integration_test -- --ignored

# CI target: build, start server, run integration tests, stop server
test-ci: build server-start
	@sleep 2
	@$(MAKE) test-integration || ($(MAKE) server-stop && exit 1)
	@$(MAKE) server-stop

# Server management
server-start:
	@echo "Starting mallardb server..."
	@./target/debug/mallardb & echo $$! > $(PID_FILE)
	@sleep 2
	@if [ -f $(PID_FILE) ] && kill -0 $$(cat $(PID_FILE)) 2>/dev/null; then \
		echo "Server started (PID: $$(cat $(PID_FILE)))"; \
	else \
		echo "Failed to start server"; \
		exit 1; \
	fi

server-stop:
	@echo "Stopping mallardb server..."
	@if [ -f $(PID_FILE) ]; then \
		kill $$(cat $(PID_FILE)) 2>/dev/null || true; \
		rm -f $(PID_FILE); \
		echo "Server stopped"; \
	else \
		echo "No PID file found"; \
	fi

# Run the server interactively
run:
	cargo run

run-release:
	cargo run --release

# Clean
clean:
	cargo clean
	rm -f $(PID_FILE)

# Help
help:
	@echo "MallardDB Makefile"
	@echo ""
	@echo "Build targets:"
	@echo "  build          - Debug build"
	@echo "  build-release  - Release build"
	@echo ""
	@echo "Test targets:"
	@echo "  test           - Run unit tests"
	@echo "  test-unit      - Run unit tests only"
	@echo "  test-integration - Run integration tests (requires running server)"
	@echo "  test-ci        - Build, start server, run integration tests, stop server"
	@echo ""
	@echo "Server management:"
	@echo "  server-start   - Start server in background"
	@echo "  server-stop    - Stop background server"
	@echo "  run            - Run server interactively (debug)"
	@echo "  run-release    - Run server interactively (release)"
	@echo ""
	@echo "Other:"
	@echo "  clean          - Clean build artifacts"
	@echo "  help           - Show this help"
