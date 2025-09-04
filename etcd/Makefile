# Makefile for etcd registrar tests

.PHONY: help test test-unit test-integration test-coverage test-verbose clean start-etcd stop-etcd check-etcd

# Default target
help:
	@echo "Available targets:"
	@echo "  test              - Run all tests (starts etcd if needed)"
	@echo "  test-unit         - Run unit tests only (no etcd required)"
	@echo "  test-integration  - Run integration tests only (requires etcd)"
	@echo "  test-coverage     - Run tests with coverage report"
	@echo "  test-verbose      - Run tests with verbose output"
	@echo "  start-etcd        - Start etcd via Docker Compose"
	@echo "  stop-etcd         - Stop etcd and cleanup"
	@echo "  check-etcd        - Check if etcd is running"
	@echo "  clean             - Clean up test artifacts and stop etcd"

# Test configuration
ETCD_ENDPOINTS := localhost:12379
GO_TEST_TIMEOUT := 60s
TEST_FLAGS := -timeout $(GO_TEST_TIMEOUT)

# Start etcd container
start-etcd:
	@echo "Starting etcd..."
	@docker compose up -d
	@echo "Waiting for etcd to be ready..."
	@for i in {1..30}; do \
		if docker compose exec -T etcd etcdctl --endpoints=http://localhost:2379 endpoint health > /dev/null 2>&1; then \
			echo "etcd is ready!"; \
			exit 0; \
		fi; \
		sleep 1; \
	done; \
	echo "Timeout waiting for etcd to be ready"; exit 1

# Stop etcd container
stop-etcd:
	@echo "Stopping etcd..."
	@docker compose down

# Check if etcd is running and accessible
check-etcd:
	@echo "Checking etcd connection..."
	@ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) etcdctl --endpoints=$(ETCD_ENDPOINTS) endpoint health || \
		(echo "etcd is not accessible at $(ETCD_ENDPOINTS)"; exit 1)
	@echo "etcd is running and accessible"

# Run unit tests (no etcd required)
test-unit:
	@echo "Running unit tests..."
	@go test $(TEST_FLAGS) -run "^Test[^I]" .

# Run integration tests (requires etcd)
test-integration: start-etcd check-etcd
	@echo "Running integration tests..."
	@ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) go test $(TEST_FLAGS) -run "TestIntegration" .

# Run all tests
test: start-etcd check-etcd
	@echo "Running all tests..."
	@ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) go test $(TEST_FLAGS) . || ($(MAKE) stop-etcd; exit 1)

# Run tests with coverage
test-coverage: start-etcd check-etcd
	@echo "Running tests with coverage..."
	@ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) go test $(TEST_FLAGS) -cover . || ($(MAKE) stop-etcd; exit 1)

# Run tests with verbose output
test-verbose: start-etcd
	@echo "Running tests with verbose output..."
	@ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) go test $(TEST_FLAGS) -v . || ($(MAKE) stop-etcd; exit 1)

# Run specific test
test-run: start-etcd
	@if [ -z "$(RUN)" ]; then \
		echo "Usage: make test-run RUN=TestName"; \
		exit 1; \
	fi
	@echo "Running test: $(RUN)"
	@ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) go test $(TEST_FLAGS) -v -run "$(RUN)" . || ($(MAKE) stop-etcd; exit 1)

# Clean up everything
clean: stop-etcd
	@echo "Cleaning up..."
	@go clean -testcache
	@docker system prune -f --filter "label=com.docker.compose.project=etcd" > /dev/null 2>&1 || true
	@echo "Cleanup complete"

# Development helpers
dev-start: start-etcd check-etcd
	@echo "Development environment ready!"
	@echo "etcd is available at: $(ETCD_ENDPOINTS)"
	@echo "Run 'make test-integration' to run integration tests"

dev-stop: stop-etcd

# Run tests in watch mode (requires entr: brew install entr)
test-watch:
	@echo "Watching for changes (requires 'entr' to be installed)..."
	@find . -name "*.go" | entr -c make test-unit

# Benchmark tests
bench: start-etcd
	@echo "Running benchmarks..."
	@ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) go test -bench=. -benchmem . || ($(MAKE) stop-etcd; exit 1)

# Show test results in different formats
test-json: start-etcd
	@echo "Running tests with JSON output..."
	@ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) go test $(TEST_FLAGS) -json . || ($(MAKE) stop-etcd; exit 1)

# Run tests for CI (non-interactive)
test-ci: 
	@echo "Running CI tests..."
	@$(MAKE) start-etcd
	@ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) go test $(TEST_FLAGS) -cover . ; \
	EXIT_CODE=$$? ; \
	$(MAKE) stop-etcd ; \
	exit $$EXIT_CODE

# Install required tools
install-tools:
	@echo "Installing required tools..."
	@command -v etcdctl >/dev/null 2>&1 || (echo "Installing etcdctl..." && \
		ETCD_VER=v3.5.13 && \
		curl -L https://github.com/etcd-io/etcd/releases/download/$${ETCD_VER}/etcd-$${ETCD_VER}-darwin-amd64.tar.gz -o /tmp/etcd.tar.gz && \
		tar xzf /tmp/etcd.tar.gz -C /tmp && \
		sudo mv /tmp/etcd-$${ETCD_VER}-darwin-amd64/etcdctl /usr/local/bin/ && \
		rm -rf /tmp/etcd*)
	@docker compose version >/dev/null 2>&1 || (echo "Docker Compose not found. Please install Docker Desktop"; exit 1)
	@echo "All tools installed!"

# Show logs
logs:
	@docker compose logs -f etcd

# Show etcd status
status:
	@echo "=== Docker Status ==="
	@docker compose ps
	@echo ""
	@echo "=== etcd Health ==="
	@ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) etcdctl --endpoints=$(ETCD_ENDPOINTS) endpoint health || echo "etcd not accessible"
	@echo ""
	@echo "=== etcd Members ==="
	@ETCD_ENDPOINTS=$(ETCD_ENDPOINTS) etcdctl --endpoints=$(ETCD_ENDPOINTS) member list || echo "etcd not accessible" 
