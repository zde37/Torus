.PHONY: all proto test test-verbose clean help

# Default target
all: proto test

# Generate protobuf code
proto:
	@echo "Generating protobuf code..."
	protoc -I protobuf/proto \
		--go_out=. --go_opt=module=github.com/zde37/torus \
		--go-grpc_out=. --go-grpc_opt=module=github.com/zde37/torus \
		--grpc-gateway_out=. --grpc-gateway_opt=module=github.com/zde37/torus \
		--grpc-gateway_opt=logtostderr=true \
		protobuf/proto/chord.proto
	@echo "Protobuf code generated successfully!"

# Run all tests
test:
	@echo "Running tests..."
	go test ./... -count=1

# Run tests with race detection
test-race:
	@go test -count=1 -v -race ./...

# Run tests with verbose output
test-verbose:
	@echo "Running tests (verbose)..."
	go test ./... -v -count=1

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test ./... -cover -count=1

# Run tests and generate coverage report
test-coverage-report:
	@echo "Generating coverage report..."
	go test ./... -coverprofile=coverage.out -count=1
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Clean generated protobuf files
clean-proto:
	@echo "Cleaning generated protobuf files..."
	rm -f protobuf/protogen/chord.pb.go
	rm -f protobuf/protogen/chord_grpc.pb.go
	rm -f protobuf/protogen/chord.pb.gw.go
	@echo "Protobuf files cleaned!"

# Clean test artifacts
clean-test:
	@echo "Cleaning test artifacts..."
	rm -f coverage.out coverage.html
	@echo "Test artifacts cleaned!"

# Clean all generated files
clean: clean-proto clean-test
	@echo "All generated files cleaned!"

# Build the application
build:
	@echo "Building application..."
	go build -o bin/torus ./cmd/torus
	@echo "Build complete: bin/torus"

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...
	@echo "Code formatted!"

# Run linter
lint:
	@echo "Running linter..."
	golangci-lint run ./...

# Tidy dependencies
tidy:
	@echo "Tidying dependencies..."
	go mod tidy
	@echo "Dependencies tidied!"

# Install development dependencies
install-tools:
	@echo "Installing development tools..."
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
	go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest
	@echo "Tools installed!"

# Run all checks (format, lint, test)
check: fmt test
	@echo "All checks passed!"

# Show help
help:
	@echo "Available targets:"
	@echo "  make proto                - Generate protobuf code"
	@echo "  make test                 - Run all tests"
	@echo "  test-race     			   - Run tests with race detection"
	@echo "  make test-verbose         - Run tests with verbose output"
	@echo "  make test-coverage        - Run tests with coverage"
	@echo "  make test-coverage-report - Generate HTML coverage report"
	@echo "  make build                - Build the application"
	@echo "  make clean                - Clean all generated files"
	@echo "  make clean-proto          - Clean generated protobuf files"
	@echo "  make clean-test           - Clean test artifacts"
	@echo "  make fmt                  - Format code"
	@echo "  make lint                 - Run linter"
	@echo "  make tidy                 - Tidy dependencies"
	@echo "  make install-tools        - Install development tools"
	@echo "  make check                - Run format and tests"
	@echo "  make all                  - Generate proto and run tests"
	@echo "  make help                 - Show this help message"
