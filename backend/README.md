# Torus Backend

Torus is a Chord DHT (Distributed Hash Table) implementation in Go with a focus on simplicity, reliability, and observability.

## Prerequisites

- Go 1.21 or higher
- Protocol Buffers compiler (protoc)
- protoc-gen-go and protoc-gen-go-grpc plugins

## Quick Start

### Install Development Tools

```bash
make install-tools
```

This will install the required protoc plugins.

### Generate Protobuf Code

```bash
make proto
```

Generates Go code from Protocol Buffer definitions in `protobuf/proto/chord.proto`.
Generated files are placed in `protobuf/protogen/`.

### Run Tests

```bash
make test              # Run all tests
make test-verbose      # Run tests with verbose output
make test-coverage     # Run tests with coverage statistics
```

### Build the Application

```bash
make build
```

Builds the application binary to `bin/torus`.

## Available Make Targets

| Command | Description |
|---------|-------------|
| `make proto` | Generate protobuf code |
| `make test` | Run all tests |
| `make test-verbose` | Run tests with verbose output |
| `make test-coverage` | Run tests with coverage |
| `make test-coverage-report` | Generate HTML coverage report |
| `make build` | Build the application |
| `make clean` | Clean all generated files |
| `make clean-proto` | Clean generated protobuf files |
| `make clean-test` | Clean test artifacts |
| `make fmt` | Format code |
| `make lint` | Run linter (requires golangci-lint) |
| `make tidy` | Tidy dependencies |
| `make install-tools` | Install development tools |
| `make check` | Run format and tests |
| `make all` | Generate proto and run tests (default) |
| `make help` | Show available targets |

## Project Structure

```
backend/
├── cmd/                    # Application entry points
├── internal/              # Private application code
│   ├── chord/            # Chord node types (NodeAddress, FingerEntry)
│   ├── config/           # Configuration management
│   ├── hash/             # Consistent hashing and ring arithmetic
│   └── storage/          # Chord storage wrapper
├── pkg/                   # Public packages (reusable)
│   ├── logger.go         # Structured logging (zerolog)
│   ├── memory.go         # In-memory storage with TTL
│   └── errors.go         # Common error definitions
├── protobuf/             # Protocol Buffers
│   ├── proto/           # .proto definitions
│   └── protogen/        # Generated Go code
├── Makefile              # Build automation
└── go.mod               # Go module definition
```

## Development Workflow

1. **Make changes to proto files**: Edit `protobuf/proto/chord.proto`
2. **Regenerate code**: `make proto`
3. **Run tests**: `make test`
4. **Format code**: `make fmt`
5. **Build**: `make build`

## Test Coverage

Current test coverage (Step 4):

- `internal/chord`: 94.9%
- `internal/config`: 100.0%
- `internal/hash`: 93.4%
- `internal/storage`: 87.9%
- `pkg`: 93.3%
- Overall: >90% for all implementation code

## Contributing

When adding new features:

1. Write tests first (TDD)
2. Ensure `make check` passes
3. Maintain >90% test coverage
4. Update proto definitions if needed
5. Regenerate with `make proto`

## License

[Add your license here]
