package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zde37/torus/internal/api"
	"github.com/zde37/torus/internal/chord"
	"github.com/zde37/torus/internal/config"
	"github.com/zde37/torus/internal/transport"
	"github.com/zde37/torus/pkg"
)

func main() {
	// Parse command-line flags
	host := flag.String("host", "127.0.0.1", "Host address to bind to")
	port := flag.Int("port", 8440, "Port for Chord gRPC server")
	httpPort := flag.Int("http-port", 8080, "Port for HTTP API server")
	bootstrap := flag.String("bootstrap", "", "Bootstrap node address (host:port) to join existing ring (optional, uses default bootstrap nodes if not specified)")
	create := flag.Bool("create", false, "Create a new Chord ring instead of joining (useful for local testing)")
	authToken := flag.String("auth-token", "", "Authentication token for joining the ring (required for secure networks)")
	logLevel := flag.String("log-level", "info", "Log level (trace, debug, info, warn, error)")
	logFormat := flag.String("log-format", "console", "Log format (json, console)")
	flag.Parse()

	// Create configuration
	cfg := &config.Config{
		Host:               *host,
		Port:               *port,
		HTTPPort:           *httpPort,
		AuthToken:          *authToken,
		M:                  160, // 2^160 address space
		StabilizeInterval:  3 * time.Second,
		FixFingersInterval: 3 * time.Second,
		SuccessorListSize:  8, // Advanced: maintain 8 successors for fault tolerance
		RPCTimeout:         5 * time.Second,
		LogLevel:           *logLevel,
		LogFormat:          *logFormat,
	}

	// Set bootstrap nodes
	if *bootstrap != "" {
		// Use explicitly provided bootstrap node
		cfg.BootstrapNodes = []string{*bootstrap}
	} else {
		// Use default bootstrap nodes from config
		cfg.BootstrapNodes = config.DefaultBootstrapNodes
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "Invalid configuration: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger
	loggerConfig := pkg.DefaultConfig()
	loggerConfig.Level = *logLevel
	loggerConfig.Format = *logFormat

	logger, err := pkg.New(loggerConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}

	logger.Info().
		Str("host", cfg.Host).
		Int("port", cfg.Port).
		Int("http_port", cfg.HTTPPort).
		Msg("Starting Torus node")

	// Create ChordNode
	node, err := chord.NewChordNode(cfg, logger)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create Chord node")
		os.Exit(1)
	}

	// Create gRPC server
	serverAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	grpcServer, err := transport.NewGRPCServer(node, serverAddr, cfg.AuthToken, logger)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create gRPC server")
		os.Exit(1)
	}

	// Start gRPC server
	if err := grpcServer.Start(); err != nil {
		logger.Error().Err(err).Msg("Failed to start gRPC server")
		os.Exit(1)
	}

	logger.Info().
		Str("address", serverAddr).
		Msg("gRPC server started")

	// Create and set gRPC client for inter-node communication
	grpcClient := transport.NewGRPCClient(logger, cfg.AuthToken, cfg.RPCTimeout)
	node.SetRemote(grpcClient)

	// Create HTTP API server
	httpServer, err := api.NewServer(&api.Config{
		HTTPPort:  cfg.HTTPPort,
		GRPCAddr:  serverAddr,
		AuthToken: cfg.AuthToken,
	}, logger)
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create HTTP API server")
		cleanup(node, grpcServer, grpcClient, nil, logger)
		os.Exit(1)
	}

	// Start HTTP API server
	if err := httpServer.Start(cfg.HTTPPort); err != nil {
		logger.Error().Err(err).Msg("Failed to start HTTP API server")
		cleanup(node, grpcServer, grpcClient, nil, logger)
		os.Exit(1)
	}

	logger.Info().
		Int("port", cfg.HTTPPort).
		Msg("HTTP API server started")

	// Set the WebSocket broadcaster for ring updates
	node.SetBroadcaster(httpServer.GetWebSocketHub())

	// Create or join Chord ring
	if *create {
		// Explicitly creating new ring
		logger.Info().Msg("Creating new Chord ring (--create flag set)")
		if err := node.Create(); err != nil {
			logger.Error().Err(err).Msg("Failed to create Chord ring")
			cleanup(node, grpcServer, grpcClient, httpServer, logger)
			os.Exit(1)
		}
		logger.Info().
			Str("node_id", node.ID().Text(16)[:16]).
			Msg("Chord ring created successfully")
	} else {
		// Try each bootstrap node until one succeeds
		logger.Info().
			Int("bootstrap_count", len(cfg.BootstrapNodes)).
			Msg("Attempting to join existing Chord ring")

		var joinErr error
		joined := false

		for _, bootstrapAddr := range cfg.BootstrapNodes {
			logger.Debug().
				Str("bootstrap", bootstrapAddr).
				Msg("Trying bootstrap node")

			// Get bootstrap node information via RPC
			bootstrapNode, err := grpcClient.GetNodeInfo(bootstrapAddr)
			if err != nil {
				logger.Warn().
					Err(err).
					Str("bootstrap", bootstrapAddr).
					Msg("Failed to contact bootstrap node, trying next")
				joinErr = err
				continue
			}

			logger.Info().
				Str("bootstrap_id", bootstrapNode.ID.Text(16)[:16]).
				Str("bootstrap_addr", bootstrapNode.Address()).
				Msg("Retrieved bootstrap node information")

			// Join the ring
			if err := node.Join(bootstrapNode); err != nil {
				logger.Warn().
					Err(err).
					Str("bootstrap", bootstrapAddr).
					Msg("Failed to join via this bootstrap node, trying next")
				joinErr = err
				continue
			}

			logger.Info().
				Str("node_id", node.ID().Text(16)[:16]).
				Str("bootstrap", bootstrapAddr).
				Msg("Successfully joined Chord ring")
			joined = true
			break
		}

		if !joined {
			logger.Error().
				Err(joinErr).
				Msg("Failed to join ring via any bootstrap node")
			cleanup(node, grpcServer, grpcClient, httpServer, logger)
			os.Exit(1)
		}
	}

	logger.Info().Msg("Torus node is ready")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	logger.Info().
		Str("signal", sig.String()).
		Msg("Received shutdown signal")

	// Cleanup
	cleanup(node, grpcServer, grpcClient, httpServer, logger)

	logger.Info().Msg("Torus node shutdown complete")
}

// cleanup performs graceful shutdown of all components
func cleanup(node *chord.ChordNode, grpcServer *transport.GRPCServer, grpcClient *transport.GRPCClient, httpServer *api.Server, logger *pkg.Logger) {
	logger.Info().Msg("Starting graceful shutdown")

	// Stop HTTP server
	if httpServer != nil {
		if err := httpServer.Stop(); err != nil {
			logger.Error().Err(err).Msg("Error stopping HTTP server")
		}
	}

	// Stop gRPC server
	if err := grpcServer.Stop(); err != nil {
		logger.Error().Err(err).Msg("Error stopping gRPC server")
	}

	// Shutdown ChordNode
	if err := node.Shutdown(); err != nil {
		logger.Error().Err(err).Msg("Error shutting down Chord node")
	}

	// Close gRPC client connections
	if err := grpcClient.Close(); err != nil {
		logger.Error().Err(err).Msg("Error closing gRPC client")
	}
}
