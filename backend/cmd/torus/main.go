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
	bootstrap := flag.String("bootstrap", "", "Bootstrap node address (host:port) to join existing ring")
	logLevel := flag.String("log-level", "info", "Log level (trace, debug, info, warn, error)")
	logFormat := flag.String("log-format", "console", "Log format (json, console)")
	flag.Parse()

	// Create configuration
	cfg := &config.Config{
		Host:               *host,
		Port:               *port,
		HTTPPort:           *httpPort,
		M:                  160, // 2^160 address space
		StabilizeInterval:  3 * time.Second,
		FixFingersInterval: 3 * time.Second,
		SuccessorListSize:  3,
		RPCTimeout:         5 * time.Second,
		LogLevel:           *logLevel,
		LogFormat:          *logFormat,
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
	grpcServer, err := transport.NewGRPCServer(node, serverAddr, logger)
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
	grpcClient := transport.NewGRPCClient(logger, cfg.RPCTimeout)
	node.SetRemote(grpcClient)

	// Create HTTP API server
	httpServer, err := api.NewServer(&api.Config{
		HTTPPort: cfg.HTTPPort,
		GRPCAddr: serverAddr,
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

	// Create or join Chord ring
	if *bootstrap == "" {
		// No bootstrap node specified, create new ring
		logger.Info().Msg("Creating new Chord ring")
		if err := node.Create(); err != nil {
			logger.Error().Err(err).Msg("Failed to create Chord ring")
			cleanup(node, grpcServer, grpcClient, httpServer, logger)
			os.Exit(1)
		}
		logger.Info().
			Str("node_id", node.ID().Text(16)[:16]).
			Msg("Chord ring created successfully")
	} else {
		// Bootstrap node specified, join existing ring
		logger.Info().
			Str("bootstrap", *bootstrap).
			Msg("Joining existing Chord ring")

		// Get bootstrap node information via RPC
		logger.Debug().
			Str("bootstrap", *bootstrap).
			Msg("Fetching bootstrap node information")

		bootstrapNode, err := grpcClient.GetNodeInfo(*bootstrap)
		if err != nil {
			logger.Error().
				Err(err).
				Str("bootstrap", *bootstrap).
				Msg("Failed to get bootstrap node information")
			cleanup(node, grpcServer, grpcClient, httpServer, logger)
			os.Exit(1)
		}

		logger.Info().
			Str("bootstrap_id", bootstrapNode.ID.Text(16)[:16]).
			Str("bootstrap_addr", bootstrapNode.Address()).
			Msg("Retrieved bootstrap node information")

		// Join the ring
		if err := node.Join(bootstrapNode); err != nil {
			logger.Error().Err(err).Msg("Failed to join Chord ring")
			cleanup(node, grpcServer, grpcClient, httpServer, logger)
			os.Exit(1)
		}

		logger.Info().
			Str("node_id", node.ID().Text(16)[:16]).
			Msg("Joined Chord ring successfully")
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
