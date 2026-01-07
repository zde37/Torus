package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zde37/torus/backend/pkg"
	"github.com/zde37/torus/backend/pkg/api"
	"github.com/zde37/torus/backend/pkg/chord"
	"github.com/zde37/torus/backend/pkg/config"
	"github.com/zde37/torus/backend/pkg/transport"
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
	loggerConfig.Outputs = []string{pkg.LogOutputStdout, pkg.LogOutputFile}

	logger, err := pkg.NewLogger(loggerConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create logger: %v\n", err)
		os.Exit(1)
	}

	logger.Info("Starting Torus node", pkg.Fields{
		"host":      cfg.Host,
		"port":      cfg.Port,
		"http_port": cfg.HTTPPort,
	})

	// Create ChordNode
	node, err := chord.NewChordNode(cfg, logger)
	if err != nil {
		logger.Error("Failed to create Chord node", pkg.Fields{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Create gRPC server
	serverAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	grpcServer, err := transport.NewGRPCServer(node, serverAddr, cfg.AuthToken, logger)
	if err != nil {
		logger.Error("Failed to create gRPC server", pkg.Fields{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	// Start gRPC server
	if err := grpcServer.Start(); err != nil {
		logger.Error("Failed to start gRPC server", pkg.Fields{
			"error": err.Error(),
		})
		os.Exit(1)
	}

	logger.Info("gRPC server started", pkg.Fields{
		"address": serverAddr,
	})

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
		logger.Error("Failed to create HTTP API server", pkg.Fields{
			"error": err.Error(),
		})
		cleanup(node, grpcServer, grpcClient, nil, logger)
		os.Exit(1)
	}

	// Start HTTP API server
	if err := httpServer.Start(cfg.HTTPPort); err != nil {
		logger.Error("Failed to start HTTP API server", pkg.Fields{
			"error": err.Error(),
		})
		cleanup(node, grpcServer, grpcClient, nil, logger)
		os.Exit(1)
	}

	logger.Info("HTTP API server started", pkg.Fields{
		"port": cfg.HTTPPort,
	})

	// Set the WebSocket broadcaster for ring updates
	node.SetBroadcaster(httpServer.GetWebSocketHub())

	// Create or join Chord ring
	if *create {
		// Explicitly creating new ring
		logger.Info("Creating new Chord ring (--create flag set)", nil)
		if err := node.Create(); err != nil {
			logger.Error("Failed to create Chord ring", pkg.Fields{
				"error": err.Error(),
			})
			cleanup(node, grpcServer, grpcClient, httpServer, logger)
			os.Exit(1)
		}
		logger.Info("Chord ring created successfully", pkg.Fields{
			"node_id": node.ID().Text(16)[:16],
		})
	} else {
		// Try each bootstrap node until one succeeds
		logger.Info("Attempting to join existing Chord ring", pkg.Fields{
			"bootstrap_count": len(cfg.BootstrapNodes),
		})

		var joinErr error
		joined := false

		for _, bootstrapAddr := range cfg.BootstrapNodes {
			logger.Debug("Trying bootstrap node", pkg.Fields{
				"bootstrap": bootstrapAddr,
			})

			// Get bootstrap node information via RPC
			bootstrapNode, err := grpcClient.GetNodeInfo(bootstrapAddr)
			if err != nil {
				logger.Warn("Failed to contact bootstrap node, trying next", pkg.Fields{
					"error":     err.Error(),
					"bootstrap": bootstrapAddr,
				})
				joinErr = err
				continue
			}

			logger.Info("Retrieved bootstrap node information", pkg.Fields{
				"bootstrap_id":   bootstrapNode.ID.Text(16)[:16],
				"bootstrap_addr": bootstrapNode.Address(),
			})

			// Join the ring
			if err := node.Join(bootstrapNode); err != nil {
				logger.Warn("Failed to join via this bootstrap node, trying next", pkg.Fields{
					"error":     err.Error(),
					"bootstrap": bootstrapAddr,
				})
				joinErr = err
				continue
			}

			logger.Info("Successfully joined Chord ring", pkg.Fields{
				"node_id":   node.ID().Text(16)[:16],
				"bootstrap": bootstrapAddr,
			})
			joined = true
			break
		}

		if !joined {
			logger.Error("Failed to join ring via any bootstrap node", pkg.Fields{
				"error": joinErr.Error(),
			})
			cleanup(node, grpcServer, grpcClient, httpServer, logger)
			os.Exit(1)
		}
	}

	logger.Info("Torus node is ready", nil)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	sig := <-sigChan
	logger.Info("Received shutdown signal", pkg.Fields{
		"signal": sig.String(),
	})

	// Cleanup
	cleanup(node, grpcServer, grpcClient, httpServer, logger)

	logger.Info("Torus node shutdown complete", nil)
}

// cleanup performs graceful shutdown of all components
func cleanup(node *chord.ChordNode, grpcServer *transport.GRPCServer, grpcClient *transport.GRPCClient, httpServer *api.Server, logger *pkg.Logger) {
	logger.Info("Starting graceful shutdown", nil)

	// Stop HTTP server
	if httpServer != nil {
		if err := httpServer.Stop(); err != nil {
			logger.Error("Error stopping HTTP server", pkg.Fields{
				"error": err.Error(),
			})
		}
	}

	// Stop gRPC server
	if err := grpcServer.Stop(); err != nil {
		logger.Error("Error stopping gRPC server", pkg.Fields{
			"error": err.Error(),
		})
	}

	// Shutdown ChordNode
	if err := node.Shutdown(); err != nil {
		logger.Error("Error shutting down Chord node", pkg.Fields{
			"error": err.Error(),
		})
	}

	// Close gRPC client connections
	if err := grpcClient.Close(); err != nil {
		logger.Error("Error closing gRPC client", pkg.Fields{
			"error": err.Error(),
		})
	}
}
