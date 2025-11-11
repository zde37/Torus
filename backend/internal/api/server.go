package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/zde37/torus/pkg"
	pb "github.com/zde37/torus/protobuf/protogen"
)

// Server represents the HTTP API gateway server.
type Server struct {
	httpServer *http.Server
	wsHub      *WebSocketHub
	logger     *pkg.Logger
	grpcAddr   string
	ctx        context.Context
	cancel     context.CancelFunc
}

// Config holds the HTTP server configuration.
type Config struct {
	HTTPPort int
	GRPCAddr string
}

// NewServer creates a new HTTP API gateway server.
func NewServer(cfg *Config, logger *pkg.Logger) (*Server, error) {
	if logger == nil {
		return nil, fmt.Errorf("logger cannot be nil")
	}

	wsHub := NewWebSocketHub(logger)

	return &Server{
		logger:   logger.WithFields(pkg.Fields{"component": "http_api"}),
		grpcAddr: cfg.GRPCAddr,
		wsHub:    wsHub,
	}, nil
}

// Start starts the HTTP server.
func (s *Server) Start(port int) error {
	s.ctx, s.cancel = context.WithCancel(context.Background())

	// Create gRPC-gateway mux
	mux := runtime.NewServeMux(
		runtime.WithMarshalerOption(runtime.MIMEWildcard, &runtime.JSONPb{
			MarshalOptions: runtime.JSONPb{}.MarshalOptions,
			UnmarshalOptions: runtime.JSONPb{}.UnmarshalOptions,
		}),
	)

	// Setup dial options for connecting to gRPC server
	opts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}

	// Register ChordService handler
	err := pb.RegisterChordServiceHandlerFromEndpoint(s.ctx, mux, s.grpcAddr, opts)
	if err != nil {
		return fmt.Errorf("failed to register gateway: %w", err)
	}

	// Start WebSocket hub
	go s.wsHub.Run()

	// Create HTTP mux with CORS middleware
	httpMux := http.NewServeMux()

	// Register grpc-gateway routes
	httpMux.Handle("/api/", http.StripPrefix("", corsMiddleware(mux)))

	// WebSocket endpoint for live updates
	httpMux.HandleFunc("/api/ws", s.wsHub.HandleWebSocket)

	// Health check endpoint
	httpMux.HandleFunc("/health", s.healthHandler)

	// Create HTTP server
	addr := fmt.Sprintf(":%d", port)
	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      httpMux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	s.logger.Info().
		Int("port", port).
		Str("grpc_addr", s.grpcAddr).
		Msg("Starting HTTP API server")

	// Start server in goroutine
	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error().Err(err).Msg("HTTP server error")
		}
	}()

	s.logger.Info().Int("port", port).Msg("HTTP API server started")
	return nil
}

// Stop gracefully stops the HTTP server.
func (s *Server) Stop() error {
	s.logger.Info().Msg("Stopping HTTP API server")

	// Cancel the grpc-gateway context first
	if s.cancel != nil {
		s.cancel()
	}

	// Stop WebSocket hub
	if s.wsHub != nil {
		s.wsHub.Stop()
	}

	// Shutdown HTTP server
	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := s.httpServer.Shutdown(ctx); err != nil {
			return fmt.Errorf("failed to shutdown HTTP server: %w", err)
		}
	}

	s.logger.Info().Msg("HTTP API server stopped")
	return nil
}

// healthHandler handles health check requests.
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

// corsMiddleware adds CORS headers to responses.
func corsMiddleware(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		h.ServeHTTP(w, r)
	})
}
