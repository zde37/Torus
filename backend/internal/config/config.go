package config

import (
	"fmt"
	"time"
)

// Config holds all configuration for a Chord node
type Config struct {
	// Node identification
	NodeID string
	Host   string
	Port   int

	// HTTP API
	HTTPPort int

	// Bootstrap
	BootstrapNodes []string

	// Authentication
	AuthToken string // Shared secret for node authentication

	// Chord parameters
	M                  int           // Identifier space size in bits (160)
	StabilizeInterval  time.Duration // How often to run stabilization
	FixFingersInterval time.Duration // How often to fix finger table
	SuccessorListSize  int           // Number of successors to maintain
	RPCTimeout         time.Duration // Timeout for RPC calls

	// Logging (compatible with your logger)
	LogLevel  string // trace, debug, info, warn, error
	LogFormat string // json, console
}

// DefaultBootstrapNodes contains fallback bootstrap nodes for public network
// Override with -bootstrap flag or BootstrapNodes config
var DefaultBootstrapNodes = []string{
	"torus.zde37.com:8440",
}

// DefaultConfig returns a sensible default configuration
func DefaultConfig() *Config {
	return &Config{
		Host:               "127.0.0.1",
		Port:               8440,
		HTTPPort:           8080,
		BootstrapNodes:     DefaultBootstrapNodes,
		AuthToken:          "", // No auth by default (set via flag or env)
		M:                  160, // 2^160 address space
		StabilizeInterval:  1 * time.Second,
		FixFingersInterval: 3 * time.Second,
		SuccessorListSize:  8, // Advanced: maintain 8 successors for fault tolerance
		RPCTimeout:         5 * time.Second,
		LogLevel:           "info",
		LogFormat:          "console",
	}
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	if c.M <= 0 || c.M > 256 {
		return fmt.Errorf("M must be between 1 and 256, got %d", c.M)
	}
	if c.Port <= 0 || c.Port > 65535 {
		return fmt.Errorf("invalid port: %d", c.Port)
	}
	if c.HTTPPort <= 0 || c.HTTPPort > 65535 {
		return fmt.Errorf("invalid HTTP port: %d", c.HTTPPort)
	}
	return nil
}
