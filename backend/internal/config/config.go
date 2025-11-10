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

// DefaultConfig returns a sensible default configuration
func DefaultConfig() *Config {
	return &Config{
		Host:               "127.0.0.1",
		Port:               8440,
		HTTPPort:           8080,
		M:                  160, // 2^160 address space
		StabilizeInterval:  1 * time.Second,
		FixFingersInterval: 3 * time.Second,
		SuccessorListSize:  3, // MVP: small successor list
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
