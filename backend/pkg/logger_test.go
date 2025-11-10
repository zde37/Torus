package pkg

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoggerCreation(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "Default config",
			config:  DefaultConfig(),
			wantErr: false,
		},
		{
			name:    "Nil config uses default",
			config:  nil,
			wantErr: false,
		},
		{
			name: "Invalid log level",
			config: &Config{
				Level: "invalid",
			},
			wantErr: false, // Should default to info
		},
		{
			name: "Console output",
			config: &Config{
				Level:  "debug",
				Format: "console",
				Console: ConsoleConfig{
					Enable: true,
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := New(tt.config)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, logger)
			}
		})
	}
}

func TestLoggerWithFields(t *testing.T) {
	config := DefaultConfig()
	config.Level = "debug"

	logger, err := New(config)
	require.NoError(t, err)

	// Test WithFields
	fields := Fields{
		"component": "test",
		"version":   "1.0.0",
		"count":     42,
	}

	childLogger := logger.WithFields(fields)
	assert.NotNil(t, childLogger)
	assert.NotEqual(t, logger, childLogger)

	// Verify fields are actually set
	assert.Equal(t, fields["component"], childLogger.fields["component"])
	assert.Equal(t, fields["version"], childLogger.fields["version"])
	assert.Equal(t, fields["count"], childLogger.fields["count"])
}

func TestLoggerWithError(t *testing.T) {
	config := DefaultConfig()
	logger, err := New(config)
	require.NoError(t, err)

	// Test with nil error
	nilLogger := logger.WithError(nil)
	assert.Equal(t, logger, nilLogger)

	// Test with actual error
	testErr := assert.AnError
	errLogger := logger.WithError(testErr)
	assert.NotNil(t, errLogger)
	assert.NotEqual(t, logger, errLogger)
	assert.Equal(t, testErr.Error(), errLogger.fields["error"])
	assert.NotEmpty(t, errLogger.fields["error_type"])
}

func TestLoggerWithContext(t *testing.T) {
	config := DefaultConfig()
	logger, err := New(config)
	require.NoError(t, err)

	// Create context with values
	ctx := context.Background()
	ctx = context.WithValue(ctx, "trace_id", "test-trace-123")
	ctx = context.WithValue(ctx, "request_id", "req-456")

	// Get logger with context
	ctxLogger := logger.WithContext(ctx)
	assert.NotNil(t, ctxLogger)
}

func TestLoggerUpdateLevel(t *testing.T) {
	config := DefaultConfig()
	config.Level = "info"

	logger, err := New(config)
	require.NoError(t, err)

	// Update to valid level
	err = logger.UpdateLevel("debug")
	assert.NoError(t, err)
	assert.Equal(t, "debug", logger.config.Level)

	// Update to invalid level
	err = logger.UpdateLevel("invalid")
	assert.Error(t, err)
}

func TestLoggerFieldOperations(t *testing.T) {
	config := DefaultConfig()
	logger, err := New(config)
	require.NoError(t, err)

	// Add field
	logger.AddField("key1", "value1")
	assert.Equal(t, "value1", logger.fields["key1"])

	// Add another field
	logger.AddField("key2", 42)
	assert.Equal(t, 42, logger.fields["key2"])

	// Remove field
	logger.RemoveField("key1")
	_, exists := logger.fields["key1"]
	assert.False(t, exists)
	assert.Equal(t, 42, logger.fields["key2"]) // key2 should still exist
}

func TestGlobalLogger(t *testing.T) {
	// Reset global instance for testing
	instance = nil

	// Initialize global logger
	err := Init(DefaultConfig())
	assert.NoError(t, err)

	// Get global logger
	logger := Get()
	assert.NotNil(t, logger)

	// Multiple Get() calls should return same instance
	logger2 := Get()
	assert.Equal(t, logger, logger2)
}

func TestLoggerContext(t *testing.T) {
	config := DefaultConfig()
	logger, err := New(config)
	require.NoError(t, err)

	ctx := logger.With()
	assert.NotNil(t, ctx)

	// Test chaining
	ctx.
		Str("key1", "value1").
		Int("key2", 42).
		Bool("key3", true).
		Interface("key4", map[string]string{"nested": "value"})

	// Get logger from context
	contextLogger := ctx.Logger()
	assert.NotNil(t, contextLogger)
}

func TestLoggerOutput(t *testing.T) {
	// Create config with JSON format
	config := DefaultConfig()
	config.Level = "debug"
	config.Format = "json"
	config.Console.Enable = false // Disable console to use custom writer

	// We need to test by creating a logger that writes to our buffer
	// This would require modifying the New function to accept custom writers
	// For now, we'll test the global convenience functions

	t.Run("Global convenience functions", func(t *testing.T) {
		// Initialize global logger
		err := Init(config)
		assert.NoError(t, err)

		// Test convenience functions exist and don't panic
		assert.NotPanics(t, func() {
			Debug()
			Info()
			Warn()
			Error()
		})
	})
}

func TestReleaseFields(t *testing.T) {
	config := DefaultConfig()
	logger, err := New(config)
	require.NoError(t, err)

	// Add some fields
	logger.fields = Fields{
		"key1": "value1",
		"key2": "value2",
	}

	// Release fields
	logger.ReleaseFields()

	// Check fields are cleared
	assert.Nil(t, logger.fields)
}

func TestLoggerClose(t *testing.T) {
	config := DefaultConfig()
	config.AsyncWrite = true

	logger, err := New(config)
	require.NoError(t, err)

	// Add fields before closing
	logger.AddField("test", "value")

	// Close should not error
	err = logger.Close()
	assert.NoError(t, err)

	// Fields should be nil after close
	assert.Nil(t, logger.fields)
}

func TestFileConfig(t *testing.T) {
	config := DefaultConfig()
	config.File = FileConfig{
		Enable:     true,
		Path:       "/tmp/test.log",
		MaxSize:    10,
		MaxAge:     1,
		MaxBackups: 1,
		LocalTime:  true,
		Compress:   false,
	}

	logger, err := New(config)
	assert.NoError(t, err)
	assert.NotNil(t, logger)

	// Clean up
	logger.Close()
}

func TestSamplingConfig(t *testing.T) {
	config := DefaultConfig()
	config.Sampling = SamplingConfig{
		Enable:     true,
		Initial:    10,
		Thereafter: 100,
	}

	logger, err := New(config)
	assert.NoError(t, err)
	assert.NotNil(t, logger)
}

func BenchmarkLoggerWithFields(b *testing.B) {
	config := DefaultConfig()
	logger, _ := New(config)

	fields := Fields{
		"component": "benchmark",
		"version":   "1.0.0",
		"index":     123,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = logger.WithFields(fields)
		}
	})
}

func BenchmarkLoggerAddField(b *testing.B) {
	config := DefaultConfig()
	logger, _ := New(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		logger.AddField("key", "value")
	}
}

func TestLogLevelParsing(t *testing.T) {
	tests := []struct {
		level    string
		expected string
	}{
		{"trace", "trace"},
		{"debug", "debug"},
		{"info", "info"},
		{"warn", "warn"},
		{"error", "error"},
		{"invalid", "info"}, // Should default to info
		{"", "info"},        // Empty should default to info
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			config := DefaultConfig()
			config.Level = tt.level

			logger, err := New(config)
			assert.NoError(t, err)

			// Since we default to info for invalid levels
			// we can't directly test the level, but we can ensure
			// the logger was created successfully
			assert.NotNil(t, logger)
		})
	}
}

// TestJSONOutput tests that JSON output is properly formatted
func TestJSONOutput(t *testing.T) {
	// Create a buffer to capture output
	buf := &bytes.Buffer{}

	config := DefaultConfig()
	config.Level = "debug"
	config.Format = "json"
	config.Console.Enable = false // We'll write to buffer instead

	// For this test, we verify that we can create a logger with JSON format
	// and use it without errors. Since zerolog writes to io.Writer,
	// we would need to modify the New function to accept custom writers
	// to fully test JSON output parsing.
	logger, err := New(config)
	require.NoError(t, err)

	// Verify logger methods work correctly
	assert.NotPanics(t, func() {
		logger.Debug().
			Str("test", "value").
			Int("count", 42).
			Msg("test message")

		logger.Info().Msg("info message")
		logger.Warn().Msg("warn message")
		logger.Error().Err(assert.AnError).Msg("error message")
	})

	// Verify the logger was created with correct config
	assert.Equal(t, "debug", logger.config.Level)
	assert.Equal(t, "json", logger.config.Format)

	// The buffer remains unused here because we need to modify
	// the logger implementation to accept custom writers for testing
	_ = buf
}

// TestConcurrentAccess tests thread safety
func TestConcurrentAccess(t *testing.T) {
	config := DefaultConfig()
	logger, err := New(config)
	require.NoError(t, err)

	// Run concurrent operations
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			defer func() { done <- true }()

			// Add field
			logger.AddField(strings.TrimSpace(strings.Join([]string{"goroutine", string(rune(id))}, "_")), id)

			// Create child logger
			child := logger.WithFields(Fields{
				"goroutine": id,
			})
			_ = child

			// Update level
			if id%2 == 0 {
				_ = logger.UpdateLevel("debug")
			} else {
				_ = logger.UpdateLevel("info")
			}

			// Remove field
			logger.RemoveField("test")
		}(i)
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Test timed out")
		}
	}
}

// TestFieldPool tests that the sync.Pool for fields is working
func TestFieldPool(t *testing.T) {
	config := DefaultConfig()
	logger, err := New(config)
	require.NoError(t, err)

	// Create multiple child loggers to test pool reuse
	for i := 0; i < 100; i++ {
		fields := Fields{
			"iteration": i,
			"test":      "pool",
		}
		child := logger.WithFields(fields)
		assert.NotNil(t, child)

		// Release fields back to pool
		child.ReleaseFields()
	}
}

// TestContextMethods tests the Context struct methods
func TestContextMethods(t *testing.T) {
	config := DefaultConfig()
	logger, err := New(config)
	require.NoError(t, err)

	ctx := logger.With()
	require.NotNil(t, ctx)

	// Test Err method
	ctx.Err(assert.AnError)
	assert.Equal(t, assert.AnError.Error(), ctx.fields["error"])

	// Test Msg method (which logs)
	assert.NotPanics(t, func() {
		ctx.Msg("test message")
	})
}

// TestGlobalWithContext tests the global WithContext function
func TestGlobalWithContext(t *testing.T) {
	// Initialize global logger
	err := Init(DefaultConfig())
	require.NoError(t, err)

	// Create context with values
	ctx := context.Background()
	ctx = context.WithValue(ctx, "trace_id", "test-123")

	// Test global WithContext function
	logger := WithContext(ctx)
	assert.NotNil(t, logger)
}

// TestLoggerFileCreation tests file logging setup
func TestLoggerFileCreation(t *testing.T) {
	tmpDir := t.TempDir()
	config := DefaultConfig()
	config.File = FileConfig{
		Enable:     true,
		Path:       tmpDir + "/test.log",
		MaxSize:    1,
		MaxAge:     1,
		MaxBackups: 1,
		LocalTime:  true,
		Compress:   false,
	}

	logger, err := New(config)
	assert.NoError(t, err)
	assert.NotNil(t, logger)

	// Log something to ensure file is created
	logger.Info().Msg("test file logging")

	// Cleanup
	logger.Close()
}

// TestLoggerAsyncWrite tests async write configuration
func TestLoggerAsyncWrite(t *testing.T) {
	config := DefaultConfig()
	config.AsyncWrite = true
	config.BufferSize = 1000

	logger, err := New(config)
	assert.NoError(t, err)
	assert.NotNil(t, logger)

	// Log several messages
	for i := 0; i < 10; i++ {
		logger.Info().Int("count", i).Msg("async test")
	}

	// Close should flush async writer
	err = logger.Close()
	assert.NoError(t, err)
}

// TestLoggerNoOutput tests logger with no output
func TestLoggerNoOutput(t *testing.T) {
	config := DefaultConfig()
	config.Console.Enable = false
	config.File.Enable = false

	logger, err := New(config)
	assert.NoError(t, err)
	assert.NotNil(t, logger)

	// Should not panic even with no output
	assert.NotPanics(t, func() {
		logger.Info().Msg("no output test")
	})
}
