package pkg

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Fields is a map of fields to add to log entries
type Fields map[string]any

const (
	// Log levels
	LogLevelDebug = "debug"
	LogLevelInfo  = "info"
	LogLevelWarn  = "warn"
	LogLevelError = "error"
	LogLevelFatal = "fatal"

	// Log formats
	LogFormatJSON = "json"
	LogFormatText = "text"

	// Log outputs
	LogOutputStdout = "stdout"
	LogOutputStderr = "stderr"
	LogOutputFile   = "file"
)

// Logger wraps zerolog with additional functionality
type Logger struct {
	zl zerolog.Logger
}

// Config holds logger configuration
type Config struct {
	// Level is the minimum log level (debug, info, warn, error, fatal)
	Level string `json:"level" yaml:"level"`

	// Format is the output format (json, console)
	Format string `json:"format" yaml:"format"`

	// Output destinations (stdout, file, etc.)
	Outputs []string `yaml:"outputs,omitempty" json:"outputs,omitempty"`

	// File output settings
	File FileConfig `json:"file" yaml:"file"`
}

// FileConfig for file output
type FileConfig struct {
	// Path to log file
	Path string `json:"path" yaml:"path"`

	// MaxSize in megabytes
	MaxSize int `json:"max_size" yaml:"max_size"`

	// MaxAge in days
	MaxAge int `json:"max_age" yaml:"max_age"`

	// MaxBackups to keep
	MaxBackups int `json:"max_backups" yaml:"max_backups"`

	// Compress rotated files
	Compress bool `json:"compress" yaml:"compress"`
}

// NewLogger creates a new logger instance
func NewLogger(cfg *Config) (*Logger, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	// Parse log level
	level, err := zerolog.ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("invalid log level %q: %w", cfg.Level, err)
	}

	// Determine output writers
	writers := make([]io.Writer, 0, len(cfg.Outputs))
	for _, output := range cfg.Outputs {
		switch output {
		case LogOutputStdout:
			if cfg.Format == LogFormatText {
				writers = append(writers, zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339Nano})
			} else {
				writers = append(writers, os.Stdout)
			}
		case LogOutputStderr:
			if cfg.Format == LogFormatText {
				writers = append(writers, zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339Nano})
			} else {
				writers = append(writers, os.Stderr)
			}
		case LogOutputFile:
			if cfg.File.Path == "" {
				return nil, fmt.Errorf("file output specified but no file path provided")
			}
			fileWriter := &lumberjack.Logger{
				Filename:   cfg.File.Path,
				MaxSize:    cfg.File.MaxSize,
				MaxAge:     cfg.File.MaxAge,
				MaxBackups: cfg.File.MaxBackups,
				Compress:   cfg.File.Compress,
			}
			writers = append(writers, fileWriter)
		default:
			return nil, fmt.Errorf("unsupported log output: %s", output)
		}
	}

	var writer io.Writer
	if len(writers) == 0 {
		writer = io.Discard
	} else if len(writers) == 1 {
		writer = writers[0]
	} else {
		writer = io.MultiWriter(writers...)
	}

	// Enable caller information and stack trace
	zerolog.ErrorStackMarshaler = func(err error) any {
		return err.Error()
	}

	zl := zerolog.New(writer).
		Level(level).
		With().
		Timestamp().
		Str("prefix", "torus").
		Int("pid", os.Getpid()).
		CallerWithSkipFrameCount(3).
		Stack().
		Logger()

	return &Logger{zl: zl}, nil
}

// DefaultConfig returns default logger configuration
func DefaultConfig() *Config {
	return &Config{
		Level:  LogLevelInfo,
		Format: LogFormatJSON,
		File: FileConfig{
			Path:       "app.log",
			MaxSize:    100, // 100MB
			MaxAge:     30,  // 30 days
			MaxBackups: 10,
			Compress:   true,
		},
		Outputs: []string{LogOutputStdout},
	}
}

// Debug logs a debug message with optional fields.
func (l *Logger) Debug(msg string, fields Fields) {
	event := l.zl.Debug()
	for key, value := range fields {
		event = event.Interface(key, value)
	}
	event.Msg(msg)
}

// Info logs an info message with optional fields.
func (l *Logger) Info(msg string, fields Fields) {
	event := l.zl.Info()
	for key, value := range fields {
		event = event.Interface(key, value)
	}
	event.Msg(msg)
}

// Warn logs a warning message with optional fields.
func (l *Logger) Warn(msg string, fields Fields) {
	event := l.zl.Warn()
	for key, value := range fields {
		event = event.Interface(key, value)
	}
	event.Msg(msg)
}

// Error logs an error message with optional fields.
func (l *Logger) Error(msg string, fields Fields) {
	event := l.zl.Error()
	for key, value := range fields {
		event = event.Interface(key, value)
	}
	event.Msg(msg)
}

// Fatal logs a fatal message with optional fields and exits the program.
func (l *Logger) Fatal(msg string, fields Fields) {
	event := l.zl.Fatal()
	for key, value := range fields {
		event = event.Interface(key, value)
	}
	event.Msg(msg)
}
