package pkg

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/diode"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Fields is a map of fields to add to log entries
type Fields map[string]any

var (
	instance *Logger
	once     sync.Once
	mu       sync.RWMutex

	// Pool for field maps to reduce allocations
	fieldPool = &sync.Pool{
		New: func() any {
			return make(Fields, 10)
		},
	}

	// sync.Once for setting zerolog global state (to prevent data races)
	timeFormatOnce sync.Once
	stackOnce      sync.Once
	callerSkipOnce sync.Once
)

// Logger wraps zerolog with additional functionality
type Logger struct {
	*zerolog.Logger
	config *Config
	fields Fields
	mu     sync.RWMutex
}

// Config holds logger configuration
type Config struct {
	// Level is the minimum log level (trace, debug, info, warn, error, fatal, panic)
	Level string `json:"level" yaml:"level"`

	// Format is the output format (json, console)
	Format string `json:"format" yaml:"format"`

	// TimestampFormat for logs
	TimestampFormat string `json:"timestamp_format" yaml:"timestamp_format"`

	// Console output settings
	Console ConsoleConfig `json:"console" yaml:"console"`

	// File output settings
	File FileConfig `json:"file" yaml:"file"`

	// Sampling reduces log volume in production
	Sampling SamplingConfig `json:"sampling" yaml:"sampling"`

	// Fields are default fields added to all logs
	Fields Fields `json:"fields" yaml:"fields"`

	// CallerSkipFrameCount for caller information
	CallerSkipFrameCount int `json:"caller_skip_frame_count" yaml:"caller_skip_frame_count"`

	// EnableCaller adds caller information to logs
	EnableCaller bool `json:"enable_caller" yaml:"enable_caller"`

	// EnableStackTrace for error logs
	EnableStackTrace bool `json:"enable_stack_trace" yaml:"enable_stack_trace"`

	// AsyncWrite uses a diode writer for better performance
	AsyncWrite bool `json:"async_write" yaml:"async_write"`

	// BufferSize for async writer (in bytes)
	BufferSize int `json:"buffer_size" yaml:"buffer_size"`
}

// ConsoleConfig for console output
type ConsoleConfig struct {
	// Enable console output
	Enable bool `json:"enable" yaml:"enable"`

	// NoColor disables color output
	NoColor bool `json:"no_color" yaml:"no_color"`

	// TimeFormat for console output
	TimeFormat string `json:"time_format" yaml:"time_format"`

	// Output target (stdout, stderr)
	Output string `json:"output" yaml:"output"`
}

// FileConfig for file output
type FileConfig struct {
	// Enable file output
	Enable bool `json:"enable" yaml:"enable"`

	// Path to log file
	Path string `json:"path" yaml:"path"`

	// MaxSize in megabytes
	MaxSize int `json:"max_size" yaml:"max_size"`

	// MaxAge in days
	MaxAge int `json:"max_age" yaml:"max_age"`

	// MaxBackups to keep
	MaxBackups int `json:"max_backups" yaml:"max_backups"`

	// LocalTime uses local time for rotation
	LocalTime bool `json:"local_time" yaml:"local_time"`

	// Compress rotated files
	Compress bool `json:"compress" yaml:"compress"`
}

// SamplingConfig for log sampling
type SamplingConfig struct {
	// Enable sampling
	Enable bool `json:"enable" yaml:"enable"`

	// Initial samples per second
	Initial uint32 `json:"initial" yaml:"initial"`

	// Thereafter samples after initial
	Thereafter uint32 `json:"thereafter" yaml:"thereafter"`
}

// DefaultConfig returns default logger configuration
func DefaultConfig() *Config {
	return &Config{
		Level:           "info",
		Format:          "json",
		TimestampFormat: time.RFC3339Nano,
		Console: ConsoleConfig{
			Enable:     true,
			NoColor:    false,
			TimeFormat: "15:04:05.000",
			Output:     "stdout",
		},
		File: FileConfig{
			Enable:     false,
			Path:       "app.log",
			MaxSize:    100, // 100MB
			MaxAge:     30,  // 30 days
			MaxBackups: 10,
			LocalTime:  true,
			Compress:   true,
		},
		Sampling: SamplingConfig{
			Enable:     false,
			Initial:    100,
			Thereafter: 100,
		},
		Fields:               make(Fields),
		CallerSkipFrameCount: 2,
		EnableCaller:         true,
		EnableStackTrace:     true,
		AsyncWrite:           false,
		BufferSize:           10000,
	}
}

// Init initializes the global logger with configuration
func Init(config *Config) error {
	if config == nil {
		config = DefaultConfig()
	}

	logger, err := New(config)
	if err != nil {
		return err
	}

	SetGlobal(logger)
	return nil
}

// New creates a new logger instance
func New(config *Config) (*Logger, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Parse log level
	level, err := zerolog.ParseLevel(config.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}

	// Setup writers
	writers := []io.Writer{}

	// Console writer
	if config.Console.Enable {
		var output io.Writer
		switch config.Console.Output {
		case "stderr":
			output = os.Stderr
		default:
			output = os.Stdout
		}

		if config.Format == "console" {
			consoleWriter := zerolog.ConsoleWriter{
				Out:        output,
				TimeFormat: config.Console.TimeFormat,
				NoColor:    config.Console.NoColor,
			}
			writers = append(writers, consoleWriter)
		} else {
			writers = append(writers, output)
		}
	}

	// File writer
	if config.File.Enable {
		if err := os.MkdirAll(filepath.Dir(config.File.Path), 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}

		fileWriter := &lumberjack.Logger{
			Filename:   config.File.Path,
			MaxSize:    config.File.MaxSize,
			MaxAge:     config.File.MaxAge,
			MaxBackups: config.File.MaxBackups,
			LocalTime:  config.File.LocalTime,
			Compress:   config.File.Compress,
		}
		writers = append(writers, fileWriter)
	}

	// Create multi writer
	var writer io.Writer
	switch len(writers) {
	case 0:
		writer = io.Discard
	case 1:
		writer = writers[0]
	default:
		writer = zerolog.MultiLevelWriter(writers...)
	}

	// Setup async writer if enabled
	if config.AsyncWrite {
		writer = diode.NewWriter(writer, config.BufferSize, time.Second, func(missed int) {
			fmt.Fprintf(os.Stderr, "Logger dropped %d messages\n", missed)
		})
	}

	// IMPORTANT: Set global caller skip count BEFORE creating the logger
	// Use sync.Once to prevent data races when multiple loggers are created concurrently
	if config.EnableCaller {
		callerSkipOnce.Do(func() {
			zerolog.CallerSkipFrameCount = config.CallerSkipFrameCount
		})
	}

	// Create context with configuration
	contextBuilder := zerolog.New(writer).
		Level(level).
		With().
		Timestamp()

	// Add caller if enabled (use .Caller() which will use the global skip count)
	if config.EnableCaller {
		contextBuilder = contextBuilder.Caller()
	}

	// Add default fields
	for k, v := range config.Fields {
		contextBuilder = contextBuilder.Interface(k, v)
	}

	ctx := contextBuilder

	// Add stack trace for errors if enabled
	if config.EnableStackTrace {
		stackOnce.Do(func() {
			zerolog.ErrorStackMarshaler = func(err error) any {
				return fmt.Sprintf("%+v", err)
			}
		})
	}

	// Setup sampling if enabled
	var zl zerolog.Logger
	if config.Sampling.Enable {
		zl = ctx.Logger().Sample(&zerolog.BasicSampler{
			N: config.Sampling.Initial,
		})
	} else {
		zl = ctx.Logger()
	}

	// Set global time format (use sync.Once to set only once to prevent data races)
	if config.TimestampFormat != "" {
		timeFormatOnce.Do(func() {
			zerolog.TimeFieldFormat = config.TimestampFormat
		})
	}

	return &Logger{
		Logger: &zl,
		config: config,
		fields: make(Fields),
	}, nil
}

// SetGlobal sets the global logger instance
func SetGlobal(l *Logger) {
	mu.Lock()
	defer mu.Unlock()
	instance = l
}

// Get returns the global logger instance
func Get() *Logger {
	once.Do(func() {
		if instance == nil {
			l, _ := New(DefaultConfig())
			instance = l
		}
	})
	return instance
}

// With creates a child logger with additional fields
func (l *Logger) With() *Context {
	return &Context{
		logger: l,
		fields: make(Fields),
	}
}

// WithContext creates a logger with context
func (l *Logger) WithContext(ctx context.Context) *Logger {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Extract context values if needed
	zctx := l.Logger.With()

	// Add trace ID if present
	if traceID := ctx.Value("trace_id"); traceID != nil {
		zctx = zctx.Str("trace_id", fmt.Sprint(traceID))
	}

	// Add request ID if present
	if reqID := ctx.Value("request_id"); reqID != nil {
		zctx = zctx.Str("request_id", fmt.Sprint(reqID))
	}

	zl := zctx.Logger()
	return &Logger{
		Logger: &zl,
		config: l.config,
		fields: l.fields,
	}
}

// UpdateLevel updates the log level dynamically
func (l *Logger) UpdateLevel(level string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		return err
	}

	newLogger := l.Logger.Level(lvl)
	l.Logger = &newLogger
	l.config.Level = level
	return nil
}

// AddField adds a persistent field to the logger
func (l *Logger) AddField(key string, value any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.fields[key] = value
	ctx := l.Logger.With().Interface(key, value)
	zl := ctx.Logger()
	l.Logger = &zl
}

// RemoveField removes a persistent field from the logger
func (l *Logger) RemoveField(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	delete(l.fields, key)

	// Recreate logger without the field
	ctx := zerolog.New(l.Logger).With().Timestamp()
	for k, v := range l.fields {
		ctx = ctx.Interface(k, v)
	}
	zl := ctx.Logger()
	l.Logger = &zl
}

// Context for building log entries with fields
type Context struct {
	logger *Logger
	fields Fields
	mu     sync.RWMutex
}

// Str adds a string field
func (c *Context) Str(key, val string) *Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fields[key] = val
	return c
}

// Int adds an integer field
func (c *Context) Int(key string, val int) *Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fields[key] = val
	return c
}

// Bool adds a boolean field
func (c *Context) Bool(key string, val bool) *Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fields[key] = val
	return c
}

// Err adds an error field
func (c *Context) Err(err error) *Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.fields["error"] = err.Error()
	}
	return c
}

// Interface adds an interface field
func (c *Context) Interface(key string, val any) *Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.fields[key] = val
	return c
}

// Logger returns the logger with context fields applied
func (c *Context) Logger() *Logger {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ctx := c.logger.Logger.With()
	for k, v := range c.fields {
		ctx = ctx.Interface(k, v)
	}

	zl := ctx.Logger()
	return &Logger{
		Logger: &zl,
		config: c.logger.config,
		fields: c.logger.fields,
	}
}

// Msg sends the log message
func (c *Context) Msg(msg string) {
	c.Logger().Info().Msg(msg)
}

// Global logger convenience functions

// Debug logs at debug level
func Debug() *zerolog.Event {
	return Get().Debug()
}

// Info logs at info level
func Info() *zerolog.Event {
	return Get().Info()
}

// Warn logs at warn level
func Warn() *zerolog.Event {
	return Get().Warn()
}

// Error logs at error level
func Error() *zerolog.Event {
	return Get().Error()
}

// Fatal logs at fatal level and exits
func Fatal() *zerolog.Event {
	return Get().Fatal()
}

// Panic logs at panic level and panics
func Panic() *zerolog.Event {
	return Get().Panic()
}

// WithContext returns logger with context
func WithContext(ctx context.Context) *Logger {
	return Get().WithContext(ctx)
}

// WithFields creates a new logger with additional fields using sync.Pool
func (l *Logger) WithFields(fields Fields) *Logger {
	// Get new map from pool
	newFields := fieldPool.Get().(Fields)

	// Copy existing fields and get logger pointer (thread-safe)
	l.mu.RLock()
	for k, v := range l.fields {
		newFields[k] = v
	}
	baseLogger := l.Logger
	l.mu.RUnlock()

	// Add new fields
	for k, v := range fields {
		newFields[k] = v
	}

	// Create new zerolog context with all fields
	ctx := baseLogger.With()
	for k, v := range newFields {
		ctx = ctx.Interface(k, v)
	}

	zl := ctx.Logger()
	return &Logger{
		Logger: &zl,
		config: l.config,
		fields: newFields,
	}
}

// WithError creates a new logger with error details added
func (l *Logger) WithError(err error) *Logger {
	if err == nil {
		return l
	}

	fields := fieldPool.Get().(Fields)

	// Copy existing fields
	l.mu.RLock()
	for k, v := range l.fields {
		fields[k] = v
	}
	l.mu.RUnlock()

	// Add error details
	fields["error"] = err.Error()
	fields["error_type"] = fmt.Sprintf("%T", err)

	/*
		TODO: Check for custom error types if needed
		You can extend this to handle your custom error types
	*/

	ctx := l.Logger.With()
	for k, v := range fields {
		ctx = ctx.Interface(k, v)
	}

	zl := ctx.Logger()
	return &Logger{
		Logger: &zl,
		config: l.config,
		fields: fields,
	}
}

// ReleaseFields returns the fields map to the pool
func (l *Logger) ReleaseFields() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.fields != nil && len(l.fields) > 0 {
		// Clear the map
		for k := range l.fields {
			delete(l.fields, k)
		}
		// Return to pool
		fieldPool.Put(l.fields)
		l.fields = nil
	}
}

// Close closes the logger and flushes any buffered logs
func (l *Logger) Close() error {
	// Release fields back to pool
	l.ReleaseFields()

	l.mu.Lock()
	defer l.mu.Unlock()

	// If using async writer, we need to close it properly
	if l.config.AsyncWrite {
		// Give some time for final flush
		time.Sleep(100 * time.Millisecond)
	}

	return nil
}
