package pkg

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLogger(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{
			name: "default config",
			cfg:  nil,
		},
		{
			name: "stdout json output",
			cfg: &Config{
				Level:   LogLevelDebug,
				Format:  LogFormatJSON,
				Outputs: []string{LogOutputStdout},
			},
		},
		{
			name: "stdout text output",
			cfg: &Config{
				Level:   LogLevelInfo,
				Format:  LogFormatText,
				Outputs: []string{LogOutputStdout},
			},
		},
		{
			name: "stderr output",
			cfg: &Config{
				Level:   LogLevelWarn,
				Format:  LogFormatJSON,
				Outputs: []string{LogOutputStderr},
			},
		},
		{
			name: "no output",
			cfg: &Config{
				Level:   LogLevelInfo,
				Format:  LogFormatJSON,
				Outputs: []string{},
			},
		},
		{
			name: "invalid level",
			cfg: &Config{
				Level:   "invalid",
				Format:  LogFormatJSON,
				Outputs: []string{LogOutputStdout},
			},
			wantErr: true,
		},
		{
			name: "file output without path",
			cfg: &Config{
				Level:   LogLevelInfo,
				Format:  LogFormatJSON,
				Outputs: []string{LogOutputFile},
			},
			wantErr: true,
		},
		{
			name: "unsupported output",
			cfg: &Config{
				Level:   LogLevelInfo,
				Format:  LogFormatJSON,
				Outputs: []string{"syslog"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, err := NewLogger(tt.cfg)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, logger)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, logger)
			}
		})
	}
}

func TestLoggerInterface(t *testing.T) {
	logger, err := NewLogger(&Config{
		Level:   LogLevelDebug,
		Format:  LogFormatJSON,
		Outputs: []string{},
	})
	require.NoError(t, err)

	assert.NotPanics(t, func() {
		logger.Debug("debug message", nil)
		logger.Debug("debug with fields", Fields{"test": "value"})
		logger.Info("info message", Fields{"key": "value"})
		logger.Warn("warn message", Fields{"count": 42})
		logger.Error("error message", Fields{"error": "something failed"})
	})
}

func TestLoggerConcurrent(t *testing.T) {
	logger, err := NewLogger(nil)
	require.NoError(t, err)

	done := make(chan bool)
	for i := 0; i < 100; i++ {
		go func(id int) {
			defer func() { done <- true }()
			logger.Info("concurrent log", Fields{"goroutine": id})
		}(i)
	}

	for i := 0; i < 100; i++ {
		<-done
	}
}

type CustomLogger struct {
	messages []string
}

func (c *CustomLogger) Debug(msg string, fields Fields) {
	c.messages = append(c.messages, msg)
}

func (c *CustomLogger) Info(msg string, fields Fields) {
	c.messages = append(c.messages, msg)
}

func (c *CustomLogger) Warn(msg string, fields Fields) {
	c.messages = append(c.messages, msg)
}

func (c *CustomLogger) Error(msg string, fields Fields) {
	c.messages = append(c.messages, msg)
}

func (c *CustomLogger) Fatal(msg string, fields Fields) {
	c.messages = append(c.messages, msg)
}

func TestCustomLoggerImplementation(t *testing.T) {
	logger := &CustomLogger{}

	logger.Info("test message", nil)
	logger.Error("error message", Fields{"key": "value"})

	require.Len(t, logger.messages, 2)
	assert.Equal(t, "test message", logger.messages[0])
	assert.Equal(t, "error message", logger.messages[1])
}

func TestFileOutput(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger, err := NewLogger(&Config{
		Level:   LogLevelInfo,
		Format:  LogFormatJSON,
		Outputs: []string{LogOutputFile},
		File: FileConfig{
			Path:       logFile,
			MaxSize:    1,
			MaxAge:     7,
			MaxBackups: 3,
			Compress:   false,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, logger)

	logger.Info("test message", nil)

	_, err = os.Stat(logFile)
	assert.NoError(t, err, "log file should exist")
}

func TestMultipleOutputs(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "test.log")

	logger, err := NewLogger(&Config{
		Level:   LogLevelDebug,
		Format:  LogFormatJSON,
		Outputs: []string{LogOutputStdout, LogOutputFile},
		File: FileConfig{
			Path:       logFile,
			MaxSize:    1,
			MaxAge:     7,
			MaxBackups: 3,
			Compress:   false,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, logger)

	logger.Info("test message to multiple outputs", Fields{"output": "both"})

	_, err = os.Stat(logFile)
	assert.NoError(t, err, "log file should exist")
}

func TestTextFormat(t *testing.T) {
	logger, err := NewLogger(&Config{
		Level:   LogLevelInfo,
		Format:  LogFormatText,
		Outputs: []string{LogOutputStderr},
	})
	require.NoError(t, err)
	require.NotNil(t, logger)

	logger.Info("text format test", Fields{"format": "text"})
}

func TestFileOutputWithRotation(t *testing.T) {
	tmpDir := t.TempDir()
	logFile := filepath.Join(tmpDir, "rotated.log")

	logger, err := NewLogger(&Config{
		Level:   LogLevelInfo,
		Format:  LogFormatJSON,
		Outputs: []string{LogOutputFile},
		File: FileConfig{
			Path:       logFile,
			MaxSize:    10,
			MaxAge:     30,
			MaxBackups: 5,
			Compress:   true,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, logger)

	logger.Info("rotation test", Fields{"compressed": true})

	_, err = os.Stat(logFile)
	assert.NoError(t, err, "log file should exist")
}
