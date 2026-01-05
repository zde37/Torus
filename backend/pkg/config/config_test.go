package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	assert.NotNil(t, cfg)
	assert.Equal(t, 160, cfg.M)
	assert.NoError(t, cfg.Validate())
}

func TestConfigValidation(t *testing.T) {
	tests := []struct {
		name    string
		config  *Config
		wantErr bool
	}{
		{
			name:    "valid config",
			config:  DefaultConfig(),
			wantErr: false,
		},
		{
			name: "invalid M (too large)",
			config: &Config{
				M:        300,
				Port:     8440,
				HTTPPort: 8080,
			},
			wantErr: true,
		},
		{
			name: "invalid M (too small)",
			config: &Config{
				M:        0,
				Port:     8440,
				HTTPPort: 8080,
			},
			wantErr: true,
		},
		{
			name: "invalid port (negative)",
			config: &Config{
				M:        160,
				Port:     -1,
				HTTPPort: 8080,
			},
			wantErr: true,
		},
		{
			name: "invalid port (too large)",
			config: &Config{
				M:        160,
				Port:     70000,
				HTTPPort: 8080,
			},
			wantErr: true,
		},
		{
			name: "invalid HTTP port",
			config: &Config{
				M:        160,
				Port:     8440,
				HTTPPort: -1,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfigFields(t *testing.T) {
	cfg := DefaultConfig()

	// Test default values
	assert.Equal(t, "127.0.0.1", cfg.Host)
	assert.Equal(t, 8440, cfg.Port)
	assert.Equal(t, 8080, cfg.HTTPPort)
	assert.Equal(t, 160, cfg.M)
	assert.Equal(t, 8, cfg.SuccessorListSize)
	assert.Equal(t, "info", cfg.LogLevel)
	assert.Equal(t, "console", cfg.LogFormat)
}
