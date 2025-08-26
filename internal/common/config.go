package common

import (
	"dexc_conf/internal/models"
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Port string `json:"port"`

	Windows []models.Window `json:"windows"`

	BucketStep time.Duration `json:"bucket_step"`

	// Deduplication
	DedupTTL time.Duration `json:"dedup_ttl"`

	// Out-of-order handling
	ReorderDelay time.Duration `json:"reorder_delay"`

	// Snapshot
	SnapshotPath string `json:"snapshot_path"`

	// Logging
	SnapshotPeriod time.Duration `json:"snapshot_period"`

	// WebSocket
	WSWriteBuffer int `json:"ws_write_buffer"`

	// Logging
	LogLevel string `json:"log_level"`

	// Demo data generator
	EventsPerSecond int `json:"events_per_second"`
	TokenCount      int `json:"token_count"`

	BackpressureStrategy   string `json:"backpressure_strategy"`    // block, drop, buffer
	BackpressureBufferSize int    `json:"backpressure_buffer_size"` // размер внутреннего буфера
	MaxTimestampSkew time.Duration `json:"max_timestamp_skew"` // максимальное отклонение timestamp в будущее
}

func LoadConfig() (*Config, error) {
	config := &Config{
		Port:                   getEnvWithDefault("PORT", "8080"),
		BucketStep:             getEnvDurationWithDefault("BUCKET_STEP", "5s"),
		DedupTTL:               getEnvDurationWithDefault("DEDUP_TTL", "24h"),
		ReorderDelay:           getEnvDurationWithDefault("REORDER_DELAY", "3s"),
		SnapshotPath:           getEnvWithDefault("SNAPSHOT_PATH", "/tmp/rt_stats_snapshot.json"),
		SnapshotPeriod:         getEnvDurationWithDefault("SNAPSHOT_PERIOD", "5s"),
		WSWriteBuffer:          getEnvIntWithDefault("WS_WRITE_BUFFER", 65536),
		LogLevel:               getEnvWithDefault("LOG_LEVEL", "info"),
		EventsPerSecond:        getEnvIntWithDefault("EVENTS_PER_SECOND", 1000),
		TokenCount:             getEnvIntWithDefault("TOKEN_COUNT", 10),
		BackpressureStrategy:   getEnvWithDefault("BACKPRESSURE_STRATEGY", "block"),
		BackpressureBufferSize: getEnvIntWithDefault("BACKPRESSURE_BUFFER_SIZE", 1000),
		MaxTimestampSkew:       getEnvDurationWithDefault("MAX_TIMESTAMP_SKEW", "5m"),
	}

	windowsEnv := getEnvWithDefault("WINDOWS", "5m,1h,24h")
	config.Windows = models.ParseWindows(windowsEnv)

	return config, nil
}

func (c *Config) Validate() error {
	if c.Port == "" {
		return fmt.Errorf("port cannot be empty")
	}

	if c.BucketStep <= 0 {
		return fmt.Errorf("bucket step must be positive")
	}

	if c.DedupTTL <= 0 {
		return fmt.Errorf("dedup TTL must be positive")
	}

	if c.ReorderDelay < 0 {
		return fmt.Errorf("reorder delay cannot be negative")
	}

	if c.SnapshotPeriod <= 0 {
		return fmt.Errorf("snapshot period must be positive")
	}

	if c.WSWriteBuffer <= 0 {
		return fmt.Errorf("websocket write buffer must be positive")
	}

	if len(c.Windows) == 0 {
		return fmt.Errorf("at least one window must be configured")
	}

	for _, window := range c.Windows {
		if err := window.Validate(); err != nil {
			return fmt.Errorf("invalid window %s: %w", window, err)
		}
	}

	return nil
}

func getEnvWithDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvDurationWithDefault(key, defaultValue string) time.Duration {
	value := getEnvWithDefault(key, defaultValue)
	duration, err := time.ParseDuration(value)
	if err != nil {
		defaultDuration, _ := time.ParseDuration(defaultValue)
		return defaultDuration
	}
	return duration
}

func getEnvIntWithDefault(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	intValue, err := strconv.Atoi(value)
	if err != nil {
		return defaultValue
	}

	return intValue
}
