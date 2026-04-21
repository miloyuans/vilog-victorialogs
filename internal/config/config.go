package config

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"vilog-victorialogs/internal/model"
)

type Config struct {
	App          AppConfig          `yaml:"app"`
	HTTP         HTTPConfig         `yaml:"http"`
	Mongo        MongoConfig        `yaml:"mongo"`
	Logging      LoggingConfig      `yaml:"logging"`
	Cache        CacheConfig        `yaml:"cache"`
	QueryJobs    QueryJobsConfig    `yaml:"query_jobs"`
	Datasources  []ConfiguredDatasource `yaml:"datasources"`
	VictoriaLogs VictoriaLogsConfig `yaml:"victorialogs"`
	Security     SecurityConfig     `yaml:"security"`
	Telegram     TelegramConfig     `yaml:"telegram"`
	Discovery    DiscoveryConfig    `yaml:"discovery"`
	Retention    RetentionConfig    `yaml:"retention"`
}

type AppConfig struct {
	Name        string `yaml:"name"`
	Environment string `yaml:"environment"`
}

type HTTPConfig struct {
	Addr            string        `yaml:"addr"`
	ReadTimeout     time.Duration `yaml:"read_timeout"`
	WriteTimeout    time.Duration `yaml:"write_timeout"`
	IdleTimeout     time.Duration `yaml:"idle_timeout"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
	TrustedProxies  []string      `yaml:"trusted_proxies"`
}

type MongoConfig struct {
	URI            string        `yaml:"uri"`
	Database       string        `yaml:"database"`
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
	PingTimeout    time.Duration `yaml:"ping_timeout"`
}

type LoggingConfig struct {
	Level       string `yaml:"level"`
	Development bool   `yaml:"development"`
}

type CacheConfig struct {
	QueryTTL                time.Duration `yaml:"query_ttl"`
	ServiceListTTL          time.Duration `yaml:"service_list_ttl"`
	TagValuesTTL            time.Duration `yaml:"tag_values_ttl"`
	BackgroundSyncEnabled   bool          `yaml:"background_sync_enabled"`
	LocalQueryDir           string        `yaml:"local_query_dir"`
	LocalQueryRetention     time.Duration `yaml:"local_query_retention"`
	LocalLogDir             string        `yaml:"local_log_dir"`
	LocalLogHotDays         int           `yaml:"local_log_hot_days"`
	LocalLogRefreshInterval time.Duration `yaml:"local_log_refresh_interval"`
	LocalLogDailyCheckAt    string        `yaml:"local_log_daily_check_at"`
	LocalLogCheckConcurrency int          `yaml:"local_log_check_concurrency"`
	InteractiveSyncConcurrency int        `yaml:"interactive_sync_concurrency"`
	MaintenanceSyncConcurrency int        `yaml:"maintenance_sync_concurrency"`
	ServiceChunkConcurrency   int         `yaml:"service_chunk_concurrency"`
	InteractiveServiceTTL     time.Duration `yaml:"interactive_service_ttl"`
	LocalLogHistoryTTL      time.Duration `yaml:"local_log_history_ttl"`
	SourceChunkSize         int           `yaml:"source_chunk_size"`
	SourceRequestLimit      int           `yaml:"source_request_limit"`
	DenseWindowLimit        int           `yaml:"dense_window_limit"`
	MaxPartitionRows        int           `yaml:"max_partition_rows"`
	MaxRowsBeforePartial    int           `yaml:"max_rows_before_partial"`
	MaxDedupeRows           int           `yaml:"max_dedupe_rows"`
	MaxSortRows             int           `yaml:"max_sort_rows"`
	MaxPendingPartitionSyncs int          `yaml:"max_pending_partition_syncs"`
	MaxQueryWindow          int           `yaml:"max_query_window"`
}

type QueryJobsConfig struct {
	Enabled                    bool          `yaml:"enabled"`
	BaseDir                    string        `yaml:"base_dir"`
	TTLHours                   int           `yaml:"ttl_hours"`
	SegmentMaxRows             int           `yaml:"segment_max_rows"`
	SegmentMaxBytes            int           `yaml:"segment_max_bytes"`
	MaxConcurrentJobs          int           `yaml:"max_concurrent_jobs"`
	MaxConcurrentSourcesPerJob int           `yaml:"max_concurrent_sources_per_job"`
	SSEHeartbeatSeconds        int           `yaml:"sse_heartbeat_seconds"`
	ChunkWindow                time.Duration `yaml:"chunk_window"`
}

type ConfiguredDatasource struct {
	ID             string                     `yaml:"id"`
	Name           string                     `yaml:"name"`
	BaseURL        string                     `yaml:"base_url"`
	Enabled        bool                       `yaml:"enabled"`
	TimeoutSeconds int                        `yaml:"timeout_seconds"`
	Headers        model.DatasourceHeaders    `yaml:"headers"`
	QueryPaths     model.DatasourceQueryPaths `yaml:"query_paths"`
	FieldMapping   model.DatasourceFieldMapping `yaml:"field_mapping"`
	SupportsDelete bool                       `yaml:"supports_delete"`
}

type VictoriaLogsConfig struct {
	RequestRetries int `yaml:"request_retries"`
	DiscoveryLimit int `yaml:"discovery_limit"`
	TagValueLimit  int `yaml:"tag_value_limit"`
}

type SecurityConfig struct {
	TrustProxyHeaders bool     `yaml:"trust_proxy_headers"`
	Whitelist         []string `yaml:"whitelist"`
	RateLimitRPS      float64  `yaml:"rate_limit_rps"`
	RateLimitBurst    int      `yaml:"rate_limit_burst"`
}

type TelegramConfig struct {
	Enabled     bool          `yaml:"enabled"`
	BotToken    string        `yaml:"bot_token"`
	ChatID      string        `yaml:"chat_id"`
	APIBaseURL  string        `yaml:"api_base_url"`
	SendTimeout time.Duration `yaml:"send_timeout"`
}

type DiscoveryConfig struct {
	StartupEnabled bool          `yaml:"startup_enabled"`
	Window         time.Duration `yaml:"window"`
	Concurrency    int           `yaml:"concurrency"`
}

type RetentionConfig struct {
	SchedulerEnabled     bool          `yaml:"scheduler_enabled"`
	MaxDeleteTasksPerDay int           `yaml:"max_delete_tasks_per_day"`
	MaxDeleteRangeDays   int           `yaml:"max_delete_range_days"`
	DeleteTimeout        time.Duration `yaml:"delete_timeout"`
	PollInterval         time.Duration `yaml:"poll_interval"`
}

func Default() Config {
	return Config{
		App: AppConfig{
			Name:        "vilog-victorialogs",
			Environment: "development",
		},
		HTTP: HTTPConfig{
			Addr:            ":8080",
			ReadTimeout:     10 * time.Second,
			WriteTimeout:    60 * time.Second,
			IdleTimeout:     60 * time.Second,
			ShutdownTimeout: 15 * time.Second,
			TrustedProxies:  []string{},
		},
		Mongo: MongoConfig{
			URI:            "mongodb://localhost:27017",
			Database:       "vilog_victorialogs",
			ConnectTimeout: 10 * time.Second,
			PingTimeout:    3 * time.Second,
		},
		Logging: LoggingConfig{
			Level:       "info",
			Development: true,
		},
		Cache: CacheConfig{
			QueryTTL:                5 * time.Minute,
			ServiceListTTL:          30 * time.Minute,
			TagValuesTTL:            30 * time.Minute,
			BackgroundSyncEnabled:   false,
			LocalQueryDir:           "./data/query-cache",
			LocalQueryRetention:     24 * time.Hour,
			LocalLogDir:             "./data/log-cache",
			LocalLogHotDays:         2,
			LocalLogRefreshInterval: 3 * time.Minute,
			LocalLogDailyCheckAt:    "00:05",
			LocalLogCheckConcurrency: 1,
			InteractiveSyncConcurrency: 1,
			MaintenanceSyncConcurrency: 1,
			ServiceChunkConcurrency:   1,
			InteractiveServiceTTL:     10 * time.Minute,
			LocalLogHistoryTTL:      1 * time.Hour,
			SourceChunkSize:         1000,
			SourceRequestLimit:      2000,
			DenseWindowLimit:        5000,
			MaxPartitionRows:        10000,
			MaxRowsBeforePartial:    8000,
			MaxDedupeRows:           12000,
			MaxSortRows:             12000,
			MaxPendingPartitionSyncs: 256,
			MaxQueryWindow:          100000,
		},
		QueryJobs: QueryJobsConfig{
			Enabled:                    true,
			BaseDir:                    "./data/query-cache/jobs",
			TTLHours:                   24,
			SegmentMaxRows:             2000,
			SegmentMaxBytes:            4 << 20,
			MaxConcurrentJobs:          4,
			MaxConcurrentSourcesPerJob: 4,
			SSEHeartbeatSeconds:        10,
			ChunkWindow:                15 * time.Minute,
		},
		VictoriaLogs: VictoriaLogsConfig{
			RequestRetries: 1,
			DiscoveryLimit: 200,
			TagValueLimit:  200,
		},
		Security: SecurityConfig{
			TrustProxyHeaders: false,
			Whitelist:         []string{},
			RateLimitRPS:      0,
			RateLimitBurst:    0,
		},
		Telegram: TelegramConfig{
			Enabled:     false,
			APIBaseURL:  "https://api.telegram.org",
			SendTimeout: 5 * time.Second,
		},
		Discovery: DiscoveryConfig{
			StartupEnabled: true,
			Window:         24 * time.Hour,
			Concurrency:    4,
		},
		Retention: RetentionConfig{
			SchedulerEnabled:     false,
			MaxDeleteTasksPerDay: 1,
			MaxDeleteRangeDays:   30,
			DeleteTimeout:        10 * time.Second,
			PollInterval:         30 * time.Second,
		},
	}
}

func Load(path string) (Config, string, error) {
	resolved, err := resolvePath(path)
	if err != nil {
		return Config{}, "", err
	}

	cfg := Default()
	data, err := os.ReadFile(resolved)
	if err != nil {
		return Config{}, "", fmt.Errorf("read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, "", fmt.Errorf("parse config file: %w", err)
	}

	if err := applyEnvOverrides(&cfg); err != nil {
		return Config{}, "", err
	}
	if err := resolveLocalPaths(&cfg, resolved); err != nil {
		return Config{}, "", err
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, "", err
	}

	return cfg, resolved, nil
}

func resolveLocalPaths(cfg *Config, configPath string) error {
	baseDir := filepath.Dir(configPath)
	if strings.TrimSpace(cfg.Cache.LocalQueryDir) != "" && !filepath.IsAbs(cfg.Cache.LocalQueryDir) {
		cfg.Cache.LocalQueryDir = filepath.Clean(filepath.Join(baseDir, cfg.Cache.LocalQueryDir))
	}
	if strings.TrimSpace(cfg.Cache.LocalLogDir) != "" && !filepath.IsAbs(cfg.Cache.LocalLogDir) {
		cfg.Cache.LocalLogDir = filepath.Clean(filepath.Join(baseDir, cfg.Cache.LocalLogDir))
	}
	if strings.TrimSpace(cfg.QueryJobs.BaseDir) == "" {
		if strings.TrimSpace(cfg.Cache.LocalQueryDir) != "" {
			cfg.QueryJobs.BaseDir = filepath.Clean(filepath.Join(cfg.Cache.LocalQueryDir, "jobs"))
		}
	} else if !filepath.IsAbs(cfg.QueryJobs.BaseDir) {
		cfg.QueryJobs.BaseDir = filepath.Clean(filepath.Join(baseDir, cfg.QueryJobs.BaseDir))
	}
	return nil
}

func resolvePath(path string) (string, error) {
	candidates := []string{}
	if strings.TrimSpace(path) != "" {
		candidates = append(candidates, path)
	}

	if path == "" || filepath.Base(path) == "config.yaml" {
		candidates = append(candidates, "config.example.yaml")
	}

	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err == nil && !info.IsDir() {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("config file not found, checked: %s", strings.Join(candidates, ", "))
}

func (c *Config) Validate() error {
	if strings.TrimSpace(c.App.Name) == "" {
		return fmt.Errorf("app.name is required")
	}
	if strings.TrimSpace(c.HTTP.Addr) == "" {
		return fmt.Errorf("http.addr is required")
	}
	if c.HTTP.ReadTimeout <= 0 || c.HTTP.WriteTimeout <= 0 || c.HTTP.IdleTimeout <= 0 || c.HTTP.ShutdownTimeout <= 0 {
		return fmt.Errorf("http timeouts must be positive")
	}
	if c.HTTP.WriteTimeout < 60*time.Second {
		c.HTTP.WriteTimeout = 60 * time.Second
	}
	if strings.TrimSpace(c.Mongo.URI) == "" || strings.TrimSpace(c.Mongo.Database) == "" {
		return fmt.Errorf("mongo.uri and mongo.database are required")
	}
	if c.Mongo.ConnectTimeout <= 0 || c.Mongo.PingTimeout <= 0 {
		return fmt.Errorf("mongo timeouts must be positive")
	}
	if c.Cache.QueryTTL <= 0 || c.Cache.ServiceListTTL <= 0 || c.Cache.TagValuesTTL <= 0 {
		return fmt.Errorf("cache ttls must be positive")
	}
	if c.Cache.LocalQueryRetention <= 0 {
		return fmt.Errorf("cache.local_query_retention must be positive")
	}
	if c.Cache.LocalLogHotDays <= 0 {
		return fmt.Errorf("cache.local_log_hot_days must be positive")
	}
	if c.Cache.LocalLogRefreshInterval < 0 {
		return fmt.Errorf("cache.local_log_refresh_interval must be zero or positive")
	}
	if strings.TrimSpace(c.Cache.LocalLogDailyCheckAt) != "" {
		if _, err := time.Parse("15:04", strings.TrimSpace(c.Cache.LocalLogDailyCheckAt)); err != nil {
			return fmt.Errorf("cache.local_log_daily_check_at must use HH:MM format")
		}
	}
	if c.Cache.LocalLogCheckConcurrency <= 0 {
		c.Cache.LocalLogCheckConcurrency = 1
	}
	if c.Cache.InteractiveSyncConcurrency <= 0 {
		c.Cache.InteractiveSyncConcurrency = 1
	}
	if c.Cache.MaintenanceSyncConcurrency <= 0 {
		c.Cache.MaintenanceSyncConcurrency = 1
	}
	if c.Cache.ServiceChunkConcurrency <= 0 {
		c.Cache.ServiceChunkConcurrency = 1
	}
	if c.Cache.InteractiveServiceTTL <= 0 {
		c.Cache.InteractiveServiceTTL = 10 * time.Minute
	}
	if c.Cache.LocalLogHistoryTTL <= 0 {
		return fmt.Errorf("cache.local_log_history_ttl must be positive")
	}
	if c.Cache.SourceChunkSize <= 0 {
		c.Cache.SourceChunkSize = 1000
	}
	if c.Cache.SourceRequestLimit <= 0 {
		c.Cache.SourceRequestLimit = 2000
	}
	if c.Cache.DenseWindowLimit <= 0 {
		c.Cache.DenseWindowLimit = 5000
	}
	if c.Cache.MaxPartitionRows <= 0 {
		c.Cache.MaxPartitionRows = 10000
	}
	if c.Cache.MaxRowsBeforePartial <= 0 {
		c.Cache.MaxRowsBeforePartial = minInt(c.Cache.MaxPartitionRows, 8000)
	}
	if c.Cache.MaxDedupeRows <= 0 {
		c.Cache.MaxDedupeRows = maxInt(c.Cache.MaxPartitionRows, 12000)
	}
	if c.Cache.MaxSortRows <= 0 {
		c.Cache.MaxSortRows = maxInt(c.Cache.MaxRowsBeforePartial, 12000)
	}
	if c.Cache.MaxPendingPartitionSyncs <= 0 {
		c.Cache.MaxPendingPartitionSyncs = 256
	}
	if c.Cache.MaxQueryWindow <= 0 {
		c.Cache.MaxQueryWindow = 100000
	}
	if c.Cache.MaxRowsBeforePartial > c.Cache.MaxPartitionRows {
		c.Cache.MaxRowsBeforePartial = c.Cache.MaxPartitionRows
	}
	if c.Cache.DenseWindowLimit > c.Cache.MaxPartitionRows {
		c.Cache.DenseWindowLimit = c.Cache.MaxPartitionRows
	}
	if c.Cache.SourceRequestLimit > c.Cache.MaxPartitionRows {
		c.Cache.SourceRequestLimit = c.Cache.MaxPartitionRows
	}
	if c.Cache.MaxDedupeRows < c.Cache.MaxPartitionRows {
		c.Cache.MaxDedupeRows = c.Cache.MaxPartitionRows
	}
	if c.Cache.MaxSortRows < c.Cache.MaxRowsBeforePartial {
		c.Cache.MaxSortRows = c.Cache.MaxRowsBeforePartial
	}
	if strings.TrimSpace(c.QueryJobs.BaseDir) == "" {
		if strings.TrimSpace(c.Cache.LocalQueryDir) != "" {
			c.QueryJobs.BaseDir = filepath.Clean(filepath.Join(c.Cache.LocalQueryDir, "jobs"))
		} else {
			c.QueryJobs.BaseDir = "./data/query-cache/jobs"
		}
	}
	if c.QueryJobs.TTLHours <= 0 {
		c.QueryJobs.TTLHours = 24
	}
	if c.QueryJobs.SegmentMaxRows <= 0 {
		c.QueryJobs.SegmentMaxRows = 2000
	}
	if c.QueryJobs.SegmentMaxBytes <= 0 {
		c.QueryJobs.SegmentMaxBytes = 4 << 20
	}
	if c.QueryJobs.MaxConcurrentJobs <= 0 {
		c.QueryJobs.MaxConcurrentJobs = 4
	}
	if c.QueryJobs.MaxConcurrentSourcesPerJob <= 0 {
		c.QueryJobs.MaxConcurrentSourcesPerJob = 4
	}
	if c.QueryJobs.SSEHeartbeatSeconds <= 0 {
		c.QueryJobs.SSEHeartbeatSeconds = 10
	}
	if c.QueryJobs.ChunkWindow <= 0 {
		c.QueryJobs.ChunkWindow = 15 * time.Minute
	}
	if c.VictoriaLogs.RequestRetries < 0 || c.VictoriaLogs.DiscoveryLimit <= 0 || c.VictoriaLogs.TagValueLimit <= 0 {
		return fmt.Errorf("victorialogs settings must be positive")
	}

	switch strings.ToLower(c.Logging.Level) {
	case "debug", "info", "warn", "error", "dpanic", "panic", "fatal":
	default:
		return fmt.Errorf("logging.level must be one of debug, info, warn, error, dpanic, panic, fatal")
	}

	if c.Discovery.Concurrency <= 0 || c.Discovery.Window <= 0 {
		return fmt.Errorf("discovery settings must be positive")
	}
	if c.Retention.MaxDeleteTasksPerDay <= 0 || c.Retention.MaxDeleteRangeDays <= 0 {
		return fmt.Errorf("retention safety limits must be positive")
	}
	if c.Retention.DeleteTimeout <= 0 || c.Retention.PollInterval <= 0 {
		return fmt.Errorf("retention timeouts must be positive")
	}
	if c.Telegram.Enabled && (strings.TrimSpace(c.Telegram.BotToken) == "" || strings.TrimSpace(c.Telegram.ChatID) == "") {
		return fmt.Errorf("telegram bot_token and chat_id are required when telegram.enabled=true")
	}

	for _, entry := range c.Security.Whitelist {
		if err := validateCIDROrIP(entry); err != nil {
			return fmt.Errorf("security.whitelist: %w", err)
		}
	}
	if c.Security.RateLimitRPS < 0 || c.Security.RateLimitBurst < 0 {
		return fmt.Errorf("security rate limit must be zero or positive")
	}

	return nil
}

func applyEnvOverrides(cfg *Config) error {
	setString("VILOG_APP_NAME", &cfg.App.Name)
	setString("VILOG_APP_ENVIRONMENT", &cfg.App.Environment)

	setString("VILOG_HTTP_ADDR", &cfg.HTTP.Addr)
	if err := setDuration("VILOG_HTTP_READ_TIMEOUT", &cfg.HTTP.ReadTimeout); err != nil {
		return err
	}
	if err := setDuration("VILOG_HTTP_WRITE_TIMEOUT", &cfg.HTTP.WriteTimeout); err != nil {
		return err
	}
	if err := setDuration("VILOG_HTTP_IDLE_TIMEOUT", &cfg.HTTP.IdleTimeout); err != nil {
		return err
	}
	if err := setDuration("VILOG_HTTP_SHUTDOWN_TIMEOUT", &cfg.HTTP.ShutdownTimeout); err != nil {
		return err
	}
	setStringSlice("VILOG_HTTP_TRUSTED_PROXIES", &cfg.HTTP.TrustedProxies)

	setString("VILOG_MONGO_URI", &cfg.Mongo.URI)
	setString("VILOG_MONGO_DATABASE", &cfg.Mongo.Database)
	if err := setDuration("VILOG_MONGO_CONNECT_TIMEOUT", &cfg.Mongo.ConnectTimeout); err != nil {
		return err
	}
	if err := setDuration("VILOG_MONGO_PING_TIMEOUT", &cfg.Mongo.PingTimeout); err != nil {
		return err
	}

	setString("VILOG_LOG_LEVEL", &cfg.Logging.Level)
	if err := setBool("VILOG_LOG_DEVELOPMENT", &cfg.Logging.Development); err != nil {
		return err
	}

	if err := setDuration("VILOG_CACHE_QUERY_TTL", &cfg.Cache.QueryTTL); err != nil {
		return err
	}
	if err := setDuration("VILOG_CACHE_SERVICE_LIST_TTL", &cfg.Cache.ServiceListTTL); err != nil {
		return err
	}
	if err := setDuration("VILOG_CACHE_TAG_VALUES_TTL", &cfg.Cache.TagValuesTTL); err != nil {
		return err
	}
	if err := setBool("VILOG_CACHE_BACKGROUND_SYNC_ENABLED", &cfg.Cache.BackgroundSyncEnabled); err != nil {
		return err
	}
	setString("VILOG_CACHE_LOCAL_QUERY_DIR", &cfg.Cache.LocalQueryDir)
	if err := setDuration("VILOG_CACHE_LOCAL_QUERY_RETENTION", &cfg.Cache.LocalQueryRetention); err != nil {
		return err
	}
	setString("VILOG_CACHE_LOCAL_LOG_DIR", &cfg.Cache.LocalLogDir)
	if err := setInt("VILOG_CACHE_LOCAL_LOG_HOT_DAYS", &cfg.Cache.LocalLogHotDays); err != nil {
		return err
	}
	if err := setDuration("VILOG_CACHE_LOCAL_LOG_REFRESH_INTERVAL", &cfg.Cache.LocalLogRefreshInterval); err != nil {
		return err
	}
	setString("VILOG_CACHE_LOCAL_LOG_DAILY_CHECK_AT", &cfg.Cache.LocalLogDailyCheckAt)
	if err := setInt("VILOG_CACHE_LOCAL_LOG_CHECK_CONCURRENCY", &cfg.Cache.LocalLogCheckConcurrency); err != nil {
		return err
	}
	if err := setInt("VILOG_CACHE_INTERACTIVE_SYNC_CONCURRENCY", &cfg.Cache.InteractiveSyncConcurrency); err != nil {
		return err
	}
	if err := setInt("VILOG_CACHE_MAINTENANCE_SYNC_CONCURRENCY", &cfg.Cache.MaintenanceSyncConcurrency); err != nil {
		return err
	}
	if err := setInt("VILOG_CACHE_SERVICE_CHUNK_CONCURRENCY", &cfg.Cache.ServiceChunkConcurrency); err != nil {
		return err
	}
	if err := setDuration("VILOG_CACHE_INTERACTIVE_SERVICE_TTL", &cfg.Cache.InteractiveServiceTTL); err != nil {
		return err
	}
	if err := setDuration("VILOG_CACHE_LOCAL_LOG_HISTORY_TTL", &cfg.Cache.LocalLogHistoryTTL); err != nil {
		return err
	}
	if err := setInt("VILOG_CACHE_SOURCE_CHUNK_SIZE", &cfg.Cache.SourceChunkSize); err != nil {
		return err
	}
	if err := setInt("VILOG_CACHE_SOURCE_REQUEST_LIMIT", &cfg.Cache.SourceRequestLimit); err != nil {
		return err
	}
	if err := setInt("VILOG_CACHE_DENSE_WINDOW_LIMIT", &cfg.Cache.DenseWindowLimit); err != nil {
		return err
	}
	if err := setInt("VILOG_CACHE_MAX_PARTITION_ROWS", &cfg.Cache.MaxPartitionRows); err != nil {
		return err
	}
	if err := setInt("VILOG_CACHE_MAX_ROWS_BEFORE_PARTIAL", &cfg.Cache.MaxRowsBeforePartial); err != nil {
		return err
	}
	if err := setInt("VILOG_CACHE_MAX_DEDUPE_ROWS", &cfg.Cache.MaxDedupeRows); err != nil {
		return err
	}
	if err := setInt("VILOG_CACHE_MAX_SORT_ROWS", &cfg.Cache.MaxSortRows); err != nil {
		return err
	}
	if err := setInt("VILOG_CACHE_MAX_PENDING_PARTITION_SYNCS", &cfg.Cache.MaxPendingPartitionSyncs); err != nil {
		return err
	}
	if err := setInt("VILOG_CACHE_MAX_QUERY_WINDOW", &cfg.Cache.MaxQueryWindow); err != nil {
		return err
	}
	if err := setBool("VILOG_QUERY_JOBS_ENABLED", &cfg.QueryJobs.Enabled); err != nil {
		return err
	}
	setString("VILOG_QUERY_JOBS_BASE_DIR", &cfg.QueryJobs.BaseDir)
	if err := setInt("VILOG_QUERY_JOBS_TTL_HOURS", &cfg.QueryJobs.TTLHours); err != nil {
		return err
	}
	if err := setInt("VILOG_QUERY_JOBS_SEGMENT_MAX_ROWS", &cfg.QueryJobs.SegmentMaxRows); err != nil {
		return err
	}
	if err := setInt("VILOG_QUERY_JOBS_SEGMENT_MAX_BYTES", &cfg.QueryJobs.SegmentMaxBytes); err != nil {
		return err
	}
	if err := setInt("VILOG_QUERY_JOBS_MAX_CONCURRENT_JOBS", &cfg.QueryJobs.MaxConcurrentJobs); err != nil {
		return err
	}
	if err := setInt("VILOG_QUERY_JOBS_MAX_CONCURRENT_SOURCES_PER_JOB", &cfg.QueryJobs.MaxConcurrentSourcesPerJob); err != nil {
		return err
	}
	if err := setInt("VILOG_QUERY_JOBS_SSE_HEARTBEAT_SECONDS", &cfg.QueryJobs.SSEHeartbeatSeconds); err != nil {
		return err
	}
	if err := setDuration("VILOG_QUERY_JOBS_CHUNK_WINDOW", &cfg.QueryJobs.ChunkWindow); err != nil {
		return err
	}

	if err := setInt("VILOG_VL_REQUEST_RETRIES", &cfg.VictoriaLogs.RequestRetries); err != nil {
		return err
	}
	if err := setInt("VILOG_VL_DISCOVERY_LIMIT", &cfg.VictoriaLogs.DiscoveryLimit); err != nil {
		return err
	}
	if err := setInt("VILOG_VL_TAG_VALUE_LIMIT", &cfg.VictoriaLogs.TagValueLimit); err != nil {
		return err
	}

	if err := setBool("VILOG_SECURITY_TRUST_PROXY_HEADERS", &cfg.Security.TrustProxyHeaders); err != nil {
		return err
	}
	setStringSlice("VILOG_SECURITY_WHITELIST", &cfg.Security.Whitelist)
	if err := setFloat("VILOG_SECURITY_RATE_LIMIT_RPS", &cfg.Security.RateLimitRPS); err != nil {
		return err
	}
	if err := setInt("VILOG_SECURITY_RATE_LIMIT_BURST", &cfg.Security.RateLimitBurst); err != nil {
		return err
	}

	if err := setBool("VILOG_TELEGRAM_ENABLED", &cfg.Telegram.Enabled); err != nil {
		return err
	}
	setString("VILOG_TELEGRAM_BOT_TOKEN", &cfg.Telegram.BotToken)
	setString("VILOG_TELEGRAM_CHAT_ID", &cfg.Telegram.ChatID)
	setString("VILOG_TELEGRAM_API_BASE_URL", &cfg.Telegram.APIBaseURL)
	if err := setDuration("VILOG_TELEGRAM_SEND_TIMEOUT", &cfg.Telegram.SendTimeout); err != nil {
		return err
	}

	if err := setBool("VILOG_DISCOVERY_STARTUP_ENABLED", &cfg.Discovery.StartupEnabled); err != nil {
		return err
	}
	if err := setDuration("VILOG_DISCOVERY_WINDOW", &cfg.Discovery.Window); err != nil {
		return err
	}
	if err := setInt("VILOG_DISCOVERY_CONCURRENCY", &cfg.Discovery.Concurrency); err != nil {
		return err
	}

	if err := setBool("VILOG_RETENTION_SCHEDULER_ENABLED", &cfg.Retention.SchedulerEnabled); err != nil {
		return err
	}
	if err := setInt("VILOG_RETENTION_MAX_DELETE_TASKS_PER_DAY", &cfg.Retention.MaxDeleteTasksPerDay); err != nil {
		return err
	}
	if err := setInt("VILOG_RETENTION_MAX_DELETE_RANGE_DAYS", &cfg.Retention.MaxDeleteRangeDays); err != nil {
		return err
	}
	if err := setDuration("VILOG_RETENTION_DELETE_TIMEOUT", &cfg.Retention.DeleteTimeout); err != nil {
		return err
	}
	if err := setDuration("VILOG_RETENTION_POLL_INTERVAL", &cfg.Retention.PollInterval); err != nil {
		return err
	}

	return nil
}

func setString(key string, target *string) {
	if value, ok := os.LookupEnv(key); ok {
		*target = strings.TrimSpace(value)
	}
}

func setStringSlice(key string, target *[]string) {
	if value, ok := os.LookupEnv(key); ok {
		*target = splitCSV(value)
	}
}

func setBool(key string, target *bool) error {
	value, ok := os.LookupEnv(key)
	if !ok {
		return nil
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("%s: parse bool: %w", key, err)
	}
	*target = parsed
	return nil
}

func setInt(key string, target *int) error {
	value, ok := os.LookupEnv(key)
	if !ok {
		return nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("%s: parse int: %w", key, err)
	}
	*target = parsed
	return nil
}

func setFloat(key string, target *float64) error {
	value, ok := os.LookupEnv(key)
	if !ok {
		return nil
	}
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	if err != nil {
		return fmt.Errorf("%s: parse float: %w", key, err)
	}
	*target = parsed
	return nil
}

func setDuration(key string, target *time.Duration) error {
	value, ok := os.LookupEnv(key)
	if !ok {
		return nil
	}
	parsed, err := time.ParseDuration(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("%s: parse duration: %w", key, err)
	}
	*target = parsed
	return nil
}

func splitCSV(value string) []string {
	rawItems := strings.Split(value, ",")
	items := make([]string, 0, len(rawItems))
	for _, item := range rawItems {
		trimmed := strings.TrimSpace(item)
		if trimmed != "" {
			items = append(items, trimmed)
		}
	}
	return items
}

func validateCIDROrIP(value string) error {
	if ip := net.ParseIP(strings.TrimSpace(value)); ip != nil {
		return nil
	}
	if _, _, err := net.ParseCIDR(strings.TrimSpace(value)); err == nil {
		return nil
	}
	return fmt.Errorf("invalid IP or CIDR %q", value)
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
