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
)

type Config struct {
	App          AppConfig          `yaml:"app"`
	HTTP         HTTPConfig         `yaml:"http"`
	Mongo        MongoConfig        `yaml:"mongo"`
	Logging      LoggingConfig      `yaml:"logging"`
	Cache        CacheConfig        `yaml:"cache"`
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
	QueryTTL       time.Duration `yaml:"query_ttl"`
	ServiceListTTL time.Duration `yaml:"service_list_ttl"`
	TagValuesTTL   time.Duration `yaml:"tag_values_ttl"`
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
			WriteTimeout:    15 * time.Second,
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
			QueryTTL:       5 * time.Minute,
			ServiceListTTL: 30 * time.Minute,
			TagValuesTTL:   30 * time.Minute,
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

	if err := cfg.Validate(); err != nil {
		return Config{}, "", err
	}

	return cfg, resolved, nil
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

func (c Config) Validate() error {
	if strings.TrimSpace(c.App.Name) == "" {
		return fmt.Errorf("app.name is required")
	}
	if strings.TrimSpace(c.HTTP.Addr) == "" {
		return fmt.Errorf("http.addr is required")
	}
	if c.HTTP.ReadTimeout <= 0 || c.HTTP.WriteTimeout <= 0 || c.HTTP.IdleTimeout <= 0 || c.HTTP.ShutdownTimeout <= 0 {
		return fmt.Errorf("http timeouts must be positive")
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
