package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAppliesEnvOverrides(t *testing.T) {
	t.Setenv("VILOG_HTTP_ADDR", ":9090")
	t.Setenv("VILOG_MONGO_DATABASE", "vilog_override")
	t.Setenv("VILOG_SECURITY_WHITELIST", "127.0.0.1,10.0.0.0/24")

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	content := []byte(`
app:
  name: vilog-victorialogs
http:
  addr: ":8080"
mongo:
  uri: mongodb://localhost:27017
  database: vilog_victorialogs
logging:
  level: info
`)

	if err := os.WriteFile(configPath, content, 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, resolved, err := Load(configPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if resolved != configPath {
		t.Fatalf("resolved config path = %q, want %q", resolved, configPath)
	}

	if cfg.HTTP.Addr != ":9090" {
		t.Fatalf("http.addr = %q, want :9090", cfg.HTTP.Addr)
	}

	if cfg.Mongo.Database != "vilog_override" {
		t.Fatalf("mongo.database = %q, want vilog_override", cfg.Mongo.Database)
	}

	if len(cfg.Security.Whitelist) != 2 {
		t.Fatalf("whitelist length = %d, want 2", len(cfg.Security.Whitelist))
	}
}

func TestValidateRejectsInvalidWhitelist(t *testing.T) {
	cfg := Default()
	cfg.Security.Whitelist = []string{"not-an-ip"}

	if err := cfg.Validate(); err == nil {
		t.Fatal("expected whitelist validation error")
	}
}
