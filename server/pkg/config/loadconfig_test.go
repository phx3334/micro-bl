package config

import "testing"

func TestExpandEnvPlaceholders(t *testing.T) {
	t.Setenv("FOO", "bar")

	cases := []struct {
		name string
		in   string
		want string
	}{
		{name: "plain", in: "plain", want: "plain"},
		{name: "set", in: "${FOO}", want: "bar"},
		{name: "inline", in: "prefix-${FOO}-suffix", want: "prefix-bar-suffix"},
		{name: "default when missing", in: "${MISSING:-def}", want: "def"},
		{name: "empty when missing", in: "${MISSING}", want: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := expandEnvPlaceholders(tc.in); got != tc.want {
				t.Fatalf("expandEnvPlaceholders(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestValidateConfig(t *testing.T) {
	valid := &Config{
		Server:   ServerConfig{Port: 8081, GRPCPort: 9081},
		Database: DatabaseConfig{Host: "mysql.middleware"},
		Redis:    RedisConfig{Host: "redis.middleware"},
		Kafka:    KafkaConfig{Brokers: []string{"kafka.middleware:9092"}},
	}
	if err := validateConfig(valid); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	cases := []struct {
		name string
		mut  func(*Config)
	}{
		{name: "http port", mut: func(c *Config) { c.Server.Port = 0 }},
		{name: "grpc port", mut: func(c *Config) { c.Server.GRPCPort = 0 }},
		{name: "database host", mut: func(c *Config) { c.Database.Host = "" }},
		{name: "redis host", mut: func(c *Config) { c.Redis.Host = "" }},
		{name: "kafka brokers", mut: func(c *Config) { c.Kafka.Brokers = nil }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := *valid
			tc.mut(&cfg)
			if err := validateConfig(&cfg); err == nil {
				t.Fatalf("invalid config accepted: %s", tc.name)
			}
		})
	}
}
