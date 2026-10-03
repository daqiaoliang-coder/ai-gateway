package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type File struct {
	ListenAddr            string     `json:"listen_addr"`
	RequestTimeout        string     `json:"request_timeout"`
	MaxRequestBytes       int64      `json:"max_request_bytes"`
	MaxConcurrentRequests int        `json:"max_concurrent_requests"`
	RetryMaxAttempts      int        `json:"retry_max_attempts"`
	BreakerFailures       int        `json:"breaker_failures"`
	BreakerOpenSeconds    int        `json:"breaker_open_seconds"`
	MetricsTokenEnv       string     `json:"metrics_token_env"`
	Clients               []Client   `json:"clients"`
	Providers             []Provider `json:"providers"`
	Models                []Model    `json:"models"`
}

type Client struct {
	ID            string   `json:"id"`
	KeyEnv        string   `json:"key_env"`
	AllowedModels []string `json:"allowed_models"`
	RPM           int      `json:"rpm"`
}

type Provider struct {
	Name      string `json:"name"`
	BaseURL   string `json:"base_url"`
	Model     string `json:"model"`
	APIKeyEnv string `json:"api_key_env,omitempty"`
}

type Model struct {
	Name        string   `json:"name"`
	Deployments []string `json:"deployments"`
}

func Load(path string) (File, error) {
	f, err := os.Open(path)
	if err != nil {
		return File{}, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	var cfg File
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return File{}, fmt.Errorf("decode config: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return File{}, errors.New("config must contain exactly one JSON object")
	}
	cfg.defaults()
	if err := cfg.Validate(); err != nil {
		return File{}, err
	}
	return cfg, nil
}

func (c *File) defaults() {
	if c.ListenAddr == "" {
		c.ListenAddr = ":8080"
	}
	if c.RequestTimeout == "" {
		c.RequestTimeout = "120s"
	}
	if c.MaxRequestBytes == 0 {
		c.MaxRequestBytes = 2 << 20
	}
	if c.MaxConcurrentRequests == 0 {
		c.MaxConcurrentRequests = 128
	}
	if c.RetryMaxAttempts == 0 {
		c.RetryMaxAttempts = 2
	}
	if c.BreakerFailures == 0 {
		c.BreakerFailures = 5
	}
	if c.BreakerOpenSeconds == 0 {
		c.BreakerOpenSeconds = 30
	}
}

func (c File) Validate() error {
	if c.MaxRequestBytes < 1 || c.MaxConcurrentRequests < 1 {
		return errors.New("max_request_bytes and max_concurrent_requests must be positive")
	}
	if c.RetryMaxAttempts < 1 || c.RetryMaxAttempts > 5 {
		return errors.New("retry_max_attempts must be between 1 and 5")
	}
	if c.BreakerFailures < 1 || c.BreakerOpenSeconds < 1 {
		return errors.New("breaker_failures and breaker_open_seconds must be positive")
	}
	if _, err := parsePositiveDuration(c.RequestTimeout); err != nil {
		return fmt.Errorf("request_timeout: %w", err)
	}

	providerNames := make(map[string]Provider, len(c.Providers))
	for _, p := range c.Providers {
		if p.Name == "" || p.Model == "" || p.BaseURL == "" {
			return errors.New("each provider requires name, base_url, and model")
		}
		if _, exists := providerNames[p.Name]; exists {
			return fmt.Errorf("duplicate provider %q", p.Name)
		}
		if p.APIKeyEnv != "" {
			if _, ok := os.LookupEnv(p.APIKeyEnv); !ok || os.Getenv(p.APIKeyEnv) == "" {
				return fmt.Errorf("provider %q requires non-empty environment variable %s", p.Name, p.APIKeyEnv)
			}
		}
		providerNames[p.Name] = p
	}
	if len(providerNames) == 0 {
		return errors.New("at least one provider is required")
	}

	modelNames := make(map[string]bool, len(c.Models))
	for _, m := range c.Models {
		if m.Name == "" || len(m.Deployments) == 0 {
			return errors.New("each model requires a name and at least one deployment")
		}
		if modelNames[m.Name] {
			return fmt.Errorf("duplicate model %q", m.Name)
		}
		modelNames[m.Name] = true
		for _, name := range m.Deployments {
			if _, ok := providerNames[name]; !ok {
				return fmt.Errorf("model %q references unknown provider %q", m.Name, name)
			}
		}
	}
	if len(modelNames) == 0 {
		return errors.New("at least one model is required")
	}

	if len(c.Clients) == 0 {
		return errors.New("at least one client is required")
	}
	clientIDs := make(map[string]bool, len(c.Clients))
	for _, client := range c.Clients {
		if client.ID == "" || client.KeyEnv == "" || len(client.AllowedModels) == 0 {
			return errors.New("each client requires id, key_env, and allowed_models")
		}
		if clientIDs[client.ID] {
			return fmt.Errorf("duplicate client %q", client.ID)
		}
		if client.RPM < 0 {
			return fmt.Errorf("client %q rpm cannot be negative", client.ID)
		}
		if _, ok := os.LookupEnv(client.KeyEnv); !ok || os.Getenv(client.KeyEnv) == "" {
			return fmt.Errorf("client %q requires non-empty environment variable %s", client.ID, client.KeyEnv)
		}
		for _, model := range client.AllowedModels {
			if !modelNames[model] {
				return fmt.Errorf("client %q allows unknown model %q", client.ID, model)
			}
		}
		clientIDs[client.ID] = true
	}
	if c.MetricsTokenEnv != "" {
		if _, ok := os.LookupEnv(c.MetricsTokenEnv); !ok || os.Getenv(c.MetricsTokenEnv) == "" {
			return fmt.Errorf("metrics requires non-empty environment variable %s", c.MetricsTokenEnv)
		}
	}
	return nil
}

func parsePositiveDuration(raw string) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, errors.New("must be specified")
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	if d <= 0 {
		return 0, errors.New("must be positive")
	}
	return d, nil
}
