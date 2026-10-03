package gateway

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/daqiaoliang-coder/ai-gateway/internal/config"
)

type Gateway struct {
	clients        map[string]*client
	models         map[string]*modelRoute
	modelNames     []string
	metricsToken   string
	requestTimeout time.Duration
	maxRequest     int64
	maxConcurrent  int
	retryMax       int
	semaphore      chan struct{}
	limiter        *rateLimiter
	metrics        *metrics
	logger         *slog.Logger
}

type client struct {
	id     string
	key    []byte
	models map[string]struct{}
	rpm    int
}

type provider struct {
	name    string
	baseURL *url.URL
	model   string
	apiKey  string
	http    *http.Client
	breaker *circuitBreaker
}

type modelRoute struct {
	name        string
	deployments []*provider
	next        atomic.Uint64
}

func New(cfg config.File, logger *slog.Logger) (*Gateway, error) {
	if logger == nil {
		logger = slog.Default()
	}
	timeout, err := time.ParseDuration(cfg.RequestTimeout)
	if err != nil || timeout <= 0 {
		return nil, fmt.Errorf("invalid request_timeout %q", cfg.RequestTimeout)
	}

	providers := make(map[string]*provider, len(cfg.Providers))
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		MaxIdleConns:        512,
		MaxIdleConnsPerHost: 64,
		MaxConnsPerHost:     128,
		IdleConnTimeout:     90 * time.Second,
		TLSHandshakeTimeout: 10 * time.Second,
		ForceAttemptHTTP2:   true,
	}
	upstreamClient := &http.Client{
		Transport: transport,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for _, item := range cfg.Providers {
		parsed, err := url.Parse(item.BaseURL)
		if err != nil || parsed.Host == "" || parsed.User != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.RawQuery != "" || parsed.Fragment != "" {
			return nil, fmt.Errorf("provider %q has invalid base_url", item.Name)
		}
		apiKey := ""
		if item.APIKeyEnv != "" {
			apiKey = strings.TrimSpace(getenv(item.APIKeyEnv))
			if apiKey == "" {
				return nil, fmt.Errorf("provider %q has empty API key environment variable %s", item.Name, item.APIKeyEnv)
			}
		}
		providers[item.Name] = &provider{
			name:    item.Name,
			baseURL: parsed,
			model:   item.Model,
			apiKey:  apiKey,
			http:    upstreamClient,
			breaker: newCircuitBreaker(cfg.BreakerFailures, time.Duration(cfg.BreakerOpenSeconds)*time.Second),
		}
	}

	g := &Gateway{
		clients:        make(map[string]*client, len(cfg.Clients)),
		models:         make(map[string]*modelRoute, len(cfg.Models)),
		metricsToken:   "",
		requestTimeout: timeout,
		maxRequest:     cfg.MaxRequestBytes,
		maxConcurrent:  cfg.MaxConcurrentRequests,
		retryMax:       cfg.RetryMaxAttempts,
		semaphore:      make(chan struct{}, cfg.MaxConcurrentRequests),
		limiter:        newRateLimiter(),
		metrics:        &metrics{},
		logger:         logger,
	}
	if cfg.MetricsTokenEnv != "" {
		g.metricsToken = strings.TrimSpace(getenv(cfg.MetricsTokenEnv))
		if g.metricsToken == "" {
			return nil, fmt.Errorf("metrics token environment variable %s is empty", cfg.MetricsTokenEnv)
		}
	}
	for _, item := range cfg.Clients {
		key := strings.TrimSpace(getenv(item.KeyEnv))
		if key == "" {
			return nil, fmt.Errorf("client %q has empty key environment variable %s", item.ID, item.KeyEnv)
		}
		allowed := make(map[string]struct{}, len(item.AllowedModels))
		for _, name := range item.AllowedModels {
			allowed[name] = struct{}{}
		}
		g.clients[item.ID] = &client{id: item.ID, key: []byte(key), models: allowed, rpm: item.RPM}
	}
	for _, item := range cfg.Models {
		route := &modelRoute{name: item.Name}
		for _, deployment := range item.Deployments {
			upstream := providers[deployment]
			if upstream == nil {
				return nil, fmt.Errorf("model %q references unknown provider %q", item.Name, deployment)
			}
			route.deployments = append(route.deployments, upstream)
		}
		g.models[item.Name] = route
		g.modelNames = append(g.modelNames, item.Name)
	}
	sort.Strings(g.modelNames)
	return g, nil
}

func getenv(name string) string {
	return os.Getenv(name)
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ready\n")
	})
	mux.HandleFunc("GET /metrics", g.handleMetrics)
	mux.HandleFunc("GET /v1/models", g.handleModels)
	mux.HandleFunc("POST /v1/chat/completions", g.handleChatCompletions)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, "not_found", "route not found", requestID(r.Context()))
	})
	return g.withRequestID(mux)
}

type requestIDContextKey struct{}

func (g *Gateway) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := randomRequestID()
		ctx := context.WithValue(r.Context(), requestIDContextKey{}, id)
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func randomRequestID() string {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(raw[:])
}

func requestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDContextKey{}).(string)
	return id
}

func (g *Gateway) handleModels(w http.ResponseWriter, r *http.Request) {
	caller, ok := g.authenticate(w, r)
	if !ok {
		return
	}
	data := make([]map[string]string, 0, len(caller.models))
	for _, name := range g.modelNames {
		if _, allowed := caller.models[name]; allowed {
			data = append(data, map[string]string{"id": name, "object": "model", "owned_by": "ai-gateway"})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data})
}

func (g *Gateway) handleChatCompletions(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	g.metrics.requests.Add(1)
	g.metrics.inflight.Add(1)
	defer g.metrics.inflight.Add(-1)
	status := http.StatusOK
	var inputTokens, outputTokens int64
	var selectedModel, selectedProvider, callerID string
	defer func() {
		g.metrics.observe(status, time.Since(started))
		g.logger.Info("gateway request",
			"request_id", requestID(r.Context()),
			"client_id", callerID,
			"model", selectedModel,
			"provider", selectedProvider,
			"status", status,
			"duration_ms", time.Since(started).Milliseconds(),
			"input_tokens", inputTokens,
			"output_tokens", outputTokens,
		)
	}()

	caller, ok := g.authenticate(w, r)
	if !ok {
		status = http.StatusUnauthorized
		return
	}
	callerID = caller.id
	if caller.rpm > 0 && !g.limiter.allow(caller.id, caller.rpm) {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "rate_limit_exceeded", "client request rate limit exceeded", requestID(r.Context()))
		status = http.StatusTooManyRequests
		return
	}
	select {
	case g.semaphore <- struct{}{}:
		defer func() { <-g.semaphore }()
	default:
		writeError(w, http.StatusServiceUnavailable, "gateway_overloaded", "gateway is at its concurrency limit", requestID(r.Context()))
		status = http.StatusServiceUnavailable
		return
	}
	if r.ContentLength > g.maxRequest {
		writeError(w, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds configured limit", requestID(r.Context()))
		status = http.StatusRequestEntityTooLarge
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, g.maxRequest)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
			writeError(w, status, "request_too_large", "request body exceeds configured limit", requestID(r.Context()))
			return
		}
		status = http.StatusBadRequest
		writeError(w, status, "invalid_request", "could not read request body", requestID(r.Context()))
		return
	}
	var request struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(body, &request); err != nil || request.Model == "" {
		status = http.StatusBadRequest
		writeError(w, status, "invalid_request", "body must be valid JSON and include a model", requestID(r.Context()))
		return
	}
	selectedModel = request.Model
	if _, allowed := caller.models[request.Model]; !allowed {
		status = http.StatusForbidden
		writeError(w, status, "model_not_allowed", "client is not allowed to use the requested model", requestID(r.Context()))
		return
	}
	route := g.models[request.Model]
	if route == nil {
		status = http.StatusBadRequest
		writeError(w, status, "model_not_found", "requested model is not configured", requestID(r.Context()))
		return
	}
	select {
	case <-r.Context().Done():
		status = http.StatusRequestTimeout
		return
	default:
	}
	status, selectedProvider, inputTokens, outputTokens = g.forward(w, r, route, body, request.Stream)
}

func (g *Gateway) authenticate(w http.ResponseWriter, r *http.Request) (*client, bool) {
	const prefix = "Bearer "
	if len(r.Header.Get("Authorization")) <= len(prefix) || !strings.EqualFold(r.Header.Get("Authorization")[:len(prefix)], prefix) {
		writeError(w, http.StatusUnauthorized, "unauthorized", "valid bearer token required", requestID(r.Context()))
		return nil, false
	}
	provided := strings.TrimSpace(r.Header.Get("Authorization")[len(prefix):])
	for _, candidate := range g.clients {
		if subtle.ConstantTimeCompare([]byte(provided), candidate.key) == 1 {
			return candidate, true
		}
	}
	writeError(w, http.StatusUnauthorized, "unauthorized", "valid bearer token required", requestID(r.Context()))
	return nil, false
}

func (g *Gateway) forward(w http.ResponseWriter, r *http.Request, route *modelRoute, body []byte, stream bool) (int, string, int64, int64) {
	deployments := route.ordered()
	maxAttempts := g.retryMax
	if maxAttempts > len(deployments) {
		maxAttempts = len(deployments)
	}
	attempts := 0
	lastStatus := http.StatusBadGateway
	var lastErr error
	for _, target := range deployments {
		if attempts >= maxAttempts {
			break
		}
		if !target.breaker.allow() {
			g.metrics.breakerSkips.Add(1)
			continue
		}
		attempts++
		g.metrics.upstreamAttempts.Add(1)
		if attempts > 1 {
			g.metrics.fallbacks.Add(1)
		}
		upstreamBody, err := rewriteModel(body, target.model)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "request could not be prepared", requestID(r.Context()))
			return http.StatusBadRequest, target.name, 0, 0
		}
		endpoint := *target.baseURL
		endpoint.Path = path.Join(endpoint.Path, "chat/completions")
		ctx, cancel := context.WithTimeout(r.Context(), g.requestTimeout)
		upstreamRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), bytes.NewReader(upstreamBody))
		if err != nil {
			cancel()
			lastErr = err
			lastStatus = http.StatusBadGateway
			target.breaker.failure()
			continue
		}
		upstreamRequest.Header.Set("Content-Type", "application/json")
		upstreamRequest.Header.Set("Accept", "application/json")
		if stream {
			upstreamRequest.Header.Set("Accept", "text/event-stream")
		}
		if target.apiKey != "" {
			upstreamRequest.Header.Set("Authorization", "Bearer "+target.apiKey)
		}
		upstreamRequest.Header.Set("X-Request-ID", requestID(r.Context()))
		response, err := target.http.Do(upstreamRequest)
		if err != nil {
			cancel()
			lastErr = err
			lastStatus = http.StatusBadGateway
			target.breaker.failure()
			g.metrics.upstreamErrors.Add(1)
			continue
		}
		if shouldRetry(response.StatusCode) {
			lastStatus = response.StatusCode
			lastErr = fmt.Errorf("upstream returned status %d", response.StatusCode)
			response.Body.Close()
			cancel()
			if response.StatusCode >= 500 {
				target.breaker.failure()
			} else {
				target.breaker.success()
			}
			g.metrics.upstreamErrors.Add(1)
			continue
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			input, output, copyErr := copyResponse(w, response, requestID(r.Context()), stream)
			_ = response.Body.Close()
			cancel()
			if copyErr != nil {
				target.breaker.failure()
				g.metrics.upstreamErrors.Add(1)
				if !stream {
					lastStatus = http.StatusBadGateway
					lastErr = copyErr
					continue
				}
				return response.StatusCode, target.name, input, output
			}
			target.breaker.success()
			return response.StatusCode, target.name, input, output
		}
		// Provider 4xx responses other than 429 describe this request, so they are
		// returned directly instead of silently changing the model.
		target.breaker.success()
		copyErrorResponse(w, response, requestID(r.Context()))
		status := response.StatusCode
		cancel()
		return status, target.name, 0, 0
	}
	if attempts == 0 {
		lastErr = errors.New("all deployments are circuit-open or unavailable")
	}
	if lastStatus == http.StatusTooManyRequests {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "upstream_rate_limited", "all available model deployments are rate limited", requestID(r.Context()))
		return http.StatusTooManyRequests, "", 0, 0
	}
	g.logger.Warn("upstream unavailable", "request_id", requestID(r.Context()), "model", route.name, "attempts", attempts, "error", lastErr)
	writeError(w, http.StatusBadGateway, "upstream_unavailable", "no model deployment completed the request", requestID(r.Context()))
	return http.StatusBadGateway, "", 0, 0
}

func (r *modelRoute) ordered() []*provider {
	count := uint64(len(r.deployments))
	if count == 0 {
		return nil
	}
	start := r.next.Add(1) - 1
	ordered := make([]*provider, 0, len(r.deployments))
	for i := uint64(0); i < count; i++ {
		ordered = append(ordered, r.deployments[(start+i)%count])
	}
	return ordered
}

func rewriteModel(body []byte, model string) ([]byte, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	value, err := json.Marshal(model)
	if err != nil {
		return nil, err
	}
	payload["model"] = value
	return json.Marshal(payload)
}

func shouldRetry(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

func copyResponse(w http.ResponseWriter, response *http.Response, id string, stream bool) (int64, int64, error) {
	copyHeader(w.Header(), response.Header)
	w.Header().Set("X-Request-ID", id)
	if stream {
		w.Header().Set("X-Accel-Buffering", "no")
		w.WriteHeader(response.StatusCode)
		flusher, ok := w.(http.Flusher)
		if !ok {
			return 0, 0, errors.New("streaming is not supported by response writer")
		}
		reader := bufio.NewReader(response.Body)
		var input, output int64
		var streamedBytes int64
		for {
			line, err := reader.ReadString('\n')
			if len(line) > 0 {
				streamedBytes += int64(len(line))
				if streamedBytes > 64<<20 {
					return input, output, errors.New("upstream stream exceeded 64 MiB limit")
				}
				if strings.HasPrefix(strings.TrimSpace(line), "data:") {
					data := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "data:"))
					if data != "[DONE]" {
						in, out := extractUsage([]byte(data))
						if in > 0 || out > 0 {
							input, output = in, out
						}
					}
				}
				if _, writeErr := io.WriteString(w, line); writeErr != nil {
					return input, output, writeErr
				}
				flusher.Flush()
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					return input, output, nil
				}
				return input, output, err
			}
		}
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, (16<<20)+1))
	if err != nil {
		return 0, 0, err
	}
	if len(body) > 16<<20 {
		return 0, 0, errors.New("upstream response exceeds 16 MiB limit")
	}
	input, output := extractUsage(body)
	w.WriteHeader(response.StatusCode)
	_, err = w.Write(body)
	return input, output, err
}

func copyErrorResponse(w http.ResponseWriter, response *http.Response, id string) {
	defer response.Body.Close()
	copyHeader(w.Header(), response.Header)
	w.Header().Set("X-Request-ID", id)
	body, err := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
	if err != nil {
		writeError(w, http.StatusBadGateway, "upstream_error", "provider error response could not be read", id)
		return
	}
	if len(body) > 1<<20 {
		writeError(w, http.StatusBadGateway, "upstream_error", "provider error response exceeded the size limit", id)
		return
	}
	w.WriteHeader(response.StatusCode)
	_, _ = w.Write(body)
}

func copyHeader(dst, src http.Header) {
	for _, name := range []string{"Content-Type", "Cache-Control"} {
		if value := src.Get(name); value != "" {
			dst.Set(name, value)
		}
	}
}

func extractUsage(body []byte) (int64, int64) {
	var envelope struct {
		Usage struct {
			PromptTokens     int64 `json:"prompt_tokens"`
			CompletionTokens int64 `json:"completion_tokens"`
		} `json:"usage"`
	}
	if json.Unmarshal(body, &envelope) != nil {
		return 0, 0
	}
	return envelope.Usage.PromptTokens, envelope.Usage.CompletionTokens
}

func (g *Gateway) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if g.metricsToken == "" {
		http.NotFound(w, r)
		return
	}
	header := r.Header.Get("Authorization")
	if len(header) <= 7 || !strings.EqualFold(header[:7], "Bearer ") || subtle.ConstantTimeCompare([]byte(strings.TrimSpace(header[7:])), []byte(g.metricsToken)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	g.metrics.writePrometheus(w)
}

func writeError(w http.ResponseWriter, status int, code, message, id string) {
	w.Header().Set("Content-Type", "application/json")
	if id != "" {
		w.Header().Set("X-Request-ID", id)
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{
			"message": message,
			"type":    "gateway_error",
			"code":    code,
		},
	})
}

// rateLimiter is process-local. Multi-replica deployments should replace it
// with a shared limiter before relying on strict fleet-wide RPM enforcement.
type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]bucket
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter() *rateLimiter {
	return &rateLimiter{buckets: make(map[string]bucket)}
}

func (l *rateLimiter) allow(key string, rpm int) bool {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b, ok := l.buckets[key]
	if !ok {
		b = bucket{tokens: float64(rpm), last: now}
	}
	b.tokens += now.Sub(b.last).Seconds() * float64(rpm) / 60
	if b.tokens > float64(rpm) {
		b.tokens = float64(rpm)
	}
	b.last = now
	if b.tokens < 1 {
		l.buckets[key] = b
		return false
	}
	b.tokens--
	l.buckets[key] = b
	return true
}

type circuitBreaker struct {
	mu               sync.Mutex
	threshold        int
	openDuration     time.Duration
	failures         int
	openUntil        time.Time
	halfOpenInFlight bool
}

func newCircuitBreaker(threshold int, openDuration time.Duration) *circuitBreaker {
	return &circuitBreaker{threshold: threshold, openDuration: openDuration}
}

func (b *circuitBreaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.openUntil.IsZero() {
		return true
	}
	if time.Now().Before(b.openUntil) {
		return false
	}
	if b.halfOpenInFlight {
		return false
	}
	b.halfOpenInFlight = true
	return true
}

func (b *circuitBreaker) success() {
	b.mu.Lock()
	b.failures = 0
	b.openUntil = time.Time{}
	b.halfOpenInFlight = false
	b.mu.Unlock()
}

func (b *circuitBreaker) failure() {
	b.mu.Lock()
	b.failures++
	b.halfOpenInFlight = false
	if b.failures >= b.threshold {
		b.openUntil = time.Now().Add(b.openDuration)
	}
	b.mu.Unlock()
}
