package gateway

import (
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
	mathrand "math/rand"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/daqiaoliang-coder/ai-gateway/internal/config"
	"github.com/daqiaoliang-coder/ai-gateway/internal/policy"
)

// maxErrorBodyBytes bounds how much of a provider error response is buffered.
const maxErrorBodyBytes = 1 << 20

// maxResponseBytes bounds a buffered non-streamed provider response.
const maxResponseBytes = 16 << 20

// minAttemptBudgetFloor is the smallest remaining deadline that still justifies
// starting another attempt.
const minAttemptBudgetFloor = 5 * time.Second

type Gateway struct {
	clients       map[string]*client
	models        map[string]*modelRoute
	modelNames    []string
	providerList  []*provider
	metricsToken  string
	maxRequest    int64
	maxConcurrent int
	semaphore     chan struct{}
	limiter       *rateLimiter
	metrics       *metrics
	logger        *slog.Logger

	contract         *policy.Contract
	totalDeadline    time.Duration
	attemptTimeout   time.Duration
	minAttemptBudget time.Duration
	backoffInitial   time.Duration
	backoffMax       time.Duration
	maxRetryAfter    time.Duration
	streamMaxEvent   int64
	streamMaxBytes   int64
	draining         atomic.Bool
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

	lastSuccessUnix atomic.Int64
	lastFailureUnix atomic.Int64
}

func (p *provider) markSuccess() { p.lastSuccessUnix.Store(time.Now().UnixNano()) }
func (p *provider) markFailure() { p.lastFailureUnix.Store(time.Now().UnixNano()) }

type modelRoute struct {
	name        string
	deployments []*provider
	next        atomic.Uint64
}

func New(cfg config.File, contract *policy.Contract, logger *slog.Logger) (*Gateway, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if contract == nil {
		contract = policy.Default()
	}
	if err := contract.Validate(); err != nil {
		return nil, fmt.Errorf("invalid behavior contract: %w", err)
	}
	// config.RequestTimeout stays validated by config.Load for compatibility;
	// the behavior contract owns the effective time budget from now on.
	if _, err := time.ParseDuration(cfg.RequestTimeout); err != nil {
		return nil, fmt.Errorf("invalid request_timeout %q", cfg.RequestTimeout)
	}

	totalDeadline, err := time.ParseDuration(contract.Timeouts.TotalDeadline)
	if err != nil {
		return nil, fmt.Errorf("invalid contract total_deadline: %w", err)
	}
	attemptTimeout, err := time.ParseDuration(contract.Timeouts.AttemptTimeout)
	if err != nil {
		return nil, fmt.Errorf("invalid contract attempt_timeout: %w", err)
	}
	backoffInitial, err := time.ParseDuration(contract.Retry.BackoffInitial)
	if err != nil {
		return nil, fmt.Errorf("invalid contract backoff_initial: %w", err)
	}
	backoffMax, err := time.ParseDuration(contract.Retry.BackoffMax)
	if err != nil {
		return nil, fmt.Errorf("invalid contract backoff_max: %w", err)
	}
	maxRetryAfter, err := time.ParseDuration(contract.Retry.MaxRetryAfter)
	if err != nil {
		return nil, fmt.Errorf("invalid contract max_retry_after: %w", err)
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
		maxRequest:     cfg.MaxRequestBytes,
		maxConcurrent:  cfg.MaxConcurrentRequests,
		semaphore:      make(chan struct{}, cfg.MaxConcurrentRequests),
		limiter:        newRateLimiter(),
		metrics:        &metrics{},
		logger:         logger,
		contract:       contract,
		totalDeadline:  totalDeadline,
		attemptTimeout: attemptTimeout,
		backoffInitial: backoffInitial,
		backoffMax:     backoffMax,
		maxRetryAfter:  maxRetryAfter,
		streamMaxEvent: contract.Streaming.MaxEventBytes,
		streamMaxBytes: contract.Streaming.MaxStreamBytes,
	}
	g.minAttemptBudget = attemptTimeout
	if g.minAttemptBudget > minAttemptBudgetFloor {
		g.minAttemptBudget = minAttemptBudgetFloor
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
	for name := range providers {
		g.providerList = append(g.providerList, providers[name])
	}
	sort.Slice(g.providerList, func(i, j int) bool { return g.providerList[i].name < g.providerList[j].name })

	logger.Info("behavior contract in effect",
		"total_deadline", contract.Timeouts.TotalDeadline,
		"attempt_timeout", contract.Timeouts.AttemptTimeout,
		"max_attempts", contract.Retry.MaxAttempts,
		"replay_unknown_outcome", contract.ReplaysUnknownOutcome(),
		"stream_max_event_bytes", contract.Streaming.MaxEventBytes,
		"stream_max_bytes", contract.Streaming.MaxStreamBytes,
	)
	return g, nil
}

// Drain marks the instance as no longer accepting new traffic. It is called
// before the HTTP server starts shutting down.
func (g *Gateway) Drain() { g.draining.Store(true) }

func (g *Gateway) isDraining() bool { return g.draining.Load() }

func getenv(name string) string {
	return os.Getenv(name)
}

func (g *Gateway) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("GET /readyz", g.handleReady)
	mux.HandleFunc("GET /upstreams", g.handleUpstreams)
	mux.HandleFunc("GET /metrics", g.handleMetrics)
	mux.HandleFunc("GET /v1/models", g.handleModels)
	mux.HandleFunc("POST /v1/chat/completions", g.handleChatCompletions)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, policy.CodeNotFound, "route not found", requestID(r.Context()))
	})
	return g.withRequestID(mux)
}

func (g *Gateway) handleReady(w http.ResponseWriter, r *http.Request) {
	if g.isDraining() {
		writeError(w, http.StatusServiceUnavailable, policy.CodeDraining, "instance is draining and no longer accepts traffic", requestID(r.Context()))
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "ready\n")
}

type upstreamStatus struct {
	Name               string `json:"name"`
	Model              string `json:"model"`
	CircuitOpen        bool   `json:"circuit_open"`
	ConsecutiveFailure int    `json:"consecutive_failures"`
	LastSuccess        string `json:"last_success,omitempty"`
	LastFailure        string `json:"last_failure,omitempty"`
}

// handleUpstreams exposes provider health for observation only. Liveness never
// depends on it, so a provider outage cannot restart the fleet.
func (g *Gateway) handleUpstreams(w http.ResponseWriter, r *http.Request) {
	if !g.authorizeMetrics(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	statuses := make([]upstreamStatus, 0, len(g.providerList))
	for _, p := range g.providerList {
		item := upstreamStatus{
			Name:               p.name,
			Model:              p.model,
			CircuitOpen:        !p.breaker.allow(),
			ConsecutiveFailure: p.breaker.failureCount(),
		}
		if ts := p.lastSuccessUnix.Load(); ts > 0 {
			item.LastSuccess = time.Unix(0, ts).UTC().Format(time.RFC3339Nano)
		}
		if ts := p.lastFailureUnix.Load(); ts > 0 {
			item.LastFailure = time.Unix(0, ts).UTC().Format(time.RFC3339Nano)
		}
		statuses = append(statuses, item)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": statuses})
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
	completion := policy.StreamCompleted
	outcome := policy.OutcomeNotAttempted
	var attempts int
	defer func() {
		g.metrics.observe(status, time.Since(started), completion)
		args := []any{
			"request_id", requestID(r.Context()),
			"client_id", callerID,
			"model", selectedModel,
			"provider", selectedProvider,
			"status", status,
			"duration_ms", time.Since(started).Milliseconds(),
			"input_tokens", inputTokens,
			"output_tokens", outputTokens,
			"outcome", outcome.String(),
			"stream_completion", string(completion),
			"attempts", attempts,
		}
		if completion.CountsAsSuccess() {
			g.logger.Info("gateway request", args...)
			return
		}
		g.logger.Warn("gateway request incomplete", args...)
	}()

	if g.isDraining() {
		status = http.StatusServiceUnavailable
		setRetryAfter(w, time.Second)
		writeError(w, status, policy.CodeDraining, "instance is draining and no longer accepts requests", requestID(r.Context()))
		return
	}
	caller, ok := g.authenticate(w, r)
	if !ok {
		status = http.StatusUnauthorized
		return
	}
	callerID = caller.id
	if caller.rpm > 0 && !g.limiter.allow(caller.id, caller.rpm) {
		setRetryAfter(w, time.Second)
		writeError(w, http.StatusTooManyRequests, policy.CodeRateLimitExceeded, "client request rate limit exceeded", requestID(r.Context()))
		status = http.StatusTooManyRequests
		return
	}
	select {
	case g.semaphore <- struct{}{}:
		defer func() { <-g.semaphore }()
	default:
		setRetryAfter(w, time.Second)
		writeError(w, http.StatusServiceUnavailable, policy.CodeGatewayOverloaded, "gateway is at its concurrency limit", requestID(r.Context()))
		status = http.StatusServiceUnavailable
		return
	}
	if r.ContentLength > g.maxRequest {
		writeError(w, http.StatusRequestEntityTooLarge, policy.CodeRequestTooLarge, "request body exceeds configured limit", requestID(r.Context()))
		status = http.StatusRequestEntityTooLarge
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, g.maxRequest)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			status = http.StatusRequestEntityTooLarge
			writeError(w, status, policy.CodeRequestTooLarge, "request body exceeds configured limit", requestID(r.Context()))
			return
		}
		status = http.StatusBadRequest
		writeError(w, status, policy.CodeInvalidRequest, "could not read request body", requestID(r.Context()))
		return
	}
	var request struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(body, &request); err != nil || request.Model == "" {
		status = http.StatusBadRequest
		writeError(w, status, policy.CodeInvalidRequest, "body must be valid JSON and include a model", requestID(r.Context()))
		return
	}
	selectedModel = request.Model
	if _, allowed := caller.models[request.Model]; !allowed {
		status = http.StatusForbidden
		writeError(w, status, policy.CodeModelNotAllowed, "client is not allowed to use the requested model", requestID(r.Context()))
		return
	}
	route := g.models[request.Model]
	if route == nil {
		status = http.StatusBadRequest
		writeError(w, status, policy.CodeModelNotFound, "requested model is not configured", requestID(r.Context()))
		return
	}

	// One deadline for the whole request, shared by every attempt.
	ctx, cancel := context.WithTimeout(r.Context(), g.totalDeadline)
	defer cancel()
	r = r.WithContext(ctx)
	if ctx.Err() != nil {
		status = http.StatusRequestTimeout
		writeError(w, status, policy.CodeUpstreamTimeout, "the request was cancelled before any upstream attempt", requestID(ctx))
		completion = policy.StreamAbortedClient
		return
	}

	result := g.forward(w, r, route, body, request.Stream)
	status = result.status
	selectedProvider = result.provider
	inputTokens = result.input
	outputTokens = result.output
	completion = result.completion
	outcome = result.outcome
	attempts = result.attempts
}

func (g *Gateway) authenticate(w http.ResponseWriter, r *http.Request) (*client, bool) {
	const prefix = "Bearer "
	if len(r.Header.Get("Authorization")) <= len(prefix) || !strings.EqualFold(r.Header.Get("Authorization")[:len(prefix)], prefix) {
		writeError(w, http.StatusUnauthorized, policy.CodeUnauthorized, "valid bearer token required", requestID(r.Context()))
		return nil, false
	}
	provided := strings.TrimSpace(r.Header.Get("Authorization")[len(prefix):])
	for _, candidate := range g.clients {
		if subtle.ConstantTimeCompare([]byte(provided), candidate.key) == 1 {
			return candidate, true
		}
	}
	writeError(w, http.StatusUnauthorized, policy.CodeUnauthorized, "valid bearer token required", requestID(r.Context()))
	return nil, false
}

// forwardResult is what the handler needs to log and account for.
type forwardResult struct {
	status     int
	provider   string
	input      int64
	output     int64
	completion policy.StreamCompletion
	outcome    policy.Outcome
	attempts   int
}

// attemptResult describes a single upstream attempt.
type attemptResult struct {
	outcome    policy.Outcome
	status     int
	provider   string
	input      int64
	output     int64
	completion policy.StreamCompletion
	retryAfter time.Duration
	fail       copyFailure
	errorBody  []byte
	responded  bool
	err        error
}

func (g *Gateway) forward(w http.ResponseWriter, r *http.Request, route *modelRoute, body []byte, stream bool) forwardResult {
	ctx := r.Context()
	id := requestID(ctx)
	order := route.ordered()
	if len(order) == 0 {
		writeError(w, http.StatusBadGateway, policy.CodeUpstreamUnavailable, "no deployment is configured for the requested model", id)
		return forwardResult{status: http.StatusBadGateway, completion: policy.StreamAbortedGateway, outcome: policy.OutcomeUnknown}
	}

	pol := g.contract
	idx := 0
	attempts := 0
	last := attemptResult{outcome: policy.OutcomeUnknown}

	for idx < len(order) && attempts < pol.Retry.MaxAttempts {
		target := order[idx]
		if !target.breaker.allow() {
			g.metrics.breakerSkips.Add(1)
			idx++
			continue
		}
		if !hasDeadlineBudget(ctx, g.minAttemptBudget) {
			g.logger.Warn("retry skipped: total deadline exhausted",
				"request_id", id, "model", route.name, "attempts", attempts)
			break
		}
		attempts++
		g.metrics.upstreamAttempts.Add(1)
		if attempts > 1 {
			g.metrics.retries.Add(1)
		}

		res := g.attemptOnce(w, ctx, target, body, stream)
		last = res

		if res.outcome == policy.OutcomeProcessed && res.err == nil {
			target.breaker.success()
			target.markSuccess()
			return forwardResult{
				status:     res.status,
				provider:   res.provider,
				input:      res.input,
				output:     res.output,
				completion: res.completion,
				outcome:    res.outcome,
				attempts:   attempts,
			}
		}

		g.recordFailure(target, res)
		decision := pol.Decide(res.outcome)
		if decision.Terminal {
			if res.outcome == policy.OutcomeUnknown || res.outcome == policy.OutcomePartial || res.outcome == policy.OutcomeProcessed {
				g.metrics.replaySkipped.Add(1)
			}
			g.logger.Warn("upstream attempt is terminal",
				"request_id", id, "model", route.name, "provider", res.provider,
				"outcome", res.outcome.String(), "reason", decision.Reason, "error", res.err)
			break
		}

		if !g.waitBeforeRetry(ctx, attempts, res.retryAfter) {
			g.logger.Warn("retry abandoned: total deadline reached while backing off",
				"request_id", id, "model", route.name, "provider", res.provider, "attempts", attempts)
			break
		}
		// Prefer sending the request somewhere else when another deployment is
		// available; replaying the same one is only the fallback when this model
		// has a single deployment.
		if decision.SwitchProvider && idx+1 < len(order) {
			idx++
			continue
		}
		if !decision.ReplaySame {
			idx++
		}
	}

	// Once bytes have been written to the client the status is already sent.
	// Nothing may rewrite it, and no other provider may take over.
	if last.responded {
		if !last.completion.CountsAsSuccess() {
			g.metrics.streamAborted.Add(1)
		}
		return forwardResult{
			status:     last.status,
			provider:   last.provider,
			input:      last.input,
			output:     last.output,
			completion: last.completion,
			outcome:    last.outcome,
			attempts:   attempts,
		}
	}
	return g.writeFailureResponse(w, ctx, route, last, attempts)
}

func (g *Gateway) attemptOnce(w http.ResponseWriter, ctx context.Context, target *provider, body []byte, stream bool) attemptResult {
	id := requestID(ctx)
	upstreamBody, err := rewriteModel(body, target.model, stream && g.contract.Streaming.RequireUsageInStream)
	if err != nil {
		return attemptResult{outcome: policy.OutcomeRejected, status: http.StatusBadRequest, provider: target.name, err: err}
	}
	endpoint := *target.baseURL
	endpoint.Path = path.Join(endpoint.Path, "chat/completions")

	attemptCtx, cancel := context.WithTimeout(ctx, g.attemptTimeout)
	defer cancel()

	// httptrace tells us whether the request bytes actually reached the wire,
	// which is what separates a replayable failure from an uncertain one.
	var wrote atomic.Bool
	trace := &httptrace.ClientTrace{
		WroteRequest: func(httptrace.WroteRequestInfo) { wrote.Store(true) },
	}
	upstreamRequest, err := http.NewRequestWithContext(
		httptrace.WithClientTrace(attemptCtx, trace),
		http.MethodPost, endpoint.String(), bytes.NewReader(upstreamBody))
	if err != nil {
		return attemptResult{outcome: policy.OutcomeNotSent, provider: target.name, err: err}
	}
	upstreamRequest.Header.Set("Content-Type", "application/json")
	upstreamRequest.Header.Set("Accept", "application/json")
	if stream {
		upstreamRequest.Header.Set("Accept", "text/event-stream")
	}
	if target.apiKey != "" {
		upstreamRequest.Header.Set("Authorization", "Bearer "+target.apiKey)
	}
	upstreamRequest.Header.Set("X-Request-ID", id)

	response, err := target.http.Do(upstreamRequest)
	if err != nil {
		phase := policy.PhasePreSend
		if wrote.Load() {
			phase = policy.PhaseAwaitingStatus
		}
		return attemptResult{
			outcome:  policy.Classify(phase, 0, err),
			provider: target.name,
			fail:     failUpstream,
			err:      err,
		}
	}
	defer response.Body.Close()

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		if stream {
			copyHeader(w.Header(), response.Header)
			w.Header().Set("X-Request-ID", id)
			w.Header().Set("X-Accel-Buffering", "no")
			w.WriteHeader(response.StatusCode)
			res := g.copyStream(w, attemptCtx, response.Body)
			return attemptResult{
				outcome:    policy.Classify(policy.PhaseBodyForwarding, response.StatusCode, res.err),
				status:     response.StatusCode,
				provider:   target.name,
				input:      res.input,
				output:     res.output,
				completion: res.completion,
				fail:       res.fail,
				responded:  true,
				err:        res.err,
			}
		}
		payload, readErr := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
		if readErr != nil {
			return attemptResult{
				outcome:  policy.Classify(policy.PhaseBodyForwarding, response.StatusCode, readErr),
				status:   response.StatusCode,
				provider: target.name,
				fail:     failUpstream,
				err:      readErr,
			}
		}
		if int64(len(payload)) > maxResponseBytes {
			return attemptResult{
				outcome:  policy.OutcomeProcessed,
				status:   response.StatusCode,
				provider: target.name,
				fail:     failGateway,
				err:      errors.New("upstream response exceeds the configured limit"),
			}
		}
		input, output := extractUsage(payload)
		copyHeader(w.Header(), response.Header)
		w.Header().Set("X-Request-ID", id)
		w.WriteHeader(response.StatusCode)
		if _, writeErr := w.Write(payload); writeErr != nil {
			return attemptResult{
				outcome:    policy.OutcomeProcessed,
				status:     response.StatusCode,
				provider:   target.name,
				input:      input,
				output:     output,
				completion: policy.StreamCompleted,
				fail:       failClient,
				responded:  true,
				err:        writeErr,
			}
		}
		return attemptResult{
			outcome:    policy.OutcomeProcessed,
			status:     response.StatusCode,
			provider:   target.name,
			input:      input,
			output:     output,
			completion: policy.StreamCompleted,
			responded:  true,
		}
	}

	errorBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxErrorBodyBytes+1))
	if readErr != nil {
		return attemptResult{
			outcome:  policy.Classify(policy.PhaseAwaitingStatus, response.StatusCode, readErr),
			status:   response.StatusCode,
			provider: target.name,
			fail:     failUpstream,
			err:      readErr,
		}
	}
	if int64(len(errorBody)) > maxErrorBodyBytes {
		errorBody = nil
	}
	return attemptResult{
		outcome:    policy.Classify(policy.PhaseAwaitingStatus, response.StatusCode, nil),
		status:     response.StatusCode,
		provider:   target.name,
		retryAfter: parseRetryAfter(response.Header.Get("Retry-After")),
		errorBody:  errorBody,
	}
}

func (g *Gateway) recordFailure(target *provider, res attemptResult) {
	g.metrics.upstreamErrors.Add(1)
	switch res.outcome {
	case policy.OutcomeNotSent, policy.OutcomeUnknown:
		target.breaker.failure()
		target.markFailure()
	case policy.OutcomeThrottled, policy.OutcomeRejected:
		// The provider answered, so it is reachable; the request itself is the
		// problem, not the deployment.
		target.breaker.success()
		target.markSuccess()
	default:
		// Processed or partial output: only blame the provider when the failure
		// was not caused by the client or by the gateway deadline.
		if res.fail == failUpstream {
			target.breaker.failure()
			target.markFailure()
		}
	}
}

func (g *Gateway) writeFailureResponse(w http.ResponseWriter, ctx context.Context, route *modelRoute, last attemptResult, attempts int) forwardResult {
	id := requestID(ctx)
	status := http.StatusBadGateway
	code := policy.CodeUpstreamUnavailable
	message := "no model deployment completed the request"
	completion := policy.StreamAbortedUpstream

	switch {
	case attempts == 0:
		message = "all model deployments are circuit-open or unavailable"
	case last.outcome == policy.OutcomeThrottled:
		status, code, message = http.StatusTooManyRequests, policy.CodeUpstreamRateLimited, "all available model deployments are rate limited"
		setRetryAfter(w, last.retryAfter)
	case last.outcome == policy.OutcomeRejected:
		if g.passthroughAllowed(last.status) && len(last.errorBody) > 0 {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Request-ID", id)
			w.WriteHeader(last.status)
			_, _ = w.Write(last.errorBody)
			return forwardResult{status: last.status, provider: last.provider, outcome: last.outcome, completion: policy.StreamAbortedUpstream, attempts: attempts}
		}
		if last.status >= 400 && last.status < 500 {
			status = last.status
		}
		code, message = policy.CodeUpstreamError, "the provider rejected the request"
	case last.outcome == policy.OutcomeUnknown:
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			code, message = policy.CodeUpstreamTimeout, "the request deadline was reached before a deployment completed it"
			completion = policy.StreamAbortedGateway
		}
	case last.outcome == policy.OutcomeProcessed, last.outcome == policy.OutcomePartial:
		g.metrics.streamAborted.Add(1)
		code, message = policy.CodeUpstreamError, "the provider response could not be delivered"
	}

	g.logger.Warn("upstream unavailable",
		"request_id", id, "model", route.name, "provider", last.provider,
		"attempts", attempts, "outcome", last.outcome.String(), "error", last.err)
	writeErrorWithProvider(w, status, code, message, id, last.provider, last.status)
	return forwardResult{status: status, provider: last.provider, outcome: last.outcome, completion: completion, attempts: attempts}
}

func (g *Gateway) passthroughAllowed(status int) bool {
	for _, allowed := range g.contract.ErrorCodes.PassthroughStatuses {
		if allowed == status {
			return true
		}
	}
	return false
}

// waitBeforeRetry sleeps before another attempt. It returns false when the
// total deadline expires first, so retries can never outlive the request.
func (g *Gateway) waitBeforeRetry(ctx context.Context, attempt int, retryAfter time.Duration) bool {
	var wait time.Duration
	switch {
	case g.contract.Retry.RespectRetryAfter && retryAfter > 0:
		wait = retryAfter
		if wait > g.maxRetryAfter {
			wait = g.maxRetryAfter
		}
	default:
		wait = g.backoffInitial
		for i := 1; i < attempt; i++ {
			wait = time.Duration(float64(wait) * g.contract.Retry.BackoffMultiplier)
			if wait >= g.backoffMax {
				wait = g.backoffMax
				break
			}
		}
		if wait > g.backoffMax {
			wait = g.backoffMax
		}
	}
	if g.contract.Retry.Jitter && wait > 0 {
		half := int64(wait) / 2
		wait = time.Duration(half) + time.Duration(mathrand.Int63n(half+1))
	}
	if wait <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func hasDeadlineBudget(ctx context.Context, min time.Duration) bool {
	deadline, ok := ctx.Deadline()
	if !ok {
		return true
	}
	return time.Until(deadline) >= min
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

// rewriteModel replaces the client-facing alias with the provider model name and
// optionally asks for usage reporting on streams, without which streamed token
// counts would always be zero.
func rewriteModel(body []byte, model string, includeStreamUsage bool) ([]byte, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return nil, err
	}
	value, err := json.Marshal(model)
	if err != nil {
		return nil, err
	}
	payload["model"] = value
	if includeStreamUsage {
		options := map[string]any{}
		if existing, ok := payload["stream_options"]; ok && len(existing) > 0 {
			_ = json.Unmarshal(existing, &options)
		}
		if _, ok := options["include_usage"]; !ok {
			options["include_usage"] = true
		}
		merged, err := json.Marshal(options)
		if err != nil {
			return nil, err
		}
		payload["stream_options"] = merged
	}
	return json.Marshal(payload)
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

func parseRetryAfter(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if moment, err := http.ParseTime(raw); err == nil {
		return time.Until(moment)
	}
	return 0
}

func setRetryAfter(w http.ResponseWriter, d time.Duration) {
	if d <= 0 {
		d = time.Second
	}
	seconds := int(d.Seconds())
	if seconds < 1 {
		seconds = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(seconds))
}

func (g *Gateway) authorizeMetrics(r *http.Request) bool {
	if g.metricsToken == "" {
		return false
	}
	header := r.Header.Get("Authorization")
	return len(header) > 7 &&
		strings.EqualFold(header[:7], "Bearer ") &&
		subtle.ConstantTimeCompare([]byte(strings.TrimSpace(header[7:])), []byte(g.metricsToken)) == 1
}

func (g *Gateway) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if g.metricsToken == "" {
		http.NotFound(w, r)
		return
	}
	if !g.authorizeMetrics(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	g.metrics.writePrometheus(w)
}

func writeError(w http.ResponseWriter, status int, code, message, id string) {
	writeErrorWithProvider(w, status, code, message, id, "", 0)
}

func writeErrorWithProvider(w http.ResponseWriter, status int, code, message, id, provider string, providerStatus int) {
	w.Header().Set("Content-Type", "application/json")
	if id != "" {
		w.Header().Set("X-Request-ID", id)
	}
	w.WriteHeader(status)
	payload := map[string]any{
		"message": message,
		"type":    "gateway_error",
		"code":    code,
	}
	if provider != "" {
		payload["provider"] = provider
	}
	if providerStatus > 0 {
		payload["provider_status"] = providerStatus
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"error": payload})
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

func (b *circuitBreaker) failureCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.failures
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
