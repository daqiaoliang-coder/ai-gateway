package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daqiaoliang-coder/ai-gateway/internal/audit"
	"github.com/daqiaoliang-coder/ai-gateway/internal/cost"
	"github.com/daqiaoliang-coder/ai-gateway/internal/policy"
	"github.com/daqiaoliang-coder/ai-gateway/internal/quota"
)

const testKey = "test-client-key"

// testUpstream is a provider simulator that counts how many requests it served.
type testUpstream struct {
	name   string
	server *httptest.Server
	calls  atomic.Int64
	body   atomic.Value
}

func newUpstream(t *testing.T, name string, handler http.HandlerFunc) *testUpstream {
	t.Helper()
	u := &testUpstream{name: name}
	u.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.calls.Add(1)
		raw, _ := io.ReadAll(r.Body)
		u.body.Store(string(raw))
		handler(w, r)
	}))
	t.Cleanup(u.server.Close)
	return u
}

func (u *testUpstream) requestBody(t *testing.T) string {
	t.Helper()
	raw, _ := u.body.Load().(string)
	return raw
}

func (u *testUpstream) parsedURL(t *testing.T) *url.URL {
	t.Helper()
	parsed, err := url.Parse(u.server.URL)
	if err != nil {
		t.Fatalf("parse upstream url: %v", err)
	}
	return parsed
}

func mustDuration(t *testing.T, raw string) time.Duration {
	t.Helper()
	d, err := time.ParseDuration(raw)
	if err != nil {
		t.Fatalf("parse duration %q: %v", raw, err)
	}
	return d
}

func newTestGateway(t *testing.T, contract *policy.Contract, upstreams ...*testUpstream) *Gateway {
	t.Helper()
	if contract == nil {
		contract = policy.Default()
	}
	if err := contract.Validate(); err != nil {
		t.Fatalf("invalid contract: %v", err)
	}
	attempt := mustDuration(t, contract.Timeouts.AttemptTimeout)
	budget := attempt
	if budget > minAttemptBudgetFloor {
		budget = minAttemptBudgetFloor
	}
	g := &Gateway{
		clients:          map[string]*client{},
		models:           map[string]*modelRoute{},
		modelNames:       []string{"fast"},
		maxRequest:       1 << 20,
		maxConcurrent:    8,
		semaphore:        make(chan struct{}, 8),
		metrics:          &metrics{},
		logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		contract:         contract,
		totalDeadline:    mustDuration(t, contract.Timeouts.TotalDeadline),
		attemptTimeout:   attempt,
		minAttemptBudget: budget,
		backoffInitial:   mustDuration(t, contract.Retry.BackoffInitial),
		backoffMax:       mustDuration(t, contract.Retry.BackoffMax),
		maxRetryAfter:    mustDuration(t, contract.Retry.MaxRetryAfter),
		streamMaxEvent:   contract.Streaming.MaxEventBytes,
		streamMaxBytes:   contract.Streaming.MaxStreamBytes,
	}
	g.clients["test"] = &client{
		id:     "test",
		key:    []byte(testKey),
		models: map[string]struct{}{"fast": {}},
	}
	route := &modelRoute{name: "fast"}
	for _, upstream := range upstreams {
		route.deployments = append(route.deployments, &provider{
			name:    upstream.name,
			baseURL: upstream.parsedURL(t),
			model:   "upstream-model",
			http:    upstream.server.Client(),
			breaker: newCircuitBreaker(5, time.Second),
		})
		g.providerList = append(g.providerList, route.deployments[len(route.deployments)-1])
	}
	g.models["fast"] = route
	return g
}

func postChat(t *testing.T, g *Gateway, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+testKey)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	g.Handler().ServeHTTP(recorder, request)
	return recorder
}

func chatBody(model string, stream bool) string {
	raw, _ := json.Marshal(map[string]any{
		"model":    model,
		"stream":   stream,
		"messages": []map[string]string{{"role": "user", "content": "hello"}},
	})
	return string(raw)
}

func errorCode(t *testing.T, recorder *httptest.ResponseRecorder) string {
	t.Helper()
	var envelope struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode error envelope from %q: %v", recorder.Body.String(), err)
	}
	return envelope.Error.Code
}

// Acceptance 1: a request whose upstream outcome is uncertain must not be
// replayed, by default, at all.
func TestNoReplayOnUpstream5xx(t *testing.T) {
	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"boom"}`))
	})
	backup := newUpstream(t, "backup", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"usage":{"prompt_tokens":1,"completion_tokens":2}}`))
	})
	g := newTestGateway(t, nil, primary, backup)

	recorder := postChat(t, g, chatBody("fast", false))

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", recorder.Code)
	}
	if got := errorCode(t, recorder); got != policy.CodeUpstreamUnavailable {
		t.Fatalf("code = %s, want %s", got, policy.CodeUpstreamUnavailable)
	}
	if primary.calls.Load() != 1 {
		t.Fatalf("primary calls = %d, want 1", primary.calls.Load())
	}
	if backup.calls.Load() != 0 {
		t.Fatalf("backup calls = %d, want 0: a 500 means the provider may already have generated output", backup.calls.Load())
	}
	if g.metrics.replaySkipped.Load() != 1 {
		t.Fatalf("replaySkipped = %d, want 1", g.metrics.replaySkipped.Load())
	}
}

// A transport failure after the request bytes reached the wire is equally
// uncertain and must not be replayed.
func TestNoReplayAfterRequestWasSent(t *testing.T) {
	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("hijacking not supported")
			return
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_ = conn.Close()
	})
	backup := newUpstream(t, "backup", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"usage":{"prompt_tokens":1,"completion_tokens":2}}`))
	})
	g := newTestGateway(t, nil, primary, backup)

	recorder := postChat(t, g, chatBody("fast", false))

	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", recorder.Code)
	}
	if primary.calls.Load() != 1 {
		t.Fatalf("primary calls = %d, want 1", primary.calls.Load())
	}
	if backup.calls.Load() != 0 {
		t.Fatalf("backup calls = %d, want 0: bytes already reached the provider", backup.calls.Load())
	}
}

// A failure before the request reached the wire is safe to send elsewhere.
func TestSwitchProviderWhenRequestNeverSent(t *testing.T) {
	dead := newUpstream(t, "dead", func(http.ResponseWriter, *http.Request) {})
	dead.server.Close()
	backup := newUpstream(t, "backup", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"usage":{"prompt_tokens":3,"completion_tokens":4}}`))
	})
	g := newTestGateway(t, nil, dead, backup)

	recorder := postChat(t, g, chatBody("fast", false))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if backup.calls.Load() != 1 {
		t.Fatalf("backup calls = %d, want 1", backup.calls.Load())
	}
}

// 429 means the provider did not process it, so another deployment may.
func TestThrottledSwitchesProvider(t *testing.T) {
	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":"slow down"}`))
	})
	backup := newUpstream(t, "backup", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"usage":{"prompt_tokens":5,"completion_tokens":6}}`))
	})
	g := newTestGateway(t, nil, primary, backup)

	recorder := postChat(t, g, chatBody("fast", false))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", recorder.Code, recorder.Body.String())
	}
	if primary.calls.Load() != 1 || backup.calls.Load() != 1 {
		t.Fatalf("calls = primary %d backup %d, want 1 1", primary.calls.Load(), backup.calls.Load())
	}
}

func TestRetryAfterIsPropagatedWhenEveryDeploymentIsThrottled(t *testing.T) {
	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	g := newTestGateway(t, nil, primary)

	recorder := postChat(t, g, chatBody("fast", false))

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", recorder.Code)
	}
	if got := errorCode(t, recorder); got != policy.CodeUpstreamRateLimited {
		t.Fatalf("code = %s, want %s", got, policy.CodeUpstreamRateLimited)
	}
	if got := recorder.Header().Get("Retry-After"); got != "3" {
		t.Fatalf("Retry-After = %q, want 3", got)
	}
}

// Acceptance 2: every attempt shares one total deadline.
func TestTotalDeadlineIsSharedByAllAttempts(t *testing.T) {
	contract := policy.Default()
	contract.Timeouts.TotalDeadline = "1s"
	contract.Timeouts.AttemptTimeout = "900ms"
	contract.Timeouts.UpstreamConnect = "500ms"
	contract.Retry.MaxAttempts = 3
	contract.Retry.BackoffInitial = "50ms"
	contract.Retry.BackoffMax = "100ms"
	contract.Retry.MaxRetryAfter = "200ms"
	contract.Retry.Jitter = false

	slow := func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(700 * time.Millisecond)
		w.Header().Set("Retry-After", "1")
		w.WriteHeader(http.StatusTooManyRequests)
	}
	primary := newUpstream(t, "primary", slow)
	backup := newUpstream(t, "backup", slow)
	g := newTestGateway(t, contract, primary, backup)

	started := time.Now()
	recorder := postChat(t, g, chatBody("fast", false))
	elapsed := time.Since(started)

	if elapsed > 1200*time.Millisecond {
		t.Fatalf("elapsed = %s, want under 1.2s: the total deadline must not be reset per attempt", elapsed)
	}
	if backup.calls.Load() != 0 {
		t.Fatalf("backup calls = %d, want 0: too little deadline left to start another attempt", backup.calls.Load())
	}
	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", recorder.Code)
	}
}

// Acceptance 3: once response bytes are on the wire no other provider may take
// over, and the interruption is recorded instead of being counted as success.
func TestStreamIsNotHandedToAnotherProviderAfterWriting(t *testing.T) {
	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			return
		}
		_ = conn.Close()
	})
	backup := newUpstream(t, "backup", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"from-backup\"}}]}\n\ndata: [DONE]\n\n"))
	})
	g := newTestGateway(t, nil, primary, backup)

	recorder := postChat(t, g, chatBody("fast", true))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (headers already sent)", recorder.Code)
	}
	if backup.calls.Load() != 0 {
		t.Fatalf("backup calls = %d, want 0: a started stream must never switch provider", backup.calls.Load())
	}
	if !strings.Contains(recorder.Body.String(), "partial") {
		t.Fatalf("expected the partial chunk to be forwarded, got %q", recorder.Body.String())
	}
	if strings.Contains(recorder.Body.String(), "from-backup") {
		t.Fatal("response must not contain output from the backup provider")
	}
	if g.metrics.streamAborted.Load() != 1 {
		t.Fatalf("streamAborted = %d, want 1", g.metrics.streamAborted.Load())
	}
	if g.metrics.incomplete.Load() != 1 {
		t.Fatalf("incomplete = %d, want 1: an interrupted stream must not count as a success", g.metrics.incomplete.Load())
	}
}

func TestCompleteStreamCountsAsSuccess(t *testing.T) {
	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
		_, _ = w.Write([]byte("data: {\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":8}}\n\n"))
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	})
	g := newTestGateway(t, nil, primary)

	recorder := postChat(t, g, chatBody("fast", true))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if g.metrics.streamAborted.Load() != 0 {
		t.Fatalf("streamAborted = %d, want 0", g.metrics.streamAborted.Load())
	}
	if g.metrics.incomplete.Load() != 0 {
		t.Fatalf("incomplete = %d, want 0", g.metrics.incomplete.Load())
	}
}

func TestStreamWithoutTerminalEventIsNotSuccess(t *testing.T) {
	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n"))
	})
	g := newTestGateway(t, nil, primary)

	recorder := postChat(t, g, chatBody("fast", true))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", recorder.Code)
	}
	if g.metrics.incomplete.Load() != 1 {
		t.Fatalf("incomplete = %d, want 1", g.metrics.incomplete.Load())
	}
}

// Acceptance 4: hard memory limits on a single event and on the whole stream.
func TestOversizedSSEEventIsTruncated(t *testing.T) {
	contract := policy.Default()
	contract.Streaming.MaxEventBytes = 4096
	contract.Streaming.MaxStreamBytes = 1 << 20

	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: "))
		_, _ = w.Write([]byte(strings.Repeat("x", 2<<20)))
		_, _ = w.Write([]byte("\n\ndata: [DONE]\n\n"))
	})
	g := newTestGateway(t, contract, primary)

	recorder := postChat(t, g, chatBody("fast", true))

	if recorder.Body.Len() > 2<<20 {
		t.Fatalf("forwarded %d bytes, expected the oversized event to be cut off", recorder.Body.Len())
	}
	if g.metrics.streamAborted.Load() != 1 {
		t.Fatalf("streamAborted = %d, want 1", g.metrics.streamAborted.Load())
	}
}

func TestStreamTotalSizeLimit(t *testing.T) {
	contract := policy.Default()
	contract.Streaming.MaxEventBytes = 1024
	contract.Streaming.MaxStreamBytes = 8192

	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		chunk := "data: " + strings.Repeat("y", 500) + "\n\n"
		for i := 0; i < 100; i++ {
			if _, err := w.Write([]byte(chunk)); err != nil {
				return
			}
		}
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	})
	g := newTestGateway(t, contract, primary)

	recorder := postChat(t, g, chatBody("fast", true))

	if recorder.Body.Len() > 8192+4096 {
		t.Fatalf("forwarded %d bytes, expected the stream to stop at the size limit", recorder.Body.Len())
	}
	if g.metrics.streamAborted.Load() != 1 {
		t.Fatalf("streamAborted = %d, want 1", g.metrics.streamAborted.Load())
	}
}

// A provider that already produced a response must not be asked again, and the
// failure must not be reported as a successful request.
func TestIncompleteNonStreamResponseIsNotRetried(t *testing.T) {
	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Error("hijacking not supported")
			return
		}
		conn, buffer, err := hijacker.Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		_, _ = buffer.WriteString("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1000\r\n\r\n{\"partial\":true}")
		_ = buffer.Flush()
		_ = conn.Close()
	})
	backup := newUpstream(t, "backup", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	})
	g := newTestGateway(t, nil, primary, backup)

	recorder := postChat(t, g, chatBody("fast", false))

	if primary.calls.Load() != 1 || backup.calls.Load() != 0 {
		t.Fatalf("calls = primary %d backup %d, want 1 0", primary.calls.Load(), backup.calls.Load())
	}
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", recorder.Code)
	}
	if g.metrics.replaySkipped.Load() != 1 {
		t.Fatalf("replaySkipped = %d, want 1", g.metrics.replaySkipped.Load())
	}
}

// Client-side failures must not be blamed on the provider.
func TestClientSideFailureDoesNotOpenCircuit(t *testing.T) {
	g := newTestGateway(t, nil)
	target := &provider{name: "primary", breaker: newCircuitBreaker(2, time.Minute)}

	g.recordFailure(target, attemptResult{outcome: policy.OutcomeProcessed, fail: failClient})
	if target.breaker.failureCount() != 0 {
		t.Fatalf("failures = %d, want 0: a client disconnect is not a provider failure", target.breaker.failureCount())
	}
	g.recordFailure(target, attemptResult{outcome: policy.OutcomePartial, fail: failGateway})
	if target.breaker.failureCount() != 0 {
		t.Fatalf("failures = %d, want 0: a gateway deadline is not a provider failure", target.breaker.failureCount())
	}
	g.recordFailure(target, attemptResult{outcome: policy.OutcomeUnknown, fail: failUpstream})
	if target.breaker.failureCount() != 1 {
		t.Fatalf("failures = %d, want 1", target.breaker.failureCount())
	}
	g.recordFailure(target, attemptResult{outcome: policy.OutcomeThrottled})
	if target.breaker.failureCount() != 0 {
		t.Fatalf("failures = %d, want 0: a 429 means the provider answered", target.breaker.failureCount())
	}
}

func TestUnknownOutcomeReplayOnlyWhenExplicitlyEnabled(t *testing.T) {
	contract := policy.Default()
	contract.Retry.RetryOnUnknownOutcome = true
	contract.Fallback.AllowCrossProviderOnUnknown = true
	contract.Retry.BackoffInitial = "10ms"
	contract.Retry.BackoffMax = "20ms"

	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	backup := newUpstream(t, "backup", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	})
	g := newTestGateway(t, contract, primary, backup)

	recorder := postChat(t, g, chatBody("fast", false))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 when cross-provider retry is enabled: %s", recorder.Code, recorder.Body.String())
	}
	if primary.calls.Load() != 1 || backup.calls.Load() != 1 {
		t.Fatalf("calls = primary %d backup %d, want 1 1", primary.calls.Load(), backup.calls.Load())
	}
}

// Streams must ask for usage, otherwise token accounting is always zero.
func TestStreamUsageIsRequested(t *testing.T) {
	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	})
	g := newTestGateway(t, nil, primary)

	postChat(t, g, chatBody("fast", true))

	var payload map[string]any
	if err := json.Unmarshal([]byte(primary.requestBody(t)), &payload); err != nil {
		t.Fatalf("decode upstream body: %v", err)
	}
	options, ok := payload["stream_options"].(map[string]any)
	if !ok {
		t.Fatalf("stream_options missing from %v", payload)
	}
	if options["include_usage"] != true {
		t.Fatalf("include_usage = %v, want true", options["include_usage"])
	}
}

func TestStreamUsageNotInjectedForNonStreamRequests(t *testing.T) {
	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	})
	g := newTestGateway(t, nil, primary)

	postChat(t, g, chatBody("fast", false))

	var payload map[string]any
	if err := json.Unmarshal([]byte(primary.requestBody(t)), &payload); err != nil {
		t.Fatalf("decode upstream body: %v", err)
	}
	if _, ok := payload["stream_options"]; ok {
		t.Fatalf("stream_options must not be injected for non-stream requests: %v", payload)
	}
	if payload["model"] != "upstream-model" {
		t.Fatalf("model = %v, want upstream-model", payload["model"])
	}
}

func TestDrainingStopsNewTraffic(t *testing.T) {
	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	})
	g := newTestGateway(t, nil, primary)

	recorder := postChat(t, g, chatBody("fast", false))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status before drain = %d, want 200", recorder.Code)
	}

	g.Drain()
	recorder = postChat(t, g, chatBody("fast", false))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status after drain = %d, want 503", recorder.Code)
	}
	if got := errorCode(t, recorder); got != policy.CodeDraining {
		t.Fatalf("code = %s, want %s", got, policy.CodeDraining)
	}
}

func TestReadinessFailsWhileDrainingButLivenessDoesNot(t *testing.T) {
	g := newTestGateway(t, nil)
	handler := g.Handler()

	ready := httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusOK {
		t.Fatalf("readyz before drain = %d, want 200", ready.Code)
	}

	g.Drain()

	ready = httptest.NewRecorder()
	handler.ServeHTTP(ready, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if ready.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz during drain = %d, want 503", ready.Code)
	}

	// Liveness must never depend on upstream health, otherwise a provider
	// outage would restart every replica.
	for _, target := range g.providerList {
		target.breaker.failure()
		target.breaker.failure()
		target.breaker.failure()
	}
	live := httptest.NewRecorder()
	handler.ServeHTTP(live, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if live.Code != http.StatusOK {
		t.Fatalf("healthz = %d, want 200 even when every provider is failing", live.Code)
	}
}

func TestUpstreamsEndpointRequiresToken(t *testing.T) {
	primary := newUpstream(t, "primary", func(http.ResponseWriter, *http.Request) {})
	g := newTestGateway(t, nil, primary)
	handler := g.Handler()

	anonymous := httptest.NewRecorder()
	handler.ServeHTTP(anonymous, httptest.NewRequest(http.MethodGet, "/upstreams", nil))
	if anonymous.Code != http.StatusNotFound && anonymous.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated /upstreams = %d, want 404 or 401", anonymous.Code)
	}

	g.metricsToken = "metrics-secret"
	authorized := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/upstreams", nil)
	request.Header.Set("Authorization", "Bearer metrics-secret")
	handler.ServeHTTP(authorized, request)
	if authorized.Code != http.StatusOK {
		t.Fatalf("authorized /upstreams = %d, want 200", authorized.Code)
	}
	if !strings.Contains(authorized.Body.String(), "primary") {
		t.Fatalf("expected provider names in %q", authorized.Body.String())
	}
}

func TestParseRetryAfter(t *testing.T) {
	if got := parseRetryAfter("5"); got != 5*time.Second {
		t.Fatalf("parseRetryAfter(5) = %s, want 5s", got)
	}
	if got := parseRetryAfter(""); got != 0 {
		t.Fatalf("parseRetryAfter(\"\") = %s, want 0", got)
	}
	if got := parseRetryAfter("garbage"); got != 0 {
		t.Fatalf("parseRetryAfter(garbage) = %s, want 0", got)
	}
	moment := time.Now().Add(30 * time.Second).UTC().Format(http.TimeFormat)
	if got := parseRetryAfter(moment); got <= 0 || got > 31*time.Second {
		t.Fatalf("parseRetryAfter(date) = %s, want about 30s", got)
	}
}

func TestWaitBeforeRetryRespectsTotalDeadline(t *testing.T) {
	g := newTestGateway(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	if g.waitBeforeRetry(ctx, 1, 5*time.Second) {
		t.Fatal("expected the deadline to abort the retry wait")
	}
}

// ---------- Stage 2: shared quotas and cost accounting ----------

type countingStore struct {
	inner    *quota.MemoryStore
	reserved atomic.Int64
	settled  atomic.Int64
}

func (c *countingStore) Reserve(ctx context.Context, plan quota.Plan) (quota.Decision, error) {
	c.reserved.Add(1)
	return c.inner.Reserve(ctx, plan)
}
func (c *countingStore) Settle(ctx context.Context, s quota.Settlement) error {
	c.settled.Add(1)
	return c.inner.Settle(ctx, s)
}
func (c *countingStore) Abandon(ctx context.Context, id string) error {
	return c.inner.Abandon(ctx, id)
}
func (c *countingStore) Sweep(ctx context.Context, keys []string, now time.Time) (int, error) {
	return c.inner.Sweep(ctx, keys, now)
}
func (c *countingStore) Close() error { return nil }

type capturingSink struct {
	mu    sync.Mutex
	usage []audit.UsageEvent
	quota []audit.QuotaEvent
}

func (s *capturingSink) WriteUsage(_ context.Context, event audit.UsageEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage = append(s.usage, event)
	return nil
}
func (s *capturingSink) WriteQuota(_ context.Context, event audit.QuotaEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quota = append(s.quota, event)
	return nil
}
func (s *capturingSink) Close() error { return nil }

func (s *capturingSink) usageCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.usage)
}

func (s *capturingSink) usageSnapshot() []audit.UsageEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]audit.UsageEvent(nil), s.usage...)
}

// priceTableFor builds a versioned price table matching the test upstreams.
func priceTableFor(t *testing.T, providers ...string) *cost.Table {
	t.Helper()
	prices := make([]cost.Price, 0, len(providers))
	for _, name := range providers {
		prices = append(prices, cost.Price{
			Model: "upstream-model", Provider: name, Currency: "USD", Unit: cost.UnitToken,
			Version: "2026-01", InputRate: 1, OutputRate: 4,
		})
	}
	table, err := cost.NewTable(prices)
	if err != nil {
		t.Fatalf("price table: %v", err)
	}
	return table
}

// enableQuota wires a store, prices and a recorder onto a test gateway.
func enableQuota(t *testing.T, g *Gateway, shared quota.Store, recorder *audit.Recorder, upstreams ...string) {
	t.Helper()
	g.quota = quota.NewManager(shared, quota.NewMemoryStore(), "degrade", 0.5, nil)
	g.prices = priceTableFor(t, upstreams...)
	g.recorder = recorder
	g.reserveTTL = time.Minute
}

func setTenantQuota(g *Gateway, rpm int, tpm, budgetMicros int64) {
	g.clients["test"].rpm = rpm
	g.clients["test"].tpm = tpm
	g.clients["test"].budgetMicros = budgetMicros
}

func successUpstream(tokens int64) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(fmt.Sprintf(`{"usage":{"prompt_tokens":10,"completion_tokens":%d}}`, tokens)))
	}
}

// Acceptance: money is a dimension of its own. A tenant can be well inside its
// request rate and still be out of budget.
func TestBudgetExhaustionBlocksTheNextRequest(t *testing.T) {
	contract := policy.Default()
	contract.Quota.StreamReserveTokens = 64
	contract.Quota.Store.Mode = "local"
	primary := newUpstream(t, "primary", successUpstream(20))
	g := newTestGateway(t, contract, primary)
	enableQuota(t, g, quota.NewMemoryStore(), nil, "primary")
	setTenantQuota(g, 0, 0, 300)

	first := postChat(t, g, chatBody("fast", false))
	if first.Code != http.StatusOK {
		t.Fatalf("first request = %d, want 200: %s", first.Code, first.Body.String())
	}

	second := postChat(t, g, chatBody("fast", false))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second request = %d, want 429", second.Code)
	}
	if got := errorCode(t, second); got != policy.CodeBudgetExceeded {
		t.Fatalf("code = %s, want %s", got, policy.CodeBudgetExceeded)
	}
	if primary.calls.Load() != 1 {
		t.Fatalf("upstream calls = %d, want 1: a budget refusal must not reach the provider", primary.calls.Load())
	}
}

func TestRequestRateLimitIsSeparateFromBudget(t *testing.T) {
	contract := policy.Default()
	contract.Quota.Store.Mode = "local"
	primary := newUpstream(t, "primary", successUpstream(20))
	g := newTestGateway(t, contract, primary)
	enableQuota(t, g, quota.NewMemoryStore(), nil, "primary")
	setTenantQuota(g, 1, 0, 0)

	if got := postChat(t, g, chatBody("fast", false)); got.Code != http.StatusOK {
		t.Fatalf("first request = %d, want 200", got.Code)
	}
	second := postChat(t, g, chatBody("fast", false))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second request = %d, want 429", second.Code)
	}
	if got := errorCode(t, second); got != policy.CodeRateLimitExceeded {
		t.Fatalf("code = %s, want %s", got, policy.CodeRateLimitExceeded)
	}
}

// Acceptance: replicas share one tenant budget. Under process-local counters
// each replica would have handed out the full amount.
func TestReplicasShareOneTenantBudget(t *testing.T) {
	contract := policy.Default()
	contract.Quota.StreamReserveTokens = 64
	contract.Quota.Store.Mode = "local"
	shared := quota.NewMemoryStore()

	replicaAUpstream := newUpstream(t, "primary", successUpstream(20))
	replicaBUpstream := newUpstream(t, "primary", successUpstream(20))
	replicaA := newTestGateway(t, contract, replicaAUpstream)
	replicaB := newTestGateway(t, contract, replicaBUpstream)
	enableQuota(t, replicaA, shared, nil, "primary")
	enableQuota(t, replicaB, shared, nil, "primary")
	setTenantQuota(replicaA, 0, 0, 300)
	setTenantQuota(replicaB, 0, 0, 300)

	if got := postChat(t, replicaA, chatBody("fast", false)); got.Code != http.StatusOK {
		t.Fatalf("replica A = %d, want 200: %s", got.Code, got.Body.String())
	}
	second := postChat(t, replicaB, chatBody("fast", false))
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("replica B = %d, want 429: the budget must be shared, not per replica", second.Code)
	}
	if replicaBUpstream.calls.Load() != 0 {
		t.Fatalf("replica B upstream calls = %d, want 0", replicaBUpstream.calls.Load())
	}
}

// Acceptance: a completed request is traceable by tenant, model and provider
// with a real cost attached.
func TestUsageEventIsTraceableByTenantModelAndProvider(t *testing.T) {
	contract := policy.Default()
	contract.Quota.StreamReserveTokens = 64
	contract.Quota.Store.Mode = "local"
	primary := newUpstream(t, "primary", successUpstream(20))
	g := newTestGateway(t, contract, primary)
	sink := &capturingSink{}
	recorder := audit.NewRecorder([]audit.Sink{sink}, 16)
	recorder.Start()
	defer func() { _ = recorder.Stop() }()
	enableQuota(t, g, quota.NewMemoryStore(), recorder, "primary")
	setTenantQuota(g, 0, 0, 100000)

	if got := postChat(t, g, chatBody("fast", false)); got.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", got.Code, got.Body.String())
	}
	deadline := time.Now().Add(2 * time.Second)
	for sink.usageCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	events := sink.usageSnapshot()
	if len(events) != 1 {
		t.Fatalf("usage events = %d, want 1", len(events))
	}
	event := events[0]
	if event.Tenant != "test" || event.Model != "fast" || event.Provider != "primary" {
		t.Fatalf("event identity = tenant %q model %q provider %q", event.Tenant, event.Model, event.Provider)
	}
	if event.InputTokens != 10 || event.OutputTokens != 20 {
		t.Fatalf("usage = %d/%d, want 10/20", event.InputTokens, event.OutputTokens)
	}
	// 10 input at 1 micro and 20 output at 4 micros.
	if event.TotalMicros != 10+80 {
		t.Fatalf("total = %d, want 90 micros", event.TotalMicros)
	}
	if event.Reconciliation != string(cost.StatusOK) {
		t.Fatalf("reconciliation = %s, want ok", event.Reconciliation)
	}
	if !event.PriceFound || event.PriceVersion == "" {
		t.Fatalf("price found = %v version = %q, want true and a version", event.PriceFound, event.PriceVersion)
	}
}

// A request that fails with an uncertain outcome is charged exactly once, which
// is what stage 1's no-replay rule means in money terms.
func TestUncertainFailureIsChargedOnce(t *testing.T) {
	contract := policy.Default()
	contract.Quota.StreamReserveTokens = 64
	contract.Quota.Store.Mode = "local"
	primary := newUpstream(t, "primary", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	g := newTestGateway(t, contract, primary)
	store := &countingStore{inner: quota.NewMemoryStore()}
	enableQuota(t, g, store, nil, "primary")
	setTenantQuota(g, 0, 10000, 100000)

	recorder := postChat(t, g, chatBody("fast", false))
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", recorder.Code)
	}
	if store.reserved.Load() != 1 {
		t.Fatalf("reserve calls = %d, want 1", store.reserved.Load())
	}
	if store.settled.Load() > 1 {
		t.Fatalf("settle calls = %d, must not exceed 1", store.settled.Load())
	}
	if pending := store.inner.PendingIDs(); len(pending) != 0 {
		t.Fatalf("pending reservations = %v, want none after settling", pending)
	}
}
