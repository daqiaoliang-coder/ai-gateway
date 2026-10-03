package gateway

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"
)

type metrics struct {
	requests         atomic.Int64
	responses        atomic.Int64
	inflight         atomic.Int64
	clientErrors     atomic.Int64
	serverErrors     atomic.Int64
	upstreamAttempts atomic.Int64
	fallbacks        atomic.Int64
	upstreamErrors   atomic.Int64
	breakerSkips     atomic.Int64
	durationCount    atomic.Int64
	durationNanos    atomic.Int64
}

func (m *metrics) observe(status int, duration time.Duration) {
	m.responses.Add(1)
	m.durationCount.Add(1)
	m.durationNanos.Add(duration.Nanoseconds())
	if status >= 400 && status < 500 {
		m.clientErrors.Add(1)
	}
	if status >= 500 {
		m.serverErrors.Add(1)
	}
}

func (m *metrics) writePrometheus(w io.Writer) {
	count := m.durationCount.Load()
	sum := float64(m.durationNanos.Load()) / float64(time.Second)
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_requests_total Total requests received by the gateway.\n# TYPE ai_gateway_requests_total counter\nai_gateway_requests_total %d\n", m.requests.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_responses_total Requests completed with an HTTP response.\n# TYPE ai_gateway_responses_total counter\nai_gateway_responses_total %d\n", m.responses.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_requests_in_flight Requests currently being processed.\n# TYPE ai_gateway_requests_in_flight gauge\nai_gateway_requests_in_flight %d\n", m.inflight.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_client_errors_total Responses with a 4xx status.\n# TYPE ai_gateway_client_errors_total counter\nai_gateway_client_errors_total %d\n", m.clientErrors.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_server_errors_total Responses with a 5xx status.\n# TYPE ai_gateway_server_errors_total counter\nai_gateway_server_errors_total %d\n", m.serverErrors.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_upstream_attempts_total Attempts sent to providers.\n# TYPE ai_gateway_upstream_attempts_total counter\nai_gateway_upstream_attempts_total %d\n", m.upstreamAttempts.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_fallbacks_total Attempts after the first deployment.\n# TYPE ai_gateway_fallbacks_total counter\nai_gateway_fallbacks_total %d\n", m.fallbacks.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_upstream_errors_total Provider errors and transport failures.\n# TYPE ai_gateway_upstream_errors_total counter\nai_gateway_upstream_errors_total %d\n", m.upstreamErrors.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_circuit_breaker_skips_total Deployments skipped because their circuit is open.\n# TYPE ai_gateway_circuit_breaker_skips_total counter\nai_gateway_circuit_breaker_skips_total %d\n", m.breakerSkips.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_request_duration_seconds Request duration sum and count.\n# TYPE ai_gateway_request_duration_seconds summary\nai_gateway_request_duration_seconds_count %d\nai_gateway_request_duration_seconds_sum %.9f\n", count, sum)
}
