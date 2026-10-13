package gateway

import (
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/daqiaoliang-coder/ai-gateway/internal/policy"
)

type metrics struct {
	requests         atomic.Int64
	responses        atomic.Int64
	inflight         atomic.Int64
	clientErrors     atomic.Int64
	serverErrors     atomic.Int64
	incomplete       atomic.Int64
	upstreamAttempts atomic.Int64
	retries          atomic.Int64
	replaySkipped    atomic.Int64
	upstreamErrors   atomic.Int64
	streamAborted    atomic.Int64
	breakerSkips     atomic.Int64
	durationCount    atomic.Int64
	durationNanos    atomic.Int64
}

// observe records one completed request. A response only counts as successful
// when its status is below 400 and, for streams, the stream actually reached
// its terminal event.
func (m *metrics) observe(status int, duration time.Duration, completion policy.StreamCompletion) {
	m.responses.Add(1)
	m.durationCount.Add(1)
	m.durationNanos.Add(duration.Nanoseconds())
	if status >= 400 && status < 500 {
		m.clientErrors.Add(1)
	}
	if status >= 500 {
		m.serverErrors.Add(1)
	}
	if status < 400 && !completion.CountsAsSuccess() {
		m.incomplete.Add(1)
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
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_incomplete_responses_total Responses with a 2xx status that did not complete.\n# TYPE ai_gateway_incomplete_responses_total counter\nai_gateway_incomplete_responses_total %d\n", m.incomplete.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_upstream_attempts_total Attempts sent to providers.\n# TYPE ai_gateway_upstream_attempts_total counter\nai_gateway_upstream_attempts_total %d\n", m.upstreamAttempts.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_retries_total Attempts after the first one.\n# TYPE ai_gateway_retries_total counter\nai_gateway_retries_total %d\n", m.retries.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_replay_skipped_total Failures not retried because the upstream outcome was uncertain.\n# TYPE ai_gateway_replay_skipped_total counter\nai_gateway_replay_skipped_total %d\n", m.replaySkipped.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_upstream_errors_total Provider errors and transport failures.\n# TYPE ai_gateway_upstream_errors_total counter\nai_gateway_upstream_errors_total %d\n", m.upstreamErrors.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_stream_aborted_total Streamed responses that did not reach their terminal event.\n# TYPE ai_gateway_stream_aborted_total counter\nai_gateway_stream_aborted_total %d\n", m.streamAborted.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_circuit_breaker_skips_total Deployments skipped because their circuit is open.\n# TYPE ai_gateway_circuit_breaker_skips_total counter\nai_gateway_circuit_breaker_skips_total %d\n", m.breakerSkips.Load())
	_, _ = fmt.Fprintf(w, "# HELP ai_gateway_request_duration_seconds Request duration sum and count.\n# TYPE ai_gateway_request_duration_seconds summary\nai_gateway_request_duration_seconds_count %d\nai_gateway_request_duration_seconds_sum %.9f\n", count, sum)
}
