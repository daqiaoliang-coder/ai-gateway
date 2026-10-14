package policy

import (
	"errors"
	"os"
	"strings"
	"testing"
)

func writeFile(path, content string) error {
	return os.WriteFile(path, []byte(content), 0o600)
}

func TestDefaultContractIsValid(t *testing.T) {
	c := Default()
	if err := c.Validate(); err != nil {
		t.Fatalf("default contract must be valid: %v", err)
	}
	if c.ReplaysUnknownOutcome() {
		t.Fatal("default contract must not replay requests with unknown outcome")
	}
	if c.HasDurableAuditSink() {
		t.Fatal("default contract must not claim durability without a configured path")
	}
	if !c.Usage.EmitAuditEvents {
		t.Fatal("default contract must still emit usage events")
	}
	if c.Quota.Store.Mode != "shared" {
		t.Fatal("multi-replica sharing must be the default store mode")
	}
	if c.Quota.Store.OnUnavailable != "degrade" {
		t.Fatal("losing the shared store should degrade, not fail open or closed")
	}
	if !c.Streaming.RequireUsageInStream {
		t.Fatal("default contract must require usage reporting on streams")
	}
	if c.Streaming.MaxEventBytes > c.Streaming.MaxStreamBytes {
		t.Fatal("single SSE event limit must not exceed the whole stream limit")
	}
}

func TestDefaultContractFitsEntranceTimeout(t *testing.T) {
	c := Default()
	if c.Timeouts.TotalDeadline != "45s" || c.Entrance.Timeout != "60s" {
		t.Fatalf("unexpected defaults: total=%s entrance=%s", c.Timeouts.TotalDeadline, c.Entrance.Timeout)
	}
}

func TestValidateRejectsInvalidContracts(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Contract)
		wantErr string
	}{
		{
			name:    "total deadline longer than entrance timeout",
			mutate:  func(c *Contract) { c.Timeouts.TotalDeadline = "90s" },
			wantErr: "must fit inside entrance.timeout",
		},
		{
			name:    "total deadline plus margin equal to entrance timeout is allowed",
			mutate:  func(c *Contract) { c.Timeouts.TotalDeadline = "55s" },
			wantErr: "",
		},
		{
			name:    "attempt timeout longer than total deadline",
			mutate:  func(c *Contract) { c.Timeouts.AttemptTimeout = "60s" },
			wantErr: "must not exceed timeouts.total_deadline",
		},
		{
			name:    "connect timeout longer than attempt timeout",
			mutate:  func(c *Contract) { c.Timeouts.UpstreamConnect = "30s" },
			wantErr: "must not exceed timeouts.attempt_timeout",
		},
		{
			name:    "zero total deadline",
			mutate:  func(c *Contract) { c.Timeouts.TotalDeadline = "0s" },
			wantErr: "must be positive",
		},
		{
			name:    "too many attempts",
			mutate:  func(c *Contract) { c.Retry.MaxAttempts = 9 },
			wantErr: "retry.max_attempts",
		},
		{
			name:    "backoff initial above max",
			mutate:  func(c *Contract) { c.Retry.BackoffInitial = "5s" },
			wantErr: "must not exceed retry.backoff_max",
		},
		{
			name:    "backoff multiplier below one",
			mutate:  func(c *Contract) { c.Retry.BackoffMultiplier = 0.5 },
			wantErr: "retry.backoff_multiplier",
		},
		{
			name:    "retry-after longer than total deadline",
			mutate:  func(c *Contract) { c.Retry.MaxRetryAfter = "120s" },
			wantErr: "must not exceed timeouts.total_deadline",
		},
		{
			name:    "unknown outcome replay without cross provider fallback",
			mutate:  func(c *Contract) { c.Retry.RetryOnUnknownOutcome = true },
			wantErr: "requires fallback.allow_cross_provider_on_unknown_outcome",
		},
		{
			name: "alias name treated as compatibility",
			mutate: func(c *Contract) {
				c.Fallback.AllowAliasNameAsCompatibility = true
			},
			wantErr: "must be false",
		},
		{
			name:    "unknown tenant key",
			mutate:  func(c *Contract) { c.Quota.TenantKey = "email" },
			wantErr: "quota.tenant_key",
		},
		{
			name:    "unknown budget window",
			mutate:  func(c *Contract) { c.Quota.BudgetWindow = "forever" },
			wantErr: "quota.budget_window",
		},
		{
			name:    "lowercase currency",
			mutate:  func(c *Contract) { c.Quota.Currency = "usd" },
			wantErr: "ISO 4217",
		},
		{
			name:    "shared store without degradation policy",
			mutate:  func(c *Contract) { c.Quota.Store.OnUnavailable = "" },
			wantErr: "quota.store.on_unavailable",
		},
		{
			name:    "degrade factor out of range",
			mutate:  func(c *Contract) { c.Quota.Store.DegradeFactor = 1.5 },
			wantErr: "degrade_factor",
		},
		{
			name:    "reservation ttl too long",
			mutate:  func(c *Contract) { c.Quota.Store.ReservationTTL = "48h" },
			wantErr: "must not exceed 24h",
		},
		{
			name:    "zero stream reserve",
			mutate:  func(c *Contract) { c.Quota.StreamReserveTokens = 0 },
			wantErr: "stream_reserve_tokens",
		},
		{
			name:    "event limit larger than stream limit",
			mutate:  func(c *Contract) { c.Streaming.MaxEventBytes = 32 << 20 },
			wantErr: "must not exceed streaming.max_stream_bytes",
		},
		{
			name:    "readiness does not fail while draining",
			mutate:  func(c *Contract) { c.Drain.ReadinessFailsOnDrain = false },
			wantErr: "readiness_fails_on_drain",
		},
		{
			name:    "liveness depends on upstream",
			mutate:  func(c *Contract) { c.Drain.LivenessDependsOnUpstream = true },
			wantErr: "liveness_depends_on_upstream",
		},
		{
			name:    "unknown missing usage policy",
			mutate:  func(c *Contract) { c.Usage.MissingUsagePolicy = "guess" },
			wantErr: "missing_usage_policy",
		},
		{
			name:    "passthrough status outside 4xx and 5xx",
			mutate:  func(c *Contract) { c.ErrorCodes.PassthroughStatuses = []int{200} },
			wantErr: "passthrough_statuses",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Default()
			tt.mutate(c)
			err := c.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("expected valid contract, got %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got none", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tt.wantErr, err)
			}
		})
	}
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name   string
		phase  Phase
		status int
		err    error
		want   Outcome
	}{
		{"dns failure before send", PhasePreSend, 0, errors.New("no such host"), OutcomeNotSent},
		{"connection refused before send", PhasePreSend, 0, errors.New("connect: refused"), OutcomeNotSent},
		{"timeout while awaiting status", PhaseAwaitingStatus, 0, errors.New("context deadline"), OutcomeUnknown},
		{"reset while awaiting status", PhaseAwaitingStatus, 0, errors.New("connection reset"), OutcomeUnknown},
		{"upstream 429", PhaseAwaitingStatus, 429, nil, OutcomeThrottled},
		{"upstream 408", PhaseAwaitingStatus, 408, nil, OutcomeThrottled},
		{"upstream 400", PhaseAwaitingStatus, 400, nil, OutcomeRejected},
		{"upstream 401", PhaseAwaitingStatus, 401, nil, OutcomeRejected},
		{"upstream 500", PhaseAwaitingStatus, 500, nil, OutcomeUnknown},
		{"upstream 503", PhaseAwaitingStatus, 503, nil, OutcomeUnknown},
		{"complete 200", PhaseBodyForwarding, 200, nil, OutcomeProcessed},
		{"body read failure after 200", PhaseBodyForwarding, 200, errors.New("unexpected EOF"), OutcomePartial},
		{"client write failure during stream", PhaseBodyForwarding, 200, errors.New("broken pipe"), OutcomePartial},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Classify(tt.phase, tt.status, tt.err); got != tt.want {
				t.Fatalf("Classify(%s, %d, %v) = %s, want %s", tt.phase, tt.status, tt.err, got, tt.want)
			}
		})
	}
}

func TestDecideDefaultNeverReplaysUncertainRequests(t *testing.T) {
	c := Default()

	tests := []struct {
		outcome      Outcome
		wantReplay   bool
		wantSwitch   bool
		wantTerminal bool
	}{
		{OutcomeNotSent, true, true, false},
		{OutcomeThrottled, false, true, false},
		{OutcomeRejected, false, false, true},
		{OutcomeUnknown, false, false, true},
		{OutcomeProcessed, false, false, true},
		{OutcomePartial, false, false, true},
	}

	for _, tt := range tests {
		t.Run(tt.outcome.String(), func(t *testing.T) {
			got := c.Decide(tt.outcome)
			if got.ReplaySame != tt.wantReplay || got.SwitchProvider != tt.wantSwitch || got.Terminal != tt.wantTerminal {
				t.Fatalf("Decide(%s) = %+v, want replay=%v switch=%v terminal=%v",
					tt.outcome, got, tt.wantReplay, tt.wantSwitch, tt.wantTerminal)
			}
		})
	}
}

func TestDecideUnknownOutcomeWhenExplicitlyEnabled(t *testing.T) {
	c := Default()
	c.Retry.RetryOnUnknownOutcome = true
	c.Fallback.AllowCrossProviderOnUnknown = true
	if err := c.Validate(); err != nil {
		t.Fatalf("contract should be valid: %v", err)
	}
	got := c.Decide(OutcomeUnknown)
	if got.Terminal {
		t.Fatal("unknown outcome should be switchable when cross-provider retry is enabled")
	}
	if got.ReplaySame {
		t.Fatal("unknown outcome must never be replayed against the same deployment")
	}
	if !got.SwitchProvider {
		t.Fatal("unknown outcome should allow switching provider when enabled")
	}
}

func TestStreamCompletionSuccessAccounting(t *testing.T) {
	tests := []struct {
		completion StreamCompletion
		success    bool
	}{
		{StreamCompleted, true},
		{StreamAbortedClient, false},
		{StreamAbortedUpstream, false},
		{StreamAbortedGateway, false},
		{StreamTruncated, false},
	}
	for _, tt := range tests {
		t.Run(string(tt.completion), func(t *testing.T) {
			if got := tt.completion.CountsAsSuccess(); got != tt.success {
				t.Fatalf("%s.CountsAsSuccess() = %v, want %v", tt.completion, got, tt.success)
			}
		})
	}
}

func TestErrorCodeForOutcome(t *testing.T) {
	tests := []struct {
		outcome Outcome
		want    string
	}{
		{OutcomeThrottled, CodeUpstreamRateLimited},
		{OutcomeRejected, CodeUpstreamError},
		{OutcomeUnknown, CodeUpstreamUnavailable},
		{OutcomePartial, CodeStreamAborted},
	}
	for _, tt := range tests {
		t.Run(tt.outcome.String(), func(t *testing.T) {
			if got := ErrorCodeFor(tt.outcome); got != tt.want {
				t.Fatalf("ErrorCodeFor(%s) = %s, want %s", tt.outcome, got, tt.want)
			}
		})
	}
}

func TestOutcomeLabels(t *testing.T) {
	tests := []struct {
		outcome Outcome
		want    string
	}{
		{OutcomeNotSent, "not_sent"},
		{OutcomeThrottled, "throttled"},
		{OutcomeRejected, "rejected"},
		{OutcomeUnknown, "unknown"},
		{OutcomeProcessed, "processed"},
		{OutcomePartial, "partial"},
		{OutcomeNotAttempted, "not_attempted"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.outcome.String(); got != tt.want {
				t.Fatalf("String() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestGatewayErrorCodesAreClosedSet(t *testing.T) {
	for _, code := range GatewayErrorCodes {
		if !IsGatewayErrorCode(code) {
			t.Fatalf("%s should be a gateway error code", code)
		}
	}
	for _, code := range []string{"", "upstream_bad_gateway", "model_not_allowed ", "E_TIMEOUT"} {
		if IsGatewayErrorCode(code) {
			t.Fatalf("%q must not be a gateway error code", code)
		}
	}
}

func TestLoadFromFile(t *testing.T) {
	path := t.TempDir() + "/policy.json"
	raw := `{
      "version": 1,
      "entrance": {"timeout": "60s", "safety_margin": "5s"},
      "timeouts": {"total_deadline": "45s", "attempt_timeout": "20s", "upstream_connect_timeout": "5s"},
      "retry": {"max_attempts": 2, "backoff_initial": "200ms", "backoff_max": "2s", "backoff_multiplier": 2, "jitter": true, "respect_retry_after": true, "max_retry_after": "10s"},
      "fallback": {"require_compatibility_group": true},
      "quota": {"tenant_key": "client_id", "budget_window": "calendar_month", "currency": "USD",
                "store": {"mode": "shared", "on_unavailable": "degrade", "degrade_factor": 0.5, "reservation_ttl": "10m"},
                "stream_reserve_tokens": 4096, "stream_overage": "abort"},
      "usage": {"missing_usage_policy": "estimate", "price_version_required": true, "emit_audit_events": true, "audit_sink": "both", "audit_file_path": "/tmp/ai-gateway-audit.jsonl"},
      "streaming": {"max_event_bytes": 262144, "max_stream_bytes": 16777216, "require_usage_in_stream": true, "abort_on_client_gone": true},
      "drain": {"timeout": "30s", "readiness_fails_on_drain": true, "liveness_depends_on_upstream": false},
      "observability": {"max_label_cardinality": 200, "allowed_labels": ["tenant", "model", "provider"]},
      "error_codes": {"passthrough_statuses": [400, 401, 422]}
    }`
	if err := writeFile(path, raw); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	c, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Retry.MaxAttempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", c.Retry.MaxAttempts)
	}
	if len(c.ErrorCodes.PassthroughStatuses) != 3 {
		t.Fatalf("expected 3 passthrough statuses, got %v", c.ErrorCodes.PassthroughStatuses)
	}
}

func TestShippedExampleIsValid(t *testing.T) {
	c, err := Load("../../configs/policy.example.json")
	if err != nil {
		t.Fatalf("configs/policy.example.json must be a valid contract: %v", err)
	}
	if c.ReplaysUnknownOutcome() {
		t.Fatal("shipped example must not replay requests with unknown outcome")
	}
	if !c.Fallback.RequireCompatibilityGroup {
		t.Fatal("shipped example must require compatibility groups")
	}
	if !c.HasDurableAuditSink() {
		t.Fatal("shipped example must persist usage events durably")
	}
}

func TestLoadRejectsUnknownFields(t *testing.T) {
	path := t.TempDir() + "/policy.json"
	if err := writeFile(path, `{"version": 1, "surprise": true}`); err != nil {
		t.Fatalf("write fixture: %v", err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected unknown field to be rejected")
	}
}
