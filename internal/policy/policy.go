// Package policy encodes the gateway behavior contract.
//
// It is the single source of truth for retry, fallback, quota, budget, error
// code, timeout, stream abort and drain semantics. Later stages assert against
// this package instead of re-deriving rules inside each request path, so the
// documented rules and the shipped behavior cannot drift apart.
package policy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"
)

// Contract is the complete behavior contract. Zero values are filled by
// defaults before validation.
type Contract struct {
	Version       int            `json:"version"`
	Entrance      Entrance       `json:"entrance"`
	Timeouts      Timeouts       `json:"timeouts"`
	Retry         Retry          `json:"retry"`
	Fallback      Fallback       `json:"fallback"`
	Quota         Quota          `json:"quota"`
	Usage         Usage          `json:"usage"`
	Streaming     Streaming      `json:"streaming"`
	Drain         Drain          `json:"drain"`
	Observability Observability  `json:"observability"`
	ErrorCodes    ErrorCodeRules `json:"error_codes"`
}

// Entrance describes the hop in front of the gateway. The gateway deadline must
// fit inside it, otherwise retries outlive the client or the load balancer.
type Entrance struct {
	Timeout      string `json:"timeout"`
	SafetyMargin string `json:"safety_margin"`
}

// Timeouts splits the total request budget from the per-attempt budget.
type Timeouts struct {
	TotalDeadline   string `json:"total_deadline"`
	AttemptTimeout  string `json:"attempt_timeout"`
	UpstreamConnect string `json:"upstream_connect_timeout"`
}

// Retry controls bounded retries. The contract never replays a request whose
// upstream outcome is unknown unless RetryOnUnknownOutcome is explicitly on.
type Retry struct {
	MaxAttempts           int     `json:"max_attempts"`
	RetryOnUnknownOutcome bool    `json:"retry_on_unknown_outcome"`
	BackoffInitial        string  `json:"backoff_initial"`
	BackoffMax            string  `json:"backoff_max"`
	BackoffMultiplier     float64 `json:"backoff_multiplier"`
	Jitter                bool    `json:"jitter"`
	RespectRetryAfter     bool    `json:"respect_retry_after"`
	MaxRetryAfter         string  `json:"max_retry_after"`
}

// Fallback controls cross-provider switching.
type Fallback struct {
	RequireCompatibilityGroup     bool `json:"require_compatibility_group"`
	AllowCrossProviderOnUnknown   bool `json:"allow_cross_provider_on_unknown_outcome"`
	AllowAliasNameAsCompatibility bool `json:"allow_alias_name_as_compatibility"`
}

// Quota separates request rate limits from token and money budgets.
type Quota struct {
	TenantKey           string `json:"tenant_key"`
	BudgetWindow        string `json:"budget_window"`
	Currency            string `json:"currency"`
	Store               Store  `json:"store"`
	StreamReserveTokens int64  `json:"stream_reserve_tokens"`
	StreamOverage       string `json:"stream_overage"`
}

// Store describes where shared quota state lives and how the gateway degrades
// when that store is unreachable.
type Store struct {
	Mode           string  `json:"mode"`
	OnUnavailable  string  `json:"on_unavailable"`
	DegradeFactor  float64 `json:"degrade_factor"`
	ReservationTTL string  `json:"reservation_ttl"`
}

// Usage controls token and cost accounting.
type Usage struct {
	MissingUsagePolicy   string `json:"missing_usage_policy"`
	PriceVersionRequired bool   `json:"price_version_required"`
	EmitAuditEvents      bool   `json:"emit_audit_events"`
	AuditSink            string `json:"audit_sink"`
}

// Streaming bounds a single SSE event and the whole streamed response, and
// requires usage reporting on streams.
type Streaming struct {
	MaxEventBytes        int64 `json:"max_event_bytes"`
	MaxStreamBytes       int64 `json:"max_stream_bytes"`
	RequireUsageInStream bool  `json:"require_usage_in_stream"`
	AbortOnClientGone    bool  `json:"abort_on_client_gone"`
}

// Drain describes shutdown behavior for rolling updates.
type Drain struct {
	Timeout                   string `json:"timeout"`
	ReadinessFailsOnDrain     bool   `json:"readiness_fails_on_drain"`
	LivenessDependsOnUpstream bool   `json:"liveness_depends_on_upstream"`
}

// Observability bounds metric labels and audit emission.
type Observability struct {
	MaxLabelCardinality int      `json:"max_label_cardinality"`
	AllowedLabels       []string `json:"allowed_labels"`
}

// ErrorCodeRules defines the single error envelope returned to clients.
type ErrorCodeRules struct {
	PassthroughStatuses []int `json:"passthrough_statuses"`
}

// Enum values accepted by the contract.
var (
	tenantKeys     = []string{"client_id", "api_key_id"}
	budgetWindows  = []string{"calendar_month", "rolling_30d", "rolling_24h"}
	storeModes     = []string{"shared", "local"}
	storeFailModes = []string{"fail_closed", "fail_open", "degrade"}
	streamOverages = []string{"abort", "allow", "truncate"}
	missingUsage   = []string{"estimate", "reject", "allow_zero"}
	auditSinks     = []string{"stdout", "durable", "both"}
)

// Default returns the production-default contract.
func Default() *Contract {
	c := &Contract{}
	c.defaults()
	return c
}

func (c *Contract) defaults() {
	if c.Version == 0 {
		c.Version = 1
	}
	if c.Entrance.Timeout == "" {
		c.Entrance.Timeout = "60s"
	}
	if c.Entrance.SafetyMargin == "" {
		c.Entrance.SafetyMargin = "5s"
	}
	if c.Timeouts.TotalDeadline == "" {
		c.Timeouts.TotalDeadline = "45s"
	}
	if c.Timeouts.AttemptTimeout == "" {
		c.Timeouts.AttemptTimeout = "20s"
	}
	if c.Timeouts.UpstreamConnect == "" {
		c.Timeouts.UpstreamConnect = "5s"
	}
	if c.Retry.MaxAttempts == 0 {
		c.Retry.MaxAttempts = 3
	}
	if c.Retry.BackoffInitial == "" {
		c.Retry.BackoffInitial = "200ms"
	}
	if c.Retry.BackoffMax == "" {
		c.Retry.BackoffMax = "2s"
	}
	if c.Retry.BackoffMultiplier == 0 {
		c.Retry.BackoffMultiplier = 2
	}
	if c.Retry.MaxRetryAfter == "" {
		c.Retry.MaxRetryAfter = "10s"
	}
	if !c.Retry.Jitter {
		c.Retry.Jitter = true
	}
	if !c.Retry.RespectRetryAfter {
		c.Retry.RespectRetryAfter = true
	}
	if !c.Fallback.RequireCompatibilityGroup {
		c.Fallback.RequireCompatibilityGroup = true
	}
	if c.Quota.TenantKey == "" {
		c.Quota.TenantKey = "client_id"
	}
	if c.Quota.BudgetWindow == "" {
		c.Quota.BudgetWindow = "calendar_month"
	}
	if c.Quota.Currency == "" {
		c.Quota.Currency = "USD"
	}
	if c.Quota.Store.Mode == "" {
		c.Quota.Store.Mode = "shared"
	}
	if c.Quota.Store.OnUnavailable == "" {
		c.Quota.Store.OnUnavailable = "degrade"
	}
	if c.Quota.Store.DegradeFactor == 0 {
		c.Quota.Store.DegradeFactor = 0.5
	}
	if c.Quota.Store.ReservationTTL == "" {
		c.Quota.Store.ReservationTTL = "10m"
	}
	if c.Quota.StreamReserveTokens == 0 {
		c.Quota.StreamReserveTokens = 4096
	}
	if c.Quota.StreamOverage == "" {
		c.Quota.StreamOverage = "abort"
	}
	if c.Usage.MissingUsagePolicy == "" {
		c.Usage.MissingUsagePolicy = "estimate"
	}
	if !c.Usage.PriceVersionRequired {
		c.Usage.PriceVersionRequired = true
	}
	if !c.Usage.EmitAuditEvents {
		c.Usage.EmitAuditEvents = true
	}
	if c.Usage.AuditSink == "" {
		c.Usage.AuditSink = "both"
	}
	if c.Streaming.MaxEventBytes == 0 {
		c.Streaming.MaxEventBytes = 256 << 10
	}
	if c.Streaming.MaxStreamBytes == 0 {
		c.Streaming.MaxStreamBytes = 16 << 20
	}
	if !c.Streaming.RequireUsageInStream {
		c.Streaming.RequireUsageInStream = true
	}
	if !c.Streaming.AbortOnClientGone {
		c.Streaming.AbortOnClientGone = true
	}
	if c.Drain.Timeout == "" {
		c.Drain.Timeout = "30s"
	}
	if !c.Drain.ReadinessFailsOnDrain {
		c.Drain.ReadinessFailsOnDrain = true
	}
	if c.Observability.MaxLabelCardinality == 0 {
		c.Observability.MaxLabelCardinality = 200
	}
	if len(c.Observability.AllowedLabels) == 0 {
		c.Observability.AllowedLabels = []string{"tenant", "client", "model", "provider", "route", "status_class", "outcome", "error_code"}
	}
}

// Load reads a contract JSON file and validates it.
func Load(path string) (*Contract, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open policy: %w", err)
	}
	defer f.Close()

	var c Contract
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&c); err != nil {
		return nil, fmt.Errorf("decode policy: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("policy must contain exactly one JSON object")
	}
	c.defaults()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate checks every cross-field invariant of the contract.
func (c *Contract) Validate() error {
	if c.Version != 1 {
		return fmt.Errorf("version must be 1, got %d", c.Version)
	}

	entrance, err := parsePositiveDuration(c.Entrance.Timeout)
	if err != nil {
		return fmt.Errorf("entrance.timeout: %w", err)
	}
	margin, err := parseNonNegativeDuration(c.Entrance.SafetyMargin)
	if err != nil {
		return fmt.Errorf("entrance.safety_margin: %w", err)
	}
	total, err := parsePositiveDuration(c.Timeouts.TotalDeadline)
	if err != nil {
		return fmt.Errorf("timeouts.total_deadline: %w", err)
	}
	attempt, err := parsePositiveDuration(c.Timeouts.AttemptTimeout)
	if err != nil {
		return fmt.Errorf("timeouts.attempt_timeout: %w", err)
	}
	connect, err := parsePositiveDuration(c.Timeouts.UpstreamConnect)
	if err != nil {
		return fmt.Errorf("timeouts.upstream_connect_timeout: %w", err)
	}
	if total+margin > entrance {
		return fmt.Errorf("timeouts.total_deadline (%s) plus entrance.safety_margin (%s) must fit inside entrance.timeout (%s)", total, margin, entrance)
	}
	if attempt > total {
		return fmt.Errorf("timeouts.attempt_timeout (%s) must not exceed timeouts.total_deadline (%s)", attempt, total)
	}
	if connect > attempt {
		return fmt.Errorf("timeouts.upstream_connect_timeout (%s) must not exceed timeouts.attempt_timeout (%s)", connect, attempt)
	}

	if c.Retry.MaxAttempts < 1 || c.Retry.MaxAttempts > 5 {
		return fmt.Errorf("retry.max_attempts must be between 1 and 5, got %d", c.Retry.MaxAttempts)
	}
	initial, err := parsePositiveDuration(c.Retry.BackoffInitial)
	if err != nil {
		return fmt.Errorf("retry.backoff_initial: %w", err)
	}
	backoffMax, err := parsePositiveDuration(c.Retry.BackoffMax)
	if err != nil {
		return fmt.Errorf("retry.backoff_max: %w", err)
	}
	if initial > backoffMax {
		return fmt.Errorf("retry.backoff_initial (%s) must not exceed retry.backoff_max (%s)", initial, backoffMax)
	}
	if c.Retry.BackoffMultiplier < 1 {
		return fmt.Errorf("retry.backoff_multiplier must be >= 1, got %v", c.Retry.BackoffMultiplier)
	}
	maxRetryAfter, err := parsePositiveDuration(c.Retry.MaxRetryAfter)
	if err != nil {
		return fmt.Errorf("retry.max_retry_after: %w", err)
	}
	if maxRetryAfter > total {
		return fmt.Errorf("retry.max_retry_after (%s) must not exceed timeouts.total_deadline (%s)", maxRetryAfter, total)
	}

	// Replaying a request whose upstream outcome is unknown is only acceptable
	// as a cross-provider switch. Replaying it against the same provider is the
	// duplicate-generation hazard this contract exists to prevent.
	if c.Retry.RetryOnUnknownOutcome && !c.Fallback.AllowCrossProviderOnUnknown {
		return errors.New("retry.retry_on_unknown_outcome requires fallback.allow_cross_provider_on_unknown_outcome to be true")
	}
	if c.Fallback.AllowAliasNameAsCompatibility {
		return errors.New("fallback.allow_alias_name_as_compatibility must be false: identical model aliases do not imply capability or data-region compatibility")
	}

	if !contains(tenantKeys, c.Quota.TenantKey) {
		return fmt.Errorf("quota.tenant_key must be one of %v, got %q", tenantKeys, c.Quota.TenantKey)
	}
	if !contains(budgetWindows, c.Quota.BudgetWindow) {
		return fmt.Errorf("quota.budget_window must be one of %v, got %q", budgetWindows, c.Quota.BudgetWindow)
	}
	if !isCurrencyCode(c.Quota.Currency) {
		return fmt.Errorf("quota.currency must be a 3-letter ISO 4217 code, got %q", c.Quota.Currency)
	}
	if !contains(storeModes, c.Quota.Store.Mode) {
		return fmt.Errorf("quota.store.mode must be one of %v, got %q", storeModes, c.Quota.Store.Mode)
	}
	if c.Quota.Store.Mode == "shared" {
		if !contains(storeFailModes, c.Quota.Store.OnUnavailable) {
			return fmt.Errorf("quota.store.on_unavailable must be one of %v, got %q", storeFailModes, c.Quota.Store.OnUnavailable)
		}
		if c.Quota.Store.OnUnavailable == "degrade" {
			if c.Quota.Store.DegradeFactor <= 0 || c.Quota.Store.DegradeFactor > 1 {
				return fmt.Errorf("quota.store.degrade_factor must be in (0, 1], got %v", c.Quota.Store.DegradeFactor)
			}
		}
	}
	ttl, err := parsePositiveDuration(c.Quota.Store.ReservationTTL)
	if err != nil {
		return fmt.Errorf("quota.store.reservation_ttl: %w", err)
	}
	if ttl > 24*time.Hour {
		return fmt.Errorf("quota.store.reservation_ttl (%s) must not exceed 24h", ttl)
	}
	if c.Quota.StreamReserveTokens <= 0 {
		return errors.New("quota.stream_reserve_tokens must be positive")
	}
	if !contains(streamOverages, c.Quota.StreamOverage) {
		return fmt.Errorf("quota.stream_overage must be one of %v, got %q", streamOverages, c.Quota.StreamOverage)
	}

	if !contains(missingUsage, c.Usage.MissingUsagePolicy) {
		return fmt.Errorf("usage.missing_usage_policy must be one of %v, got %q", missingUsage, c.Usage.MissingUsagePolicy)
	}
	if !contains(auditSinks, c.Usage.AuditSink) {
		return fmt.Errorf("usage.audit_sink must be one of %v, got %q", auditSinks, c.Usage.AuditSink)
	}

	if c.Streaming.MaxEventBytes <= 0 || c.Streaming.MaxStreamBytes <= 0 {
		return errors.New("streaming.max_event_bytes and streaming.max_stream_bytes must be positive")
	}
	if c.Streaming.MaxEventBytes > c.Streaming.MaxStreamBytes {
		return fmt.Errorf("streaming.max_event_bytes (%d) must not exceed streaming.max_stream_bytes (%d)", c.Streaming.MaxEventBytes, c.Streaming.MaxStreamBytes)
	}

	drain, err := parsePositiveDuration(c.Drain.Timeout)
	if err != nil {
		return fmt.Errorf("drain.timeout: %w", err)
	}
	if drain > 10*time.Minute {
		return fmt.Errorf("drain.timeout (%s) must not exceed 10m", drain)
	}
	if !c.Drain.ReadinessFailsOnDrain {
		return errors.New("drain.readiness_fails_on_drain must be true: an instance that is shutting down must stop receiving new traffic")
	}
	// A provider outage must never restart the whole fleet.
	if c.Drain.LivenessDependsOnUpstream {
		return errors.New("drain.liveness_depends_on_upstream must be false: liveness must not depend on upstream health")
	}

	if c.Observability.MaxLabelCardinality <= 0 {
		return errors.New("observability.max_label_cardinality must be positive")
	}
	seen := make(map[string]bool, len(c.Observability.AllowedLabels))
	for _, label := range c.Observability.AllowedLabels {
		if label == "" {
			return errors.New("observability.allowed_labels must not contain empty values")
		}
		if seen[label] {
			return fmt.Errorf("observability.allowed_labels contains duplicate %q", label)
		}
		seen[label] = true
	}
	for _, status := range c.ErrorCodes.PassthroughStatuses {
		if status < 400 || status > 599 {
			return fmt.Errorf("error_codes.passthrough_statuses must only contain 4xx/5xx codes, got %d", status)
		}
	}
	return nil
}

// ReplaysUnknownOutcome reports whether the contract permits replaying a
// request whose upstream outcome is unknown. The default contract returns
// false, which is the production floor for duplicate generation.
func (c *Contract) ReplaysUnknownOutcome() bool {
	return c.Retry.RetryOnUnknownOutcome && c.Fallback.AllowCrossProviderOnUnknown
}

// HasDurableAuditSink reports whether usage events reach storage that can be
// queried later for reconciliation.
func (c *Contract) HasDurableAuditSink() bool {
	return c.Usage.EmitAuditEvents && (c.Usage.AuditSink == "durable" || c.Usage.AuditSink == "both")
}

// Phase marks how far an attempt got before it failed.
type Phase int

const (
	// PhasePreSend means the request bytes were never handed to the provider.
	PhasePreSend Phase = iota
	// PhaseAwaitingStatus means the request was sent but no response header arrived.
	PhaseAwaitingStatus
	// PhaseBodyForwarding means response bytes, or a complete response body,
	// were already produced by the provider.
	PhaseBodyForwarding
)

func (p Phase) String() string {
	switch p {
	case PhasePreSend:
		return "pre_send"
	case PhaseAwaitingStatus:
		return "awaiting_status"
	case PhaseBodyForwarding:
		return "body_forwarding"
	}
	return "unknown_phase"
}

// Outcome classifies whether the provider may have already processed a request.
type Outcome int

const (
	// OutcomeNotSent means the request was provably never delivered.
	OutcomeNotSent Outcome = iota
	// OutcomeThrottled means the provider explicitly did not process it (429, 408).
	OutcomeThrottled
	// OutcomeRejected means the provider rejected the request itself (other 4xx).
	OutcomeRejected
	// OutcomeUnknown means the request may already have been processed.
	OutcomeUnknown
	// OutcomeProcessed means the provider produced a complete result.
	OutcomeProcessed
	// OutcomePartial means the provider produced partial output before failing.
	OutcomePartial
	// OutcomeNotAttempted means no upstream request was ever made, for example
	// because the gateway rejected the request before forwarding it.
	OutcomeNotAttempted
)

func (o Outcome) String() string {
	switch o {
	case OutcomeNotSent:
		return "not_sent"
	case OutcomeThrottled:
		return "throttled"
	case OutcomeRejected:
		return "rejected"
	case OutcomeUnknown:
		return "unknown"
	case OutcomeProcessed:
		return "processed"
	case OutcomePartial:
		return "partial"
	case OutcomeNotAttempted:
		return "not_attempted"
	}
	return "unknown_outcome"
}

// Classify maps an attempt failure to its outcome. A transport error before
// the request left the process is safe to replay; anything after the bytes
// were written is not, unless the provider said otherwise.
func Classify(phase Phase, status int, err error) Outcome {
	if err != nil {
		switch phase {
		case PhasePreSend:
			return OutcomeNotSent
		case PhaseAwaitingStatus:
			return OutcomeUnknown
		default:
			return OutcomePartial
		}
	}
	switch phase {
	case PhasePreSend:
		return OutcomeNotSent
	case PhaseBodyForwarding:
		if status >= 200 && status < 300 {
			return OutcomeProcessed
		}
		return OutcomePartial
	default:
		switch {
		case status == 408 || status == 429:
			return OutcomeThrottled
		case status >= 400 && status < 500:
			return OutcomeRejected
		case status >= 500:
			return OutcomeUnknown
		default:
			return OutcomeProcessed
		}
	}
}

// Decision is what the gateway is allowed to do after a failed attempt.
type Decision struct {
	// ReplaySame allows sending the identical request to the same deployment.
	ReplaySame bool
	// SwitchProvider allows sending it to a different deployment.
	SwitchProvider bool
	// Terminal means no further attempt is allowed and the failure is final.
	Terminal bool
	// Reason is a stable code used in logs, metrics and tests.
	Reason string
}

// Decide turns an outcome into an allowed action under this contract.
func (c *Contract) Decide(o Outcome) Decision {
	switch o {
	case OutcomeNotSent:
		return Decision{ReplaySame: true, SwitchProvider: true, Reason: "request_never_delivered"}
	case OutcomeThrottled:
		// The provider did not process it, so another deployment may, but
		// replaying the same one would just burn its rate limit again.
		return Decision{SwitchProvider: true, Reason: "provider_declined_without_processing"}
	case OutcomeRejected:
		return Decision{Terminal: true, Reason: "request_rejected_by_provider"}
	case OutcomeUnknown:
		if c.ReplaysUnknownOutcome() {
			return Decision{SwitchProvider: true, Reason: "unknown_outcome_cross_provider_retry_enabled"}
		}
		return Decision{Terminal: true, Reason: "unknown_outcome_no_replay"}
	case OutcomeProcessed:
		return Decision{Terminal: true, Reason: "provider_already_processed"}
	default:
		return Decision{Terminal: true, Reason: "partial_output_not_replayable"}
	}
}

// StreamCompletion records how a streamed response ended.
type StreamCompletion string

const (
	StreamCompleted       StreamCompletion = "completed"
	StreamAbortedClient   StreamCompletion = "aborted_client"
	StreamAbortedUpstream StreamCompletion = "aborted_upstream"
	StreamAbortedGateway  StreamCompletion = "aborted_gateway"
	StreamTruncated       StreamCompletion = "truncated"
)

// CountsAsSuccess reports whether a completion may be counted as a successful
// request. Only a stream that reached its terminal event counts.
func (s StreamCompletion) CountsAsSuccess() bool {
	return s == StreamCompleted
}

// ReconciliationStatus records whether usage for a request can be trusted.
type ReconciliationStatus string

const (
	ReconciliationOK        ReconciliationStatus = "ok"
	ReconciliationMissing   ReconciliationStatus = "missing"
	ReconciliationEstimated ReconciliationStatus = "estimated"
	ReconciliationPartial   ReconciliationStatus = "partial"
)

// Gateway error codes. Every client-visible gateway error uses one of these
// inside the shared error envelope.
const (
	CodeUnauthorized        = "unauthorized"
	CodeModelNotAllowed     = "model_not_allowed"
	CodeModelNotFound       = "model_not_found"
	CodeInvalidRequest      = "invalid_request"
	CodeRequestTooLarge     = "request_too_large"
	CodeRateLimitExceeded   = "rate_limit_exceeded"
	CodeBudgetExceeded      = "budget_exceeded"
	CodeGatewayOverloaded   = "gateway_overloaded"
	CodeUpstreamUnavailable = "upstream_unavailable"
	CodeUpstreamRateLimited = "upstream_rate_limited"
	CodeUpstreamTimeout     = "upstream_timeout"
	CodeUpstreamError       = "upstream_error"
	CodeStreamAborted       = "stream_aborted"
	CodeDraining            = "draining"
	CodeNotFound            = "not_found"
	CodeInternalError       = "internal_error"
)

// GatewayErrorCodes is the closed set of codes the gateway may emit.
var GatewayErrorCodes = []string{
	CodeUnauthorized,
	CodeModelNotAllowed,
	CodeModelNotFound,
	CodeInvalidRequest,
	CodeRequestTooLarge,
	CodeRateLimitExceeded,
	CodeBudgetExceeded,
	CodeGatewayOverloaded,
	CodeUpstreamUnavailable,
	CodeUpstreamRateLimited,
	CodeUpstreamTimeout,
	CodeUpstreamError,
	CodeStreamAborted,
	CodeDraining,
	CodeNotFound,
	CodeInternalError,
}

// IsGatewayErrorCode reports whether code belongs to the closed set.
func IsGatewayErrorCode(code string) bool {
	return contains(GatewayErrorCodes, code)
}

// ErrorCodeFor maps an outcome to the code returned to the client.
func ErrorCodeFor(o Outcome) string {
	switch o {
	case OutcomeThrottled:
		return CodeUpstreamRateLimited
	case OutcomeRejected:
		return CodeUpstreamError
	case OutcomeUnknown:
		return CodeUpstreamUnavailable
	case OutcomePartial:
		return CodeStreamAborted
	default:
		return CodeUpstreamError
	}
}

func contains(set []string, value string) bool {
	for _, item := range set {
		if item == value {
			return true
		}
	}
	return false
}

func isCurrencyCode(value string) bool {
	if len(value) != 3 {
		return false
	}
	for _, r := range value {
		if r < 'A' || r > 'Z' {
			return false
		}
	}
	return true
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

func parseNonNegativeDuration(raw string) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return 0, errors.New("must be specified")
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, err
	}
	if d < 0 {
		return 0, errors.New("must not be negative")
	}
	return d, nil
}

// AllowedLabels returns the sorted label whitelist for metrics.
func (c *Contract) AllowedLabels() []string {
	out := make([]string, len(c.Observability.AllowedLabels))
	copy(out, c.Observability.AllowedLabels)
	sort.Strings(out)
	return out
}
