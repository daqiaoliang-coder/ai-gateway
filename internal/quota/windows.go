package quota

import (
	"fmt"
	"time"
)

// Window kinds supported by the contract.
const (
	WindowCalendarMonth = "calendar_month"
	WindowRolling30d    = "rolling_30d"
	WindowRolling24h    = "rolling_24h"
)

// Config describes what one request may consume.
type Config struct {
	Tenant    string
	Model     string
	Provider  string
	TenantRPM int
	// TenantTPM caps tokens per minute for the tenant.
	TenantTPM int64
	// TenantBudgetMicros caps tenant spend per budget window.
	TenantBudgetMicros int64
	// ProviderBudgetMicros caps platform-wide spend per provider per window.
	ProviderBudgetMicros int64
	// Window selects how the budget window is bucketed.
	Window string
}

// RateWindow returns the bucket id and counter lifetime for a per-minute window.
func RateWindow(now time.Time) (int64, time.Duration) {
	return now.UTC().Unix() / 60, 2 * time.Minute
}

// BudgetWindow returns the bucket id and counter lifetime for a budget window.
//
// Windows are fixed buckets rather than true sliding windows: a tenant can spend
// up to twice its budget across a boundary. Sliding windows would need a sorted
// set of every spend in the window, which does not scale to monthly volumes.
func BudgetWindow(now time.Time, window string) (int64, time.Duration, error) {
	utc := now.UTC()
	const margin = time.Hour
	switch window {
	case WindowCalendarMonth:
		start := time.Date(utc.Year(), utc.Month(), 1, 0, 0, 0, 0, time.UTC)
		next := start.AddDate(0, 1, 0)
		return start.Unix(), next.Sub(start) + margin, nil
	case WindowRolling30d:
		const period = 30 * 24 * time.Hour
		return bucketFor(utc, period, margin)
	case WindowRolling24h:
		const period = 24 * time.Hour
		return bucketFor(utc, period, margin)
	default:
		return 0, 0, fmt.Errorf("unknown budget window %q", window)
	}
}

// bucketFor returns the bucket id and how long that bucket still has to live.
func bucketFor(utc time.Time, period, margin time.Duration) (int64, time.Duration, error) {
	seconds := int64(period / time.Second)
	bucket := utc.Unix() / seconds
	end := time.Unix((bucket+1)*seconds, 0).UTC()
	return bucket, end.Sub(utc) + margin, nil
}

// Keys builds every counter key a single attempt touches.
type Keys struct {
	TenantRPM      string
	TenantTPM      string
	TenantBudget   string
	ProviderBudget string
}

// KeyFor returns the storage keys for a config at a point in time.
func KeyFor(cfg Config, now time.Time) (Keys, []time.Duration, error) {
	keys := Keys{}
	ttls := make([]time.Duration, 0, 4)
	if cfg.TenantRPM > 0 {
		bucket, ttl := RateWindow(now)
		keys.TenantRPM = fmt.Sprintf("rpm:%s:%d", cfg.Tenant, bucket)
		ttls = append(ttls, ttl)
	}
	if cfg.TenantTPM > 0 {
		bucket, ttl := RateWindow(now)
		keys.TenantTPM = fmt.Sprintf("tpm:%s:%d", cfg.Tenant, bucket)
		ttls = append(ttls, ttl)
	}
	if cfg.TenantBudgetMicros > 0 || cfg.ProviderBudgetMicros > 0 {
		bucket, ttl, err := BudgetWindow(now, cfg.Window)
		if err != nil {
			return keys, nil, err
		}
		if cfg.TenantBudgetMicros > 0 {
			keys.TenantBudget = fmt.Sprintf("budget:%s:%d", cfg.Tenant, bucket)
			ttls = append(ttls, ttl)
		}
		if cfg.ProviderBudgetMicros > 0 {
			keys.ProviderBudget = fmt.Sprintf("provider:%s:%d", cfg.Provider, bucket)
			ttls = append(ttls, ttl)
		}
	}
	return keys, ttls, nil
}

// BuildPlan assembles the reserve plan for one attempt. Tokens and money are
// charged up front because a streamed response's real size is unknown until it
// ends, and honoring a budget after the fact is not possible.
func BuildPlan(cfg Config, attemptID string, reserveTokens, reserveMicros int64, ttl time.Duration, now time.Time) (Plan, error) {
	keys, _, err := KeyFor(cfg, now)
	if err != nil {
		return Plan{}, err
	}
	plan := Plan{
		ID:       attemptID,
		Tenant:   cfg.Tenant,
		Model:    cfg.Model,
		Provider: cfg.Provider,
		TTL:      ttl,
	}
	if cfg.TenantRPM > 0 {
		plan.Lines = append(plan.Lines, Line{Key: keys.TenantRPM, Kind: KindRequests, Limit: int64(cfg.TenantRPM), Reserve: 1})
	}
	if cfg.TenantTPM > 0 {
		plan.Lines = append(plan.Lines, Line{Key: keys.TenantTPM, Kind: KindTokens, Limit: cfg.TenantTPM, Reserve: reserveTokens})
	}
	if cfg.TenantBudgetMicros > 0 {
		plan.Lines = append(plan.Lines, Line{Key: keys.TenantBudget, Kind: KindBudget, Limit: cfg.TenantBudgetMicros, Reserve: reserveMicros})
	}
	if cfg.ProviderBudgetMicros > 0 {
		plan.Lines = append(plan.Lines, Line{Key: keys.ProviderBudget, Kind: KindBudget, Limit: cfg.ProviderBudgetMicros, Reserve: reserveMicros})
	}
	return plan, nil
}

// Unmet reports which configured limits are missing from the plan, useful for
// diagnosing a deny.
func Unmet(decision Decision) Kind { return decision.DeniedKind }
