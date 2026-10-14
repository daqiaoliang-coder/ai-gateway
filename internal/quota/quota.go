// Package quota separates request rate limiting from token and money budgets,
// and gives both a shared, atomic home.
//
// Rate limits answer "how often"; budgets answer "how much". They overlap but
// are not interchangeable: a tenant can stay under its RPM and still burn a
// month of budget in a minute. Every enforcement path here goes through a three
// step reserve / settle / abandon cycle keyed by attempt id, so a retried
// request can never double-charge a tenant.
package quota

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Kind tells the store what a window is counting.
type Kind string

const (
	// KindRequests counts requests per window (RPM).
	KindRequests Kind = "requests"
	// KindTokens counts tokens per window (TPM).
	KindTokens Kind = "tokens"
	// KindBudget counts money in micro units per window.
	KindBudget Kind = "budget"
)

// Line is one enforceable dimension of a single attempt.
type Line struct {
	// Key identifies the counter, including tenant, provider and window bucket.
	Key string
	// Kind is what Key counts.
	Kind Kind
	// Limit is the cap for the window. Zero or negative means unlimited.
	Limit int64
	// Reserve is the amount held up front, before the real cost is known.
	Reserve int64
}

// Plan is the full set of dimensions one attempt consumes.
type Plan struct {
	// ID must be stable for retries of the same attempt; it makes reserve and
	// settle idempotent.
	ID string
	// Tenant owns the plan.
	Tenant string
	// Model and Provider describe what is being consumed, carried into events.
	Model    string
	Provider string
	// Lines are the windows to charge. Every line must be granted or none are.
	Lines []Line
	// TTL bounds how long an unsettled hold survives; it is what lets a crashed
	// replica's reservations be reclaimed instead of leaked forever.
	TTL time.Duration
}

// Decision is the answer to a Plan.
type Decision struct {
	Allowed bool
	// DeniedKey names the window that refused the plan.
	DeniedKey string
	// DeniedKind is what that window was counting.
	DeniedKind Kind
	// Remaining is what was left in the refusing window.
	Remaining int64
	// Duplicate reports that this plan was already decided, so a retry did not
	// consume the budget twice.
	Duplicate bool
	// Degraded means the decision was made without the shared store.
	Degraded bool
	Reason   string
}

// Settlement finalizes a plan with real numbers.
type Settlement struct {
	ID string
	// Actuals holds the measured amount per line, in the same order as the plan.
	Actuals []int64
}

// ErrUnavailable means the backing store could not be reached. The manager turns
// this into the configured degradation; stores never decide policy themselves.
var ErrUnavailable = errors.New("quota store unavailable")

// ErrUnknownReservation means settle or abandon arrived for an id the store does
// not know, which happens after a hold expired. It is not fatal.
var ErrUnknownReservation = errors.New("unknown reservation")

// Store is the shared, atomic home for quotas. Implementations must be safe for
// concurrent use and must treat Reserve as idempotent per Plan.ID.
type Store interface {
	Reserve(ctx context.Context, plan Plan) (Decision, error)
	Settle(ctx context.Context, settlement Settlement) error
	Abandon(ctx context.Context, id string) error
	// Sweep reclaims holds that outlived their TTL. Keys is a hint of the windows
	// worth scanning; a store that tracks its own state may ignore it.
	Sweep(ctx context.Context, keys []string, now time.Time) (int, error)
	Close() error
}

// Manager applies the contract's degradation policy on top of a store.
type Manager struct {
	shared  Store
	local   Store
	mode    string
	factor  float64
	mu      sync.RWMutex
	metrics *Counters
	// observed remembers which windows this replica has touched so the sweeper
	// knows what to reclaim.
	observed map[string]time.Time
}

// Counters are optional observability hooks for quota decisions.
type Counters struct {
	ReserveOK   func()
	ReserveDeny func(kind Kind)
	Degraded    func()
	Release     func()
}

// NewManager wires a store together with its degradation policy.
func NewManager(shared, local Store, mode string, factor float64, counters *Counters) *Manager {
	if mode == "" {
		mode = "fail_closed"
	}
	if factor <= 0 {
		factor = 1
	}
	return &Manager{shared: shared, local: local, mode: mode, factor: factor, metrics: counters}
}

// Mode reports the configured degradation policy.
func (m *Manager) Mode() string { return m.mode }

// Reserve grants every line or none. Store failures are resolved by the
// configured policy instead of being returned to the caller.
func (m *Manager) Reserve(ctx context.Context, plan Plan) (Decision, error) {
	decision, err := m.shared.Reserve(ctx, plan)
	if err == nil {
		m.observe(plan)
		if m.metrics != nil {
			if decision.Allowed {
				m.metrics.ReserveOK()
			} else {
				m.metrics.ReserveDeny(decision.DeniedKind)
			}
		}
		return decision, nil
	}
	if !errors.Is(err, ErrUnavailable) && !errors.Is(err, context.DeadlineExceeded) {
		return Decision{}, err
	}
	return m.degrade(ctx, plan)
}

func (m *Manager) degrade(ctx context.Context, plan Plan) (Decision, error) {
	if m.metrics != nil && m.metrics.Degraded != nil {
		m.metrics.Degraded()
	}
	switch m.mode {
	case "fail_open":
		return Decision{Allowed: true, Degraded: true, Reason: "shared_store_unavailable_fail_open"}, nil
	case "degrade":
		if m.local == nil {
			return Decision{Allowed: false, Degraded: true, Reason: "shared_store_unavailable_no_local_fallback"}, nil
		}
		scaled := plan
		scaled.Lines = make([]Line, len(plan.Lines))
		for i, line := range plan.Lines {
			if line.Limit > 0 {
				line.Limit = int64(float64(line.Limit) * m.factor)
				if line.Limit == 0 {
					line.Limit = 1
				}
			}
			scaled.Lines[i] = line
		}
		decision, err := m.local.Reserve(ctx, scaled)
		if err != nil {
			return Decision{Allowed: false, Degraded: true, Reason: "local_fallback_failed"}, nil
		}
		decision.Degraded = true
		return decision, nil
	default:
		return Decision{Allowed: false, Degraded: true, Reason: "shared_store_unavailable_fail_closed"}, nil
	}
}

// Settle records real usage. An unknown reservation is reported, not absorbed.
func (m *Manager) Settle(ctx context.Context, settlement Settlement) error {
	if err := m.shared.Settle(ctx, settlement); err != nil {
		if errors.Is(err, ErrUnknownReservation) {
			return err
		}
		if m.local != nil {
			return m.local.Settle(ctx, settlement)
		}
		if errors.Is(err, ErrUnavailable) {
			return err
		}
		return err
	}
	if m.local != nil {
		// Keep the fallback roughly honest without letting it double-charge.
		_ = m.local.Abandon(ctx, settlement.ID)
	}
	return nil
}

// Abandon returns every held amount for an attempt.
func (m *Manager) Abandon(ctx context.Context, id string) error {
	var first error
	if err := m.shared.Abandon(ctx, id); err != nil && !errors.Is(err, ErrUnknownReservation) {
		first = err
	}
	if m.local != nil {
		if err := m.local.Abandon(ctx, id); err != nil && !errors.Is(err, ErrUnknownReservation) && first == nil {
			first = err
		}
	}
	if m.metrics != nil && m.metrics.Release != nil {
		m.metrics.Release()
	}
	return first
}

// StartSweeper reclaims abandoned holds on a timer. Without it, a replica that
// dies between reserve and settle would leak its tenant's budget until the
// window rolled over.
func (m *Manager) StartSweeper(ctx context.Context, interval time.Duration, onSweep func(released int)) {
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				released, _ := m.SweepNow(ctx)
				if released > 0 && onSweep != nil {
					onSweep(released)
				}
			}
		}
	}()
}

// SweepNow reclaims expired holds in both stores.
func (m *Manager) SweepNow(ctx context.Context) (int, error) {
	keys := m.observedKeys()
	now := time.Now()
	total := 0
	if m.shared != nil {
		released, err := m.shared.Sweep(ctx, keys, now)
		total += released
		if err != nil {
			return total, err
		}
	}
	if m.local != nil {
		released, err := m.local.Sweep(ctx, keys, now)
		total += released
		if err != nil {
			return total, err
		}
	}
	return total, nil
}

const maxObservedKeys = 4096

func (m *Manager) observe(plan Plan) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.observed == nil {
		m.observed = make(map[string]time.Time, 16)
	}
	for _, line := range plan.Lines {
		if line.Key == "" {
			continue
		}
		m.observed[line.Key] = time.Now()
	}
	if len(m.observed) > maxObservedKeys {
		oldest := ""
		var oldestAt time.Time
		for key, seen := range m.observed {
			if oldest == "" || seen.Before(oldestAt) {
				oldest, oldestAt = key, seen
			}
		}
		delete(m.observed, oldest)
	}
}

func (m *Manager) observedKeys() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	keys := make([]string, 0, len(m.observed))
	for key := range m.observed {
		keys = append(keys, key)
	}
	return keys
}

// Close releases both stores.
func (m *Manager) Close() error {
	var errs []error
	if m.shared != nil {
		errs = append(errs, m.shared.Close())
	}
	if m.local != nil {
		errs = append(errs, m.local.Close())
	}
	return errors.Join(errs...)
}

// String renders a decision for logs.
func (d Decision) String() string {
	if d.Allowed {
		return "allowed"
	}
	return fmt.Sprintf("denied by %s window %s", d.DeniedKind, d.DeniedKey)
}
