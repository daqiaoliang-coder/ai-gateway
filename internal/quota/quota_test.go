package quota

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func plan(id string, lines ...Line) Plan {
	return Plan{ID: id, Tenant: "tenant-a", Model: "fast", Provider: "primary", Lines: lines, TTL: time.Minute}
}

func budgetLine(key string, limit, reserve int64) Line {
	return Line{Key: key, Kind: KindBudget, Limit: limit, Reserve: reserve}
}

// Acceptance: concurrent requests must not consume the same reservation twice,
// and the window must never be oversold.
func TestConcurrentReserveNeverOversells(t *testing.T) {
	store := NewMemoryStore()
	const (
		limit    = int64(1000)
		requests = 400
		perCall  = int64(10)
	)
	var wg sync.WaitGroup
	var allowed int64
	var mu sync.Mutex
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			decision, err := store.Reserve(context.Background(), plan("req-"+string(rune('a'+i%26))+"-"+itoa(i), budgetLine("budget:t:1", limit, perCall)))
			if err != nil {
				t.Errorf("reserve: %v", err)
				return
			}
			if decision.Allowed {
				mu.Lock()
				allowed++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if allowed*perCall > limit {
		t.Fatalf("granted %d reservations worth %d, limit is %d", allowed, allowed*perCall, limit)
	}
	held := store.Held("budget:t:1")
	if held > limit {
		t.Fatalf("held = %d, must never exceed the limit %d", held, limit)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	digits := ""
	for i > 0 {
		digits = string(rune('0'+i%10)) + digits
		i /= 10
	}
	return digits
}

func TestReserveIsIdempotentPerAttemptID(t *testing.T) {
	store := NewMemoryStore()
	p := plan("attempt-1", budgetLine("budget:t:1", 100, 40))

	first, err := store.Reserve(context.Background(), p)
	if err != nil || !first.Allowed {
		t.Fatalf("first reserve = %+v, err %v", first, err)
	}
	second, err := store.Reserve(context.Background(), p)
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	if !second.Duplicate {
		t.Fatal("replaying a reservation must be reported as a duplicate")
	}
	if store.Held("budget:t:1") != 40 {
		t.Fatalf("held = %d, want 40: a retry must not hold twice", store.Held("budget:t:1"))
	}
}

func TestSettleIsIdempotent(t *testing.T) {
	store := NewMemoryStore()
	p := plan("attempt-1", budgetLine("budget:t:1", 100, 40))
	if _, err := store.Reserve(context.Background(), p); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := store.Settle(context.Background(), Settlement{ID: p.ID, Actuals: []int64{25}}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if err := store.Settle(context.Background(), Settlement{ID: p.ID, Actuals: []int64{25}}); err != nil {
		t.Fatalf("second settle: %v", err)
	}
	if got := store.Used("budget:t:1"); got != 25 {
		t.Fatalf("used = %d, want 25: settling twice must not double-charge", got)
	}
	if got := store.Held("budget:t:1"); got != 0 {
		t.Fatalf("held = %d, want 0 after settle", got)
	}
}

func TestExpiredHoldsAreReclaimed(t *testing.T) {
	store := NewMemoryStore()
	p := plan("attempt-1", budgetLine("budget:t:1", 100, 90))
	p.TTL = time.Millisecond
	if _, err := store.Reserve(context.Background(), p); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	released, err := store.Sweep(context.Background(), nil, time.Now())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if released != 1 {
		t.Fatalf("released = %d, want 1", released)
	}
	if got := store.Held("budget:t:1"); got != 0 {
		t.Fatalf("held = %d, want 0 after reclaim", got)
	}

	// The reclaimed budget must be usable again, which is the whole point.
	decision, err := store.Reserve(context.Background(), plan("attempt-2", budgetLine("budget:t:1", 100, 90)))
	if err != nil || !decision.Allowed {
		t.Fatalf("reserve after reclaim = %+v, err %v", decision, err)
	}
}

func TestAbandonReturnsHeldBudget(t *testing.T) {
	store := NewMemoryStore()
	p := plan("attempt-1", budgetLine("budget:t:1", 100, 60))
	if _, err := store.Reserve(context.Background(), p); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := store.Abandon(context.Background(), p.ID); err != nil {
		t.Fatalf("abandon: %v", err)
	}
	if got := store.Held("budget:t:1"); got != 0 {
		t.Fatalf("held = %d, want 0 after abandon", got)
	}
	if got := store.Used("budget:t:1"); got != 0 {
		t.Fatalf("used = %d, want 0 after abandon", got)
	}
	if err := store.Abandon(context.Background(), p.ID); !errors.Is(err, ErrUnknownReservation) {
		t.Fatalf("abandoning twice = %v, want ErrUnknownReservation", err)
	}
}

// Rate limiting and budgeting are separate dimensions: a tenant inside its RPM
// can still exhaust its money, and each is reported with its own kind.
func TestRateAndBudgetAreSeparateDimensions(t *testing.T) {
	store := NewMemoryStore()
	p := plan("attempt-1",
		Line{Key: "rpm:t:1", Kind: KindRequests, Limit: 1, Reserve: 1},
		budgetLine("budget:t:1", 100, 40))
	if _, err := store.Reserve(context.Background(), p); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := store.Settle(context.Background(), Settlement{ID: p.ID, Actuals: []int64{1, 10}}); err != nil {
		t.Fatalf("settle: %v", err)
	}

	blocked := plan("attempt-2",
		Line{Key: "rpm:t:1", Kind: KindRequests, Limit: 1, Reserve: 1},
		budgetLine("budget:t:1", 100, 40))
	decision, err := store.Reserve(context.Background(), blocked)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if decision.Allowed {
		t.Fatal("second request in the same minute must be blocked")
	}
	if decision.DeniedKind != KindRequests {
		t.Fatalf("denied by %s, want %s", decision.DeniedKind, KindRequests)
	}

	// With RPM headroom but no money left, the refusal must name the budget.
	spent := plan("attempt-3", budgetLine("budget:t:2", 50, 80))
	decision, err = store.Reserve(context.Background(), spent)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if decision.Allowed || decision.DeniedKind != KindBudget {
		t.Fatalf("decision = %+v, want a budget refusal", decision)
	}
	if decision.Remaining != 50 {
		t.Fatalf("remaining = %d, want 50", decision.Remaining)
	}
}

func TestRateAndTokenBudgetsAreNotInterchangeable(t *testing.T) {
	store := NewMemoryStore()
	// One request per minute, but enough token budget for a hundred.
	p := plan("attempt-1",
		Line{Key: "rpm:t:1", Kind: KindRequests, Limit: 1, Reserve: 1},
		Line{Key: "tpm:t:1", Kind: KindTokens, Limit: 100000, Reserve: 500})
	decision, err := store.Reserve(context.Background(), p)
	if err != nil || !decision.Allowed {
		t.Fatalf("reserve = %+v, err %v", decision, err)
	}
	if err := store.Settle(context.Background(), Settlement{ID: p.ID, Actuals: []int64{1, 500}}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	// Both dimensions were charged their own actuals, not each other's.
	if got := store.Used("rpm:t:1"); got != 1 {
		t.Fatalf("rpm used = %d, want 1", got)
	}
	if got := store.Used("tpm:t:1"); got != 500 {
		t.Fatalf("tpm used = %d, want 500 tokens", got)
	}
}

type failingStore struct {
	err     error
	allowed bool
}

func (f failingStore) Reserve(context.Context, Plan) (Decision, error) {
	if f.err != nil {
		return Decision{}, f.err
	}
	return Decision{Allowed: f.allowed}, nil
}
func (f failingStore) Settle(context.Context, Settlement) error                { return f.err }
func (f failingStore) Abandon(context.Context, string) error                   { return f.err }
func (f failingStore) Sweep(context.Context, []string, time.Time) (int, error) { return 0, f.err }
func (f failingStore) Close() error                                            { return nil }

func TestManagerDegradationModes(t *testing.T) {
	p := plan("attempt-1", budgetLine("budget:t:1", 100, 40))

	t.Run("fail_closed", func(t *testing.T) {
		m := NewManager(failingStore{err: ErrUnavailable}, NewMemoryStore(), "fail_closed", 1, nil)
		decision, err := m.Reserve(context.Background(), p)
		if err != nil {
			t.Fatalf("reserve: %v", err)
		}
		if decision.Allowed {
			t.Fatal("fail_closed must refuse when the shared store is gone")
		}
		if !decision.Degraded {
			t.Fatal("decision must be marked degraded")
		}
	})

	t.Run("fail_open", func(t *testing.T) {
		m := NewManager(failingStore{err: ErrUnavailable}, NewMemoryStore(), "fail_open", 1, nil)
		decision, err := m.Reserve(context.Background(), p)
		if err != nil {
			t.Fatalf("reserve: %v", err)
		}
		if !decision.Allowed || !decision.Degraded {
			t.Fatalf("decision = %+v, want allowed and degraded", decision)
		}
	})

	t.Run("degrade_uses_local_at_reduced_limit", func(t *testing.T) {
		m := NewManager(failingStore{err: ErrUnavailable}, NewMemoryStore(), "degrade", 0.5, nil)
		// 100 halved to 50, so 60 cannot be granted.
		decision, err := m.Reserve(context.Background(), plan("attempt-1", budgetLine("budget:t:1", 100, 60)))
		if err != nil {
			t.Fatalf("reserve: %v", err)
		}
		if decision.Allowed {
			t.Fatal("half of 100 is 50, so 60 must be refused while degraded")
		}
		if !decision.Degraded {
			t.Fatal("decision must be marked degraded")
		}
		// Within the reduced cap it still works, which is the point of degrade.
		allowed, err := m.Reserve(context.Background(), plan("attempt-2", budgetLine("budget:t:1", 100, 40)))
		if err != nil || !allowed.Allowed {
			t.Fatalf("second reserve = %+v, err %v", allowed, err)
		}
	})
}

func TestManagerSweepReachesBothStores(t *testing.T) {
	shared := NewMemoryStore()
	local := NewMemoryStore()
	m := NewManager(shared, local, "degrade", 0.5, nil)
	expiring := plan("attempt-1", budgetLine("budget:t:1", 100, 90))
	expiring.TTL = time.Millisecond
	if _, err := shared.Reserve(context.Background(), expiring); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, err := m.Reserve(context.Background(), plan("attempt-2", budgetLine("budget:t:2", 100, 10))); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	time.Sleep(5 * time.Millisecond)

	released, err := m.SweepNow(context.Background())
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if released < 1 {
		t.Fatalf("released = %d, want at least the expired hold", released)
	}
	if got := shared.Held("budget:t:1"); got != 0 {
		t.Fatalf("shared held = %d, want 0", got)
	}
}

func TestBudgetWindowBucketing(t *testing.T) {
	now := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	first, ttl, err := BudgetWindow(now, WindowCalendarMonth)
	if err != nil {
		t.Fatalf("budget window: %v", err)
	}
	later, _, err := BudgetWindow(now.AddDate(0, 0, 5), WindowCalendarMonth)
	if err != nil {
		t.Fatalf("budget window: %v", err)
	}
	if first != later {
		t.Fatal("two days in the same month must share a window")
	}
	if ttl <= 10*24*time.Hour {
		t.Fatalf("ttl = %s, should cover the rest of the month", ttl)
	}
	nextMonth, _, _ := BudgetWindow(now.AddDate(0, 1, 0), WindowCalendarMonth)
	if nextMonth == first {
		t.Fatal("the next month must be a different window")
	}
	if _, _, err := BudgetWindow(now, "weekly"); err == nil {
		t.Fatal("unknown windows must be rejected")
	}
}

func TestBuildPlanSkipsUnlimitedDimensions(t *testing.T) {
	p, err := BuildPlan(Config{Tenant: "t", Window: WindowCalendarMonth}, "attempt-1", 10, 20, time.Minute, time.Now())
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}
	if len(p.Lines) != 0 {
		t.Fatalf("lines = %d, want 0 when no limit is configured", len(p.Lines))
	}
}

func TestBuildPlanChargesEveryConfiguredDimension(t *testing.T) {
	p, err := BuildPlan(Config{
		Tenant:             "t",
		TenantRPM:          10,
		TenantTPM:          5000,
		TenantBudgetMicros: 900,
		Window:             WindowCalendarMonth,
	}, "attempt-1", 100, 30, time.Minute, time.Now())
	if err != nil {
		t.Fatalf("build plan: %v", err)
	}
	if len(p.Lines) != 3 {
		t.Fatalf("lines = %d, want requests, tokens and budget", len(p.Lines))
	}
	kinds := map[Kind]bool{}
	for _, line := range p.Lines {
		kinds[line.Kind] = true
	}
	for _, want := range []Kind{KindRequests, KindTokens, KindBudget} {
		if !kinds[want] {
			t.Fatalf("missing dimension %s", want)
		}
	}
}
