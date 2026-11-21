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
func (f failingStore) HasReservation(context.Context, string) (bool, error)    { return false, f.err }
func (f failingStore) Close() error                                            { return nil }

func TestManagerDegradationModes(t *testing.T) {
	p := plan("attempt-1", Line{Key: "tpm:t:1", Kind: KindTokens, Limit: 100, Reserve: 40})

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
		if !decision.Allowed || !decision.Degraded || !decision.Untracked {
			t.Fatalf("decision = %+v, want allowed, degraded and untracked", decision)
		}
	})

	t.Run("degrade_uses_local_at_reduced_limit", func(t *testing.T) {
		m := NewManager(failingStore{err: ErrUnavailable}, NewMemoryStore(), "degrade", 0.5, nil)
		// 100 halved to 50, so 60 cannot be granted.
		decision, err := m.Reserve(context.Background(), plan("attempt-1", Line{Key: "tpm:t:1", Kind: KindTokens, Limit: 100, Reserve: 60}))
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
		allowed, err := m.Reserve(context.Background(), plan("attempt-2", Line{Key: "tpm:t:1", Kind: KindTokens, Limit: 100, Reserve: 40}))
		if err != nil || !allowed.Allowed {
			t.Fatalf("second reserve = %+v, err %v", allowed, err)
		}
	})
}

// managerTestStore simulates shared-store outages and records calls so routing
// tests cannot pass just because a wrong-store call happened to be harmless.
type managerTestStore struct {
	*MemoryStore
	mu    sync.Mutex
	err   error
	calls map[string]int
}

func newManagerTestStore() *managerTestStore {
	return &managerTestStore{MemoryStore: NewMemoryStore(), calls: make(map[string]int)}
}

func (s *managerTestStore) setError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *managerTestStore) before(op string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls[op]++
	return s.err
}

func (s *managerTestStore) callCount(op string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[op]
}

func (s *managerTestStore) Reserve(ctx context.Context, p Plan) (Decision, error) {
	if err := s.before("reserve"); err != nil {
		return Decision{}, err
	}
	return s.MemoryStore.Reserve(ctx, p)
}

func (s *managerTestStore) Settle(ctx context.Context, settlement Settlement) error {
	if err := s.before("settle"); err != nil {
		return err
	}
	return s.MemoryStore.Settle(ctx, settlement)
}

func (s *managerTestStore) Abandon(ctx context.Context, id string) error {
	if err := s.before("abandon"); err != nil {
		return err
	}
	return s.MemoryStore.Abandon(ctx, id)
}

func (s *managerTestStore) Sweep(ctx context.Context, keys []string, now time.Time) (int, error) {
	if err := s.before("sweep"); err != nil {
		return 0, err
	}
	return s.MemoryStore.Sweep(ctx, keys, now)
}

func (s *managerTestStore) HasReservation(ctx context.Context, id string) (bool, error) {
	if err := s.before("lookup"); err != nil {
		return false, err
	}
	return s.MemoryStore.HasReservation(ctx, id)
}

func TestManagerThreeReplicasCannotMintBudgetDuringOutage(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"fail_open", "degrade", "fail_closed"} {
		t.Run(mode, func(t *testing.T) {
			shared := newManagerTestStore()
			managers := make([]*Manager, 3)
			locals := make([]*managerTestStore, 3)
			for i := range managers {
				locals[i] = newManagerTestStore()
				managers[i] = NewManager(shared, locals[i], mode, 0.5, nil)
				p := plan("spent-"+itoa(i), budgetLine("budget:t:1", 1000, 300))
				d, err := managers[i].Reserve(ctx, p)
				if err != nil || !d.Allowed {
					t.Fatalf("reserve = %+v, err %v", d, err)
				}
				if err := managers[i].Settle(ctx, Settlement{ID: p.ID, Actuals: []int64{300}}); err != nil {
					t.Fatal(err)
				}
			}
			shared.setError(ErrUnavailable)
			for i, m := range managers {
				// Each local factor*budget would grant 100, spending 1200 in
				// total against the shared 1000 cap if fallback were allowed.
				d, err := m.Reserve(ctx, plan("outage-"+itoa(i),
					Line{Key: "rpm:t:1", Kind: KindRequests, Limit: 100, Reserve: 1},
					budgetLine("budget:t:1", 1000, 100)))
				if err != nil || d.Allowed || !d.Degraded || d.Untracked || d.DeniedKind != KindBudget ||
					d.DeniedKey != "budget:t:1" || d.Reason != "shared_store_unavailable_hard_budget" {
					t.Fatalf("replica %d decision = %+v, err %v", i, d, err)
				}
				if locals[i].callCount("reserve") != 0 {
					t.Fatal("hard budget must never reach local fallback")
				}
			}
			if got := shared.Used("budget:t:1"); got != 900 {
				t.Fatalf("shared used = %d, want 900", got)
			}
		})
	}
}

func TestManagerHardBudgetLimitBoundary(t *testing.T) {
	for _, mode := range []string{"fail_open", "degrade"} {
		for _, limit := range []int64{-1, 0, 1, 1<<63 - 1} {
			for _, unavailable := range []error{ErrUnavailable, context.DeadlineExceeded} {
				local := newManagerTestStore()
				m := NewManager(failingStore{err: unavailable}, local, mode, 0.5, nil)
				// Even a zero estimate must not bypass a positive money cap.
				d, err := m.Reserve(context.Background(), plan("boundary", budgetLine("budget:t:1", limit, 0)))
				if err != nil || d.Allowed != (limit <= 0) || !d.Degraded {
					t.Fatalf("mode %s, limit %d, failure %v: decision %+v, err %v", mode, limit, unavailable, d, err)
				}
				if limit > 0 && (local.callCount("reserve") != 0 || d.Reason != "shared_store_unavailable_hard_budget") {
					t.Fatalf("positive hard budget reached fallback: %+v", d)
				}
				if d.Untracked != (limit <= 0 && mode == "fail_open") {
					t.Fatalf("unexpected untracked decision: %+v", d)
				}
			}
		}
	}
}

func TestManagerLocalReservationStaysLocalAfterRecovery(t *testing.T) {
	ctx := context.Background()
	for _, op := range []string{"settle", "abandon"} {
		t.Run(op, func(t *testing.T) {
			shared, local := newManagerTestStore(), newManagerTestStore()
			shared.setError(ErrUnavailable)
			m := NewManager(shared, local, "degrade", 1, nil)
			p := plan("local", Line{Key: "tpm:t:1", Kind: KindTokens, Limit: 100, Reserve: 40})
			d, err := m.Reserve(ctx, p)
			if err != nil || !d.Allowed || !d.Degraded || d.Untracked {
				t.Fatalf("reserve = %+v, err %v", d, err)
			}
			shared.setError(nil)
			d, err = m.Reserve(ctx, p)
			if err != nil || !d.Allowed || !d.Duplicate || !d.Degraded || shared.callCount("reserve") != 1 {
				t.Fatalf("local replay after recovery = %+v, err %v", d, err)
			}
			finalize := func() error {
				if op == "settle" {
					return m.Settle(ctx, Settlement{ID: p.ID, Actuals: []int64{25}})
				}
				return m.Abandon(ctx, p.ID)
			}
			// A local error must also preserve the original owner for retries.
			local.setError(ErrUnavailable)
			if err := finalize(); err != ErrUnavailable {
				t.Fatalf("local failure = %v, want original ErrUnavailable", err)
			}
			local.setError(nil)
			for i := 0; i < 2; i++ {
				if err := finalize(); err != nil {
					t.Fatalf("retry %d: %v", i, err)
				}
			}
			wantUsed := int64(0)
			if op == "settle" {
				wantUsed = 25
			}
			if local.Held("tpm:t:1") != 0 || local.Used("tpm:t:1") != wantUsed {
				t.Fatalf("local held/used = %d/%d, want 0/%d", local.Held("tpm:t:1"), local.Used("tpm:t:1"), wantUsed)
			}
			if shared.callCount("settle") != 0 || shared.callCount("abandon") != 0 {
				t.Fatal("a local reservation must never be finalized at shared storage")
			}
		})
	}
}

func TestManagerSharedReservationRetriesAtOwner(t *testing.T) {
	ctx := context.Background()
	for _, op := range []string{"settle", "abandon"} {
		t.Run(op, func(t *testing.T) {
			shared, local := newManagerTestStore(), newManagerTestStore()
			m := NewManager(shared, local, "degrade", 0.5, nil)
			p := plan("shared", budgetLine("budget:t:1", 100, 40))
			if d, err := m.Reserve(ctx, p); err != nil || !d.Allowed {
				t.Fatalf("reserve = %+v, err %v", d, err)
			}
			finalize := func() error {
				if op == "settle" {
					return m.Settle(ctx, Settlement{ID: p.ID, Actuals: []int64{25}})
				}
				return m.Abandon(ctx, p.ID)
			}
			for _, failure := range []error{ErrUnavailable, context.DeadlineExceeded, errors.New("store write failed")} {
				shared.setError(failure)
				if err := finalize(); err != failure {
					t.Fatalf("failure = %v, want original %v", err, failure)
				}
				if shared.Held("budget:t:1") != 40 || shared.Used("budget:t:1") != 0 {
					t.Fatal("failed finalization must not be marked completed")
				}
			}
			shared.setError(nil)
			for i := 0; i < 2; i++ {
				if err := finalize(); err != nil {
					t.Fatalf("retry %d: %v", i, err)
				}
			}
			wantUsed := int64(0)
			if op == "settle" {
				wantUsed = 25
			}
			if shared.Held("budget:t:1") != 0 || shared.Used("budget:t:1") != wantUsed {
				t.Fatalf("shared held/used = %d/%d, want 0/%d", shared.Held("budget:t:1"), shared.Used("budget:t:1"), wantUsed)
			}
			if local.callCount("settle") != 0 || local.callCount("abandon") != 0 {
				t.Fatal("shared finalization must never touch local storage, even on errors")
			}
		})
	}
}

func TestManagerSharedReserveReplayDoesNotFallBack(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"degrade", "fail_open", "fail_closed"} {
		for _, op := range []string{"settle", "abandon"} {
			t.Run(mode+"/"+op, func(t *testing.T) {
				shared, local := newManagerTestStore(), newManagerTestStore()
				m := NewManager(shared, local, mode, 1, nil)
				p := plan("shared-replay", Line{Key: "tpm:t:1", Kind: KindTokens, Limit: 100, Reserve: 40})
				if d, err := m.Reserve(ctx, p); err != nil || !d.Allowed {
					t.Fatalf("reserve = %+v, err %v", d, err)
				}
				shared.setError(ErrUnavailable)
				var wg sync.WaitGroup
				for i := 0; i < 16; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						if d, err := m.Reserve(ctx, p); !errors.Is(err, ErrUnavailable) || d.Allowed {
							t.Errorf("shared replay during outage = %+v, err %v", d, err)
						}
					}()
				}
				wg.Wait()
				shared.setError(nil)
				if d, err := m.Reserve(ctx, p); err != nil || !d.Allowed || !d.Duplicate || d.Degraded {
					t.Fatalf("shared replay after recovery = %+v, err %v", d, err)
				}
				for i := 0; i < 2; i++ {
					var err error
					if op == "settle" {
						err = m.Settle(ctx, Settlement{ID: p.ID, Actuals: []int64{25}})
					} else {
						err = m.Abandon(ctx, p.ID)
					}
					if err != nil {
						t.Fatalf("%s: %v", op, err)
					}
				}
				wantUsed := int64(0)
				if op == "settle" {
					wantUsed = 25
				}
				if shared.Held(p.Lines[0].Key) != 0 || shared.Used(p.Lines[0].Key) != wantUsed {
					t.Fatalf("shared held/used = %d/%d, want 0/%d", shared.Held(p.Lines[0].Key), shared.Used(p.Lines[0].Key), wantUsed)
				}
				if local.callCount("reserve") != 0 || local.callCount("settle") != 0 || local.callCount("abandon") != 0 {
					t.Fatal("a confirmed shared reservation must never reach local fallback")
				}
			})
		}
	}
}

type blockedFirstReserveStore struct {
	*managerTestStore
	started chan struct{}
	release chan struct{}
}

func (s *blockedFirstReserveStore) Reserve(ctx context.Context, p Plan) (Decision, error) {
	d, err := s.managerTestStore.Reserve(ctx, p)
	if err == nil && d.Allowed && !d.Duplicate && p.ID == "blocked" {
		s.setError(ErrUnavailable)
		close(s.started)
		<-s.release
	}
	return d, err
}

func TestManagerConcurrentReservePublishesOwnershipBeforeRetry(t *testing.T) {
	shared := &blockedFirstReserveStore{
		managerTestStore: newManagerTestStore(),
		started:          make(chan struct{}),
		release:          make(chan struct{}),
	}
	defer close(shared.release)
	local := newManagerTestStore()
	m := NewManager(shared, local, "degrade", 1, nil)
	p := plan("blocked", Line{Key: "tpm:t:1", Kind: KindTokens, Limit: 100, Reserve: 40})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if d, err := m.Reserve(context.Background(), p); err != nil || !d.Allowed || d.Degraded {
			t.Errorf("initial reserve = %+v, err %v", d, err)
		}
	}()
	<-shared.started
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if d, err := m.Reserve(context.Background(), p); !errors.Is(err, ErrUnavailable) || d.Allowed {
				t.Errorf("concurrent retry = %+v, err %v", d, err)
			}
		}()
	}
	otherDone := make(chan struct{})
	go func() {
		defer close(otherDone)
		other := plan("unrelated", Line{Key: "tpm:other:1", Kind: KindTokens, Limit: 100, Reserve: 10})
		if d, err := m.Reserve(context.Background(), other); err != nil || !d.Allowed || !d.Degraded {
			t.Errorf("unrelated reserve = %+v, err %v", d, err)
		}
	}()
	select {
	case <-otherDone:
	case <-time.After(time.Second):
		t.Fatal("an unrelated reservation was blocked by shared I/O")
	}
	shared.release <- struct{}{}
	wg.Wait()
	if shared.Held(p.Lines[0].Key) != 40 || local.Held(p.Lines[0].Key) != 0 {
		t.Fatalf("shared/local held = %d/%d, want 40/0", shared.Held(p.Lines[0].Key), local.Held(p.Lines[0].Key))
	}
}

func TestManagerSharedOwnershipLifetime(t *testing.T) {
	ctx := context.Background()
	shared := newManagerTestStore()
	m := NewManager(shared, NewMemoryStore(), "degrade", 1, nil)
	p := plan("shared-lifetime", Line{Key: "tpm:t:1", Kind: KindTokens, Limit: 100, Reserve: 40})
	if d, err := m.Reserve(ctx, p); err != nil || !d.Allowed {
		t.Fatalf("reserve = %+v, err %v", d, err)
	}
	ownership := m.sharedReservations[p.ID]
	if ownership.ttl < 24*time.Hour {
		t.Fatal("shared ownership must cover Redis settlement deduplication")
	}
	ownership.expiresAt = time.Now().Add(-time.Second)
	m.sharedReservations[p.ID] = ownership
	shared.setError(ErrUnavailable)
	if _, err := m.SweepNow(ctx); !errors.Is(err, ErrUnavailable) || len(m.sharedReservations) != 1 {
		t.Fatalf("shared ownership was lost during outage, err %v", err)
	}
	shared.setError(nil)
	s := Settlement{ID: p.ID, Actuals: []int64{25}}
	if err := m.Settle(ctx, s); err != nil {
		t.Fatal(err)
	}
	first := m.sharedReservations[p.ID]
	if !first.settled || first.expiresAt.Before(time.Now().Add(23*time.Hour)) {
		t.Fatalf("settlement did not retain shared ownership: %+v", first)
	}
	if _, err := m.SweepNow(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Settle(ctx, s); err != nil {
		t.Fatal(err)
	}
	if m.sharedReservations[p.ID] != first {
		t.Fatal("duplicate settlement prolonged shared ownership")
	}
	first.expiresAt = time.Now().Add(-time.Second)
	m.sharedReservations[p.ID] = first
	if _, err := m.SweepNow(ctx); err != nil || len(m.sharedReservations) != 1 {
		t.Fatalf("ownership must survive while shared deduplication exists, err %v", err)
	}
	shared.MemoryStore.mu.Lock()
	hold := shared.reserved[p.ID]
	hold.settledAt = time.Now().Add(-p.TTL - time.Second)
	shared.reserved[p.ID] = hold
	shared.MemoryStore.mu.Unlock()
	if _, err := m.SweepNow(ctx); err != nil || len(m.sharedReservations) != 0 {
		t.Fatalf("expired shared ownership was not reclaimed, err %v", err)
	}
}

type blockedSettlementStore struct {
	*managerTestStore
	written chan struct{}
	release chan struct{}
}

func (s *blockedSettlementStore) Settle(ctx context.Context, settlement Settlement) error {
	if err := s.managerTestStore.Settle(ctx, settlement); err != nil {
		return err
	}
	close(s.written)
	<-s.release
	return nil
}

func TestManagerSweepDoesNotForgetAnInFlightSettlement(t *testing.T) {
	ctx := context.Background()
	shared := &blockedSettlementStore{
		managerTestStore: newManagerTestStore(),
		written:          make(chan struct{}),
		release:          make(chan struct{}),
	}
	local := newManagerTestStore()
	m := NewManager(shared, local, "degrade", 1, nil)
	p := plan("settling", Line{Key: "tpm:t:1", Kind: KindTokens, Limit: 100, Reserve: 40})
	if d, err := m.Reserve(ctx, p); err != nil || !d.Allowed {
		t.Fatalf("reserve = %+v, err %v", d, err)
	}
	owner := m.sharedReservations[p.ID]
	owner.expiresAt = time.Now().Add(-time.Second)
	m.sharedReservations[p.ID] = owner
	done := make(chan error, 1)
	defer close(shared.release)
	go func() { done <- m.Settle(ctx, Settlement{ID: p.ID, Actuals: []int64{25}}) }()
	<-shared.written
	if _, err := m.SweepNow(ctx); err != nil {
		t.Fatal(err)
	}
	shared.release <- struct{}{}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	owner, exists := m.sharedReservations[p.ID]
	if !exists || !owner.settled || owner.expiresAt.Before(time.Now().Add(23*time.Hour)) {
		t.Fatalf("settled ownership was lost: %+v", owner)
	}
	shared.setError(ErrUnavailable)
	if d, err := m.Reserve(ctx, p); !errors.Is(err, ErrUnavailable) || d.Allowed {
		t.Fatalf("settled replay during outage = %+v, err %v", d, err)
	}
	if local.callCount("reserve") != 0 || shared.Used(p.Lines[0].Key) != 25 {
		t.Fatal("settlement was rerouted or duplicated")
	}
}

type boundedSweepStore struct{ *managerTestStore }

func (s boundedSweepStore) Sweep(ctx context.Context, _ []string, _ time.Time) (int, error) {
	if err := s.before("sweep"); err != nil {
		return 0, err
	}
	ids := s.PendingIDs()
	if len(ids) > 200 {
		ids = ids[:200]
	}
	for _, id := range ids {
		if err := s.MemoryStore.Abandon(ctx, id); err != nil {
			return 0, err
		}
	}
	return len(ids), nil
}

func TestManagerBoundedSweepRetainsUnreclaimedOwnership(t *testing.T) {
	ctx := context.Background()
	shared := boundedSweepStore{newManagerTestStore()}
	past := time.Now().Add(-72 * time.Hour)
	shared.clock = func() time.Time { return past }
	local := newManagerTestStore()
	m := NewManager(shared, local, "degrade", 1, nil)
	for i := 0; i < 201; i++ {
		p := plan("backlog-"+itoa(i), Line{Key: "tpm:backlog:1", Kind: KindTokens, Limit: 1000, Reserve: 1})
		if d, err := m.Reserve(ctx, p); err != nil || !d.Allowed {
			t.Fatalf("reserve %d = %+v, err %v", i, d, err)
		}
		owner := m.sharedReservations[p.ID]
		owner.expiresAt = past.Add(owner.ttl)
		m.sharedReservations[p.ID] = owner
	}
	if n, err := m.SweepNow(ctx); err != nil || n != 200 {
		t.Fatalf("sweep = %d, err %v", n, err)
	}
	pending := shared.PendingIDs()
	if len(pending) != 1 || len(m.sharedReservations) != 1 {
		t.Fatalf("pending/owners = %d/%d, want 1/1", len(pending), len(m.sharedReservations))
	}
	id := pending[0]
	shared.setError(ErrUnavailable)
	p := plan(id, Line{Key: "tpm:backlog:1", Kind: KindTokens, Limit: 1000, Reserve: 1})
	if d, err := m.Reserve(ctx, p); !errors.Is(err, ErrUnavailable) || d.Allowed {
		t.Fatalf("unreclaimed replay during outage = %+v, err %v", d, err)
	}
	if local.callCount("reserve") != 0 {
		t.Fatal("unreclaimed shared hold was replaced locally")
	}
	shared.setError(nil)
	if n, err := m.SweepNow(ctx); err != nil || n != 1 || len(m.sharedReservations) != 0 {
		t.Fatalf("final sweep = %d, owners %d, err %v", n, len(m.sharedReservations), err)
	}
}

type lookupErrorStore struct{ *managerTestStore }

func (s lookupErrorStore) HasReservation(context.Context, string) (bool, error) {
	_ = s.before("lookup")
	return false, ErrUnavailable
}

func TestManagerLookupFailureRetainsOwnershipAndSweepsLocal(t *testing.T) {
	ctx := context.Background()
	shared := lookupErrorStore{newManagerTestStore()}
	local := newManagerTestStore()
	m := NewManager(shared, local, "degrade", 1, nil)
	for _, id := range []string{"first-lookup", "second-lookup"} {
		p := plan(id, Line{Key: "tpm:lookup:1", Kind: KindTokens, Limit: 100, Reserve: 40})
		if d, err := m.Reserve(ctx, p); err != nil || !d.Allowed {
			t.Fatalf("reserve = %+v, err %v", d, err)
		}
		owner := m.sharedReservations[id]
		owner.expiresAt = time.Now().Add(-time.Second)
		m.sharedReservations[id] = owner
	}
	if _, err := m.SweepNow(ctx); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("lookup error = %v", err)
	}
	if len(m.sharedReservations) != 2 || shared.callCount("lookup") != 1 || local.callCount("sweep") != 1 {
		t.Fatalf("owners/lookups/local sweeps = %d/%d/%d", len(m.sharedReservations), shared.callCount("lookup"), local.callCount("sweep"))
	}
}

func TestManagerLocalOwnershipExpiresDuringSharedOutage(t *testing.T) {
	ctx := context.Background()
	local := NewMemoryStore()
	past := time.Now().Add(-2 * time.Minute)
	local.clock = func() time.Time { return past }
	m := NewManager(failingStore{err: ErrUnavailable}, local, "degrade", 1, nil)
	p := plan("expired", Line{Key: "rpm:t:1", Kind: KindRequests, Limit: 100, Reserve: 1})
	if d, err := m.Reserve(ctx, p); err != nil || !d.Allowed {
		t.Fatalf("reserve = %+v, err %v", d, err)
	}
	// Advance ownership to the same expired deadline without sleeping.
	m.mu.Lock()
	reservation := m.localReservations[p.ID]
	reservation.expiresAt = past.Add(p.TTL)
	m.localReservations[p.ID] = reservation
	m.mu.Unlock()
	released, err := m.SweepNow(ctx)
	if !errors.Is(err, ErrUnavailable) || released != 1 {
		t.Fatalf("sweep = %d, %v; want one local release and ErrUnavailable", released, err)
	}
	if local.Held("rpm:t:1") != 0 || len(m.localReservations) != 0 {
		t.Fatal("expired fallback holds and ownership must be reclaimed despite the outage")
	}
}

func TestManagerLocalSettlementRetainsIdempotencyTTL(t *testing.T) {
	ctx := context.Background()
	shared := newManagerTestStore()
	shared.setError(ErrUnavailable)
	m := NewManager(shared, NewMemoryStore(), "degrade", 1, nil)
	p := plan("settled", Line{Key: "tpm:t:1", Kind: KindTokens, Limit: 100, Reserve: 40})
	if d, err := m.Reserve(ctx, p); err != nil || !d.Allowed {
		t.Fatalf("reserve = %+v, err %v", d, err)
	}
	m.mu.Lock()
	reservation := m.localReservations[p.ID]
	reservation.expiresAt = time.Now().Add(-time.Second)
	m.localReservations[p.ID] = reservation
	m.mu.Unlock()
	shared.setError(nil)
	s := Settlement{ID: p.ID, Actuals: []int64{25}}
	if err := m.Settle(ctx, s); err != nil {
		t.Fatal(err)
	}
	first := m.localReservations[p.ID]
	if !first.settled || !first.expiresAt.After(time.Now()) {
		t.Fatalf("settled ownership must receive an idempotency TTL: %+v", first)
	}
	if _, err := m.SweepNow(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Settle(ctx, s); err != nil {
		t.Fatal(err)
	}
	if got := m.localReservations[p.ID]; got != first {
		t.Fatal("duplicate settlements must not prolong ownership forever")
	}
	if shared.callCount("settle") != 0 {
		t.Fatal("duplicate settlement must remain local after original hold TTL")
	}
}

func TestManagerConcurrentLocalRoutingAndSweep(t *testing.T) {
	ctx := context.Background()
	shared, local := newManagerTestStore(), newManagerTestStore()
	shared.setError(ErrUnavailable)
	m := NewManager(shared, local, "degrade", 1, nil)
	const attempts = 128
	plans := make([]Plan, attempts)
	var wg sync.WaitGroup
	for i := range plans {
		plans[i] = plan("concurrent-"+itoa(i), Line{Key: "tpm:t:1", Kind: KindTokens, Limit: 10000, Reserve: 10})
		wg.Add(1)
		go func(p Plan) {
			defer wg.Done()
			if d, err := m.Reserve(ctx, p); err != nil || !d.Allowed || !d.Degraded {
				t.Errorf("reserve = %+v, err %v", d, err)
			}
		}(plans[i])
	}
	wg.Wait()
	shared.setError(nil)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < attempts; i++ {
			if _, err := m.SweepNow(ctx); err != nil {
				t.Errorf("sweep: %v", err)
			}
		}
	}()
	for i, p := range plans {
		wg.Add(1)
		go func(i int, p Plan) {
			defer wg.Done()
			if d, err := m.Reserve(ctx, p); err != nil || !d.Allowed || !d.Duplicate || !d.Degraded {
				t.Errorf("replay = %+v, err %v", d, err)
			}
			for retry := 0; retry < 2; retry++ {
				var err error
				if i%2 == 0 {
					err = m.Settle(ctx, Settlement{ID: p.ID, Actuals: []int64{5}})
				} else {
					err = m.Abandon(ctx, p.ID)
				}
				if err != nil {
					t.Errorf("finalize: %v", err)
				}
			}
		}(i, p)
	}
	wg.Wait()
	if local.Used("tpm:t:1") != attempts/2*5 || local.Held("tpm:t:1") != 0 {
		t.Fatalf("local used/held = %d/%d", local.Used("tpm:t:1"), local.Held("tpm:t:1"))
	}
	if shared.callCount("reserve") != attempts || shared.callCount("settle") != 0 || shared.callCount("abandon") != 0 {
		t.Fatal("recovery must not reroute local reservations to shared storage")
	}
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
