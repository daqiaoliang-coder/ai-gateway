package quota

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// These tests need a real Redis, because the store's correctness lives in Lua
// scripts that only Redis can execute. Set AI_GATEWAY_REDIS_TEST_ADDR to run
// them, for example 127.0.0.1:6399 for a throwaway container.
func redisStore(t *testing.T) *RedisStore {
	t.Helper()
	return redisStoreWithPrefix(t, fmt.Sprintf("test-%d", time.Now().UnixNano()))
}

func redisStoreWithPrefix(t *testing.T, prefix string) *RedisStore {
	t.Helper()
	addr := os.Getenv("AI_GATEWAY_REDIS_TEST_ADDR")
	if addr == "" {
		t.Skip("set AI_GATEWAY_REDIS_TEST_ADDR to run Redis integration tests")
	}
	store, err := NewRedisStore(RedisConfig{
		Addr:         addr,
		PoolSize:     8,
		DialTimeout:  2 * time.Second,
		ReadTimeout:  2 * time.Second,
		WriteTimeout: 2 * time.Second,
		KeyPrefix:    prefix,
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func redisTestInt(t *testing.T, store *RedisStore, args ...string) int64 {
	t.Helper()
	reply, err := store.pool.run(args)
	if err != nil || reply.typ != ':' {
		t.Fatalf("%v: reply = %+v, err %v", args, reply, err)
	}
	return reply.Int()
}

func assertRedisCounters(t *testing.T, store *RedisStore, key string, wantUsed, wantHeld int64) {
	t.Helper()
	used, held, err := store.Counters(key)
	if err != nil || used != wantUsed || held != wantHeld {
		t.Fatalf("%s: used/held = %d/%d, want %d/%d, err %v", key, used, held, wantUsed, wantHeld, err)
	}
}

func TestRedisReservationLookup(t *testing.T) {
	ctx := context.Background()
	store := redisStore(t)
	p := plan("lookup", budgetLine("budget:lookup:1", 100, 40))
	if exists, err := store.HasReservation(ctx, p.ID); err != nil || exists {
		t.Fatalf("missing reservation = %t, err %v", exists, err)
	}
	if d, err := store.Reserve(ctx, p); err != nil || !d.Allowed {
		t.Fatalf("reserve = %+v, err %v", d, err)
	}
	if exists, err := store.HasReservation(ctx, p.ID); err != nil || !exists {
		t.Fatalf("held reservation = %t, err %v", exists, err)
	}
	if err := store.Settle(ctx, Settlement{ID: p.ID, Actuals: []int64{25}}); err != nil {
		t.Fatal(err)
	}
	if exists, err := store.HasReservation(ctx, p.ID); err != nil || !exists {
		t.Fatalf("settled reservation = %t, err %v", exists, err)
	}
	p.ID = "abandoned-lookup"
	if d, err := store.Reserve(ctx, p); err != nil || !d.Allowed {
		t.Fatalf("reserve = %+v, err %v", d, err)
	}
	if err := store.Abandon(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	if exists, err := store.HasReservation(ctx, p.ID); err != nil || exists {
		t.Fatalf("abandoned reservation = %t, err %v", exists, err)
	}
}

func TestRedisReserveAndSettle(t *testing.T) {
	store := redisStore(t)
	key := "budget:tenant-a:1"
	p := plan("attempt-1", budgetLine(key, 1000, 400))

	decision, err := store.Reserve(context.Background(), p)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if !decision.Allowed {
		t.Fatalf("decision = %+v, want allowed", decision)
	}
	if used, held, _ := store.Counters(key); used != 0 || held != 400 {
		t.Fatalf("used/held = %d/%d, want 0/400", used, held)
	}

	if err := store.Settle(context.Background(), Settlement{ID: p.ID, Actuals: []int64{250}}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if used, held, _ := store.Counters(key); used != 250 || held != 0 {
		t.Fatalf("used/held = %d/%d, want 250/0", used, held)
	}
}

func TestRedisReserveIsIdempotent(t *testing.T) {
	store := redisStore(t)
	key := "budget:tenant-b:1"
	p := plan("attempt-1", budgetLine(key, 1000, 400))

	if _, err := store.Reserve(context.Background(), p); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	second, err := store.Reserve(context.Background(), p)
	if err != nil {
		t.Fatalf("second reserve: %v", err)
	}
	if !second.Duplicate {
		t.Fatal("a repeated reserve must be reported as a duplicate")
	}
	if _, held, _ := store.Counters(key); held != 400 {
		t.Fatalf("held = %d, want 400: a retry must not hold twice", held)
	}
	if err := store.Settle(context.Background(), Settlement{ID: p.ID, Actuals: []int64{100}}); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if err := store.Settle(context.Background(), Settlement{ID: p.ID, Actuals: []int64{100}}); err != nil {
		t.Fatalf("second settle: %v", err)
	}
	assertRedisCounters(t, store, key, 100, 0)
}

// The core reason for doing this in Lua: two replicas racing for the last unit
// of budget must not both win.
func TestRedisNeverOversellsUnderConcurrency(t *testing.T) {
	store := redisStore(t)
	key := "budget:tenant-c:1"
	const (
		limit    = int64(1000)
		requests = 200
		perCall  = int64(10)
	)
	var wg sync.WaitGroup
	var granted int64
	var mu sync.Mutex
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			decision, err := store.Reserve(context.Background(), plan(fmt.Sprintf("attempt-%d", i), budgetLine(key, limit, perCall)))
			if err != nil {
				t.Errorf("reserve: %v", err)
				return
			}
			if decision.Allowed {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()

	if granted*perCall > limit {
		t.Fatalf("granted %d reservations worth %d, limit is %d", granted, granted*perCall, limit)
	}
	if _, held, _ := store.Counters(key); held > limit {
		t.Fatalf("held exceeds the limit")
	}
}

func TestRedisSweepReclaimsExpiredHolds(t *testing.T) {
	store := redisStore(t)
	key := "budget:tenant-d:1"
	p := plan("attempt-1", budgetLine(key, 1000, 900))
	p.TTL = time.Second
	if _, err := store.Reserve(context.Background(), p); err != nil {
		t.Fatalf("reserve: %v", err)
	}

	released, err := store.Sweep(context.Background(), []string{key}, time.Now().Add(2*p.TTL))
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if released != 1 {
		t.Fatalf("released = %d, want 1", released)
	}
	if _, held, _ := store.Counters(key); held != 0 {
		t.Fatalf("held must return to 0 after reclaim")
	}

	decision, err := store.Reserve(context.Background(), plan("attempt-2", budgetLine(key, 1000, 900)))
	if err != nil || !decision.Allowed {
		t.Fatalf("reserve after reclaim = %+v, err %v", decision, err)
	}
}

func TestRedisRefusesWithoutConsuming(t *testing.T) {
	store := redisStore(t)
	key := "budget:tenant-e:1"
	// Leave room for one more hold but not for this one.
	if _, err := store.Reserve(context.Background(), plan("attempt-1", budgetLine(key, 100, 90))); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	decision, err := store.Reserve(context.Background(), plan("attempt-2", budgetLine(key, 100, 90)))
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if decision.Allowed {
		t.Fatal("second hold must be refused")
	}
	if _, held, _ := store.Counters(key); held != 90 {
		t.Fatalf("held = %d, want 90: a refusal must not consume anything", held)
	}
}

func TestRedisColdStartSweepWithEmptyKeys(t *testing.T) {
	ctx := context.Background()
	store := redisStore(t)
	p := plan("crashed-attempt",
		budgetLine("budget:cold:1", 100, 90),
		Line{Key: "rpm:cold:1", Kind: KindRequests, Limit: 10, Reserve: 1})
	if decision, err := store.Reserve(ctx, p); err != nil || !decision.Allowed {
		t.Fatalf("reserve = %+v, err %v", decision, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close previous client: %v", err)
	}

	// A new connection pool has never observed any of the previous windows.
	fresh := redisStoreWithPrefix(t, strings.TrimSuffix(store.prefix, ":"))
	if n, err := fresh.Sweep(ctx, nil, time.Now().Add(-p.TTL)); err != nil || n != 0 {
		t.Fatalf("sweep before expiry = %d, err %v", n, err)
	}
	if n, err := fresh.Sweep(ctx, nil, time.Now().Add(2*p.TTL)); err != nil || n != 1 {
		t.Fatalf("cold-start sweep = %d, want 1, err %v", n, err)
	}
	for _, line := range p.Lines {
		assertRedisCounters(t, fresh, line.Key, 0, 0)
		_, _, pend := fresh.counterKeys(line)
		if n := redisTestInt(t, fresh, "ZCARD", pend); n != 0 {
			t.Fatalf("window pending = %d, want 0", n)
		}
	}
	if n := redisTestInt(t, fresh, "ZCARD", fresh.pendingKey()); n != 0 {
		t.Fatalf("global pending = %d, want 0", n)
	}
	p.ID = "replacement-attempt"
	if decision, err := fresh.Reserve(ctx, p); err != nil || !decision.Allowed {
		t.Fatalf("reserve after cold-start reclaim = %+v, err %v", decision, err)
	}
	if err := fresh.Abandon(ctx, p.ID); err != nil {
		t.Fatalf("abandon replacement: %v", err)
	}
}

func TestRedisPendingSurvivesUntilFinalization(t *testing.T) {
	for _, action := range []string{"sweep", "settle", "abandon"} {
		t.Run(action, func(t *testing.T) {
			ctx := context.Background()
			store := redisStore(t)
			store.counterTTL = 10 * time.Millisecond
			p := plan("delayed-attempt", budgetLine("budget:delayed:1", 100, 40))
			p.TTL = 5 * time.Millisecond
			if decision, err := store.Reserve(ctx, p); err != nil || !decision.Allowed {
				t.Fatalf("reserve = %+v, err %v", decision, err)
			}

			// Redis expiry uses wall time: advancing Sweep's now alone would not
			// reproduce loss of the manifest after 8*TTL or of held satellites.
			time.Sleep(10 * p.TTL)
			_, held, pend := store.counterKeys(p.Lines[0])
			for _, key := range []string{store.resKey(p.ID), held, pend, store.pendingKey()} {
				if ttl := redisTestInt(t, store, "PTTL", key); ttl != -1 {
					t.Fatalf("unfinished key %s PTTL = %d, want persistent", key, ttl)
				}
			}
			assertRedisCounters(t, store, p.Lines[0].Key, 0, 40)
			if n := redisTestInt(t, store, "ZCARD", store.pendingKey()); n != 1 {
				t.Fatalf("global pending = %d, want 1", n)
			}

			settlement := Settlement{ID: p.ID, Actuals: []int64{25}}
			var wantUsed int64
			switch action {
			case "sweep":
				if n, err := store.Sweep(ctx, nil, time.Now()); err != nil || n != 1 {
					t.Fatalf("late sweep = %d, want 1, err %v", n, err)
				}
			case "settle":
				if err := store.Settle(ctx, settlement); err != nil {
					t.Fatalf("late settle: %v", err)
				}
				wantUsed = 25
			case "abandon":
				if err := store.Abandon(ctx, p.ID); err != nil {
					t.Fatalf("late abandon: %v", err)
				}
			}
			assertRedisCounters(t, store, p.Lines[0].Key, wantUsed, 0)
			for _, key := range []string{pend, store.pendingKey()} {
				if n := redisTestInt(t, store, "ZCARD", key); n != 0 {
					t.Fatalf("completed index %s has %d entries", key, n)
				}
			}
			if ttl := redisTestInt(t, store, "PTTL", held); ttl == -1 || ttl > store.counterTTL.Milliseconds() {
				t.Fatalf("completed held PTTL = %d, want expiring or absent", ttl)
			}

			// Settled ids keep their existing one-day deduplication TTL. Released
			// ids retain the existing unknown-reservation behavior on late settle.
			var wantErr error
			if action == "settle" {
				ttl := redisTestInt(t, store, "PTTL", store.resKey(p.ID))
				if ttl <= (23*time.Hour).Milliseconds() || ttl > (24*time.Hour).Milliseconds() {
					t.Fatalf("settled manifest PTTL = %d, want approximately one day", ttl)
				}
				if decision, err := store.Reserve(ctx, p); err != nil || !decision.Allowed || !decision.Duplicate {
					t.Fatalf("settled reserve replay = %+v, err %v", decision, err)
				}
			} else {
				wantErr = ErrUnknownReservation
			}
			for i := 0; i < 2; i++ {
				if n, err := store.Sweep(ctx, []string{p.Lines[0].Key}, time.Now()); err != nil || n != 0 {
					t.Fatalf("repeat sweep = %d, want 0, err %v", n, err)
				}
				if err := store.Settle(ctx, settlement); !errors.Is(err, wantErr) {
					t.Fatalf("repeat settle = %v, want %v", err, wantErr)
				}
				if err := store.Abandon(ctx, p.ID); !errors.Is(err, wantErr) {
					t.Fatalf("repeat abandon = %v, want %v", err, wantErr)
				}
				assertRedisCounters(t, store, p.Lines[0].Key, wantUsed, 0)
			}
			if n := redisTestInt(t, store, "ZCARD", store.pendingKey()); n != 0 {
				t.Fatalf("repeated operations recreated %d pending entries", n)
			}
		})
	}
}

func TestRedisHeldCounterTTLOnlyWhenWindowHasNoPending(t *testing.T) {
	ctx := context.Background()
	store := redisStore(t)
	line := budgetLine("budget:shared:1", 100, 40)
	for _, id := range []string{"first", "second"} {
		if decision, err := store.Reserve(ctx, plan(id, line)); err != nil || !decision.Allowed {
			t.Fatalf("reserve %s = %+v, err %v", id, decision, err)
		}
	}
	if err := store.Settle(ctx, Settlement{ID: "first", Actuals: []int64{25}}); err != nil {
		t.Fatalf("settle first: %v", err)
	}
	assertRedisCounters(t, store, line.Key, 25, 40)
	_, held, pend := store.counterKeys(line)
	for _, key := range []string{held, pend, store.pendingKey()} {
		if ttl := redisTestInt(t, store, "PTTL", key); ttl != -1 {
			t.Fatalf("key %s still has pending reservations but PTTL = %d", key, ttl)
		}
	}
	if err := store.Abandon(ctx, "second"); err != nil {
		t.Fatalf("abandon second: %v", err)
	}
	assertRedisCounters(t, store, line.Key, 25, 0)
	if ttl := redisTestInt(t, store, "PTTL", held); ttl <= 0 || ttl > store.counterTTL.Milliseconds() {
		t.Fatalf("empty held PTTL = %d, want existing counter TTL", ttl)
	}

	// A new hold must cancel the empty counter's TTL, not inherit it.
	if decision, err := store.Reserve(ctx, plan("third", line)); err != nil || !decision.Allowed {
		t.Fatalf("reserve third = %+v, err %v", decision, err)
	}
	if ttl := redisTestInt(t, store, "PTTL", held); ttl != -1 {
		t.Fatalf("reused held PTTL = %d, want persistent", ttl)
	}
	if err := store.Abandon(ctx, "third"); err != nil {
		t.Fatalf("abandon third: %v", err)
	}
}

func TestRedisSweepBoundsGlobalBatch(t *testing.T) {
	ctx := context.Background()
	store := redisStore(t)
	line := budgetLine("budget:batch:1", 1000, 1)
	for i := 0; i < 201; i++ {
		if decision, err := store.Reserve(ctx, plan(fmt.Sprintf("expired-%d", i), line)); err != nil || !decision.Allowed {
			t.Fatalf("reserve %d = %+v, err %v", i, decision, err)
		}
	}
	live := plan("live-attempt", line)
	live.TTL = time.Hour
	if decision, err := store.Reserve(ctx, live); err != nil || !decision.Allowed {
		t.Fatalf("reserve live = %+v, err %v", decision, err)
	}
	now := time.Now().Add(2 * time.Minute)
	for i, want := range []int{200, 1, 0} {
		var hints []string
		if i == 0 {
			hints = []string{line.Key, line.Key, "unobserved-window"}
		}
		if n, err := store.Sweep(ctx, hints, now); err != nil || n != want {
			t.Fatalf("sweep batch %d = %d, want %d, err %v", i, n, want, err)
		}
		wantHeld := int64(1)
		if i == 0 {
			wantHeld++
		}
		assertRedisCounters(t, store, line.Key, 0, wantHeld)
		_, held, pend := store.counterKeys(line)
		for _, key := range []string{pend, store.pendingKey()} {
			if n := redisTestInt(t, store, "ZCARD", key); n != wantHeld {
				t.Fatalf("pending entries = %d, want %d", n, wantHeld)
			}
		}
		if ttl := redisTestInt(t, store, "PTTL", held); ttl != -1 {
			t.Fatalf("live held PTTL = %d, want persistent", ttl)
		}
	}
	if err := store.Abandon(ctx, live.ID); err != nil {
		t.Fatalf("abandon live: %v", err)
	}
}

func TestRedisDeniedKindMatchesRefusingWindow(t *testing.T) {
	for _, kind := range []Kind{KindRequests, KindTokens, KindBudget} {
		t.Run(string(kind), func(t *testing.T) {
			ctx := context.Background()
			store := redisStore(t)
			// Opaque names ensure Kind comes from the matched plan line, not the
			// Redis key spelling or the first line in the plan.
			denied := Line{Key: "opaque:refusing:1", Kind: kind, Limit: 100, Reserve: 90}
			if decision, err := store.Reserve(ctx, plan("existing", denied)); err != nil || !decision.Allowed {
				t.Fatalf("reserve existing = %+v, err %v", decision, err)
			}
			first := budgetLine("opaque:available:1", 100, 40)
			decision, err := store.Reserve(ctx, plan("refused", first, denied))
			used, _, _ := store.counterKeys(denied)
			if err != nil || decision.Allowed || decision.DeniedKind != kind || decision.DeniedKey != used || decision.Remaining != 10 {
				t.Fatalf("decision = %+v, want %s refusal with 10 remaining, err %v", decision, kind, err)
			}
			assertRedisCounters(t, store, first.Key, 0, 0)
			assertRedisCounters(t, store, denied.Key, 0, 90)
			if n := redisTestInt(t, store, "EXISTS", store.resKey("refused")); n != 0 {
				t.Fatal("refusal created a manifest")
			}
			if n := redisTestInt(t, store, "ZCARD", store.pendingKey()); n != 1 {
				t.Fatalf("global pending = %d, refusal must not add an entry", n)
			}
			if err := store.Abandon(ctx, "existing"); err != nil {
				t.Fatalf("abandon existing: %v", err)
			}
		})
	}
}
