package quota

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"
)

// These tests need a real Redis, because the store's correctness lives in Lua
// scripts that only Redis can execute. Set AI_GATEWAY_REDIS_TEST_ADDR to run
// them, for example 127.0.0.1:6399 for a throwaway container.
func redisStore(t *testing.T) *RedisStore {
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
		KeyPrefix:    fmt.Sprintf("test-%d", time.Now().UnixNano()),
	})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
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
	if used, _, _ := store.Counters(key); used != 100 {
		t.Fatalf("used = %d, want 100: settling twice must not double-charge", used)
	}
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
	time.Sleep(1500 * time.Millisecond)

	released, err := store.Sweep(context.Background(), []string{key}, time.Now())
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
