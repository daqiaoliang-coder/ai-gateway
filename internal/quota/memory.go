package quota

import (
	"context"
	"sort"
	"sync"
	"time"
)

// MemoryStore is a process-local store. It is correct on its own terms: every
// guarantee the interface promises (idempotency, expiry, no double spend) holds
// inside one process. It does not coordinate across replicas, so it is for single
// instance deployments or as the degrade-mode fallback.
type MemoryStore struct {
	mu       sync.Mutex
	used     map[string]int64
	held     map[string]int64
	reserved map[string]memoryHold
	clock    func() time.Time
}

type memoryHold struct {
	plan      Plan
	granted   bool
	settled   bool
	expireAt  time.Time
	settledAt time.Time
}

// NewMemoryStore builds an empty store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		used:     make(map[string]int64),
		held:     make(map[string]int64),
		reserved: make(map[string]memoryHold),
		clock:    time.Now,
	}
}

// Reserve holds every line or none, and replays an existing decision for a
// repeated id.
func (s *MemoryStore) Reserve(_ context.Context, plan Plan) (Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.clock()
	s.sweepLocked(now)

	if existing, ok := s.reserved[plan.ID]; ok {
		return Decision{Allowed: existing.granted, Duplicate: true, Reason: "reservation_replayed"}, nil
	}
	for _, line := range plan.Lines {
		if line.Limit <= 0 {
			continue
		}
		available := line.Limit - s.used[line.Key] - s.held[line.Key]
		if available < line.Reserve {
			return Decision{
				Allowed:    false,
				DeniedKey:  line.Key,
				DeniedKind: line.Kind,
				Remaining:  available,
				Reason:     "limit_exhausted",
			}, nil
		}
	}
	for _, line := range plan.Lines {
		if line.Limit <= 0 {
			continue
		}
		s.held[line.Key] += line.Reserve
	}
	s.reserved[plan.ID] = memoryHold{plan: plan, granted: true, expireAt: now.Add(plan.TTL)}
	return Decision{Allowed: true, Reason: "granted"}, nil
}

// Settle converts held amounts into real usage. Repeating an id is a no-op.
func (s *MemoryStore) Settle(_ context.Context, settlement Settlement) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hold, ok := s.reserved[settlement.ID]
	if !ok {
		return ErrUnknownReservation
	}
	if hold.settled {
		return nil
	}
	for i, line := range hold.plan.Lines {
		if line.Limit <= 0 {
			continue
		}
		s.held[line.Key] -= line.Reserve
		if i < len(settlement.Actuals) {
			s.used[line.Key] += settlement.Actuals[i]
		}
	}
	hold.settled = true
	hold.settledAt = s.clock()
	s.reserved[settlement.ID] = hold
	return nil
}

// Abandon returns held amounts without consuming them.
func (s *MemoryStore) Abandon(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	hold, ok := s.reserved[id]
	if !ok {
		return ErrUnknownReservation
	}
	if hold.settled {
		return nil
	}
	for _, line := range hold.plan.Lines {
		if line.Limit <= 0 {
			continue
		}
		s.held[line.Key] -= line.Reserve
	}
	delete(s.reserved, id)
	return nil
}

// Close clears all state.
func (s *MemoryStore) Close() error { return nil }

// Sweep releases holds whose TTL expired, which is what stops a crashed request
// from leaking budget forever. The key hint is unused: this store already knows
// every hold it owns.
func (s *MemoryStore) Sweep(_ context.Context, _ []string, now time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sweepLocked(now), nil
}

func (s *MemoryStore) sweepLocked(now time.Time) int {
	expired := make([]string, 0)
	for id, hold := range s.reserved {
		if hold.settled {
			// A settled hold is only kept long enough to make a duplicate settle
			// a no-op, then dropped so the store does not grow forever.
			if now.After(hold.settledAt.Add(hold.plan.TTL)) {
				expired = append(expired, id)
			}
			continue
		}
		if now.After(hold.expireAt) {
			expired = append(expired, id)
		}
	}
	sort.Strings(expired)
	for _, id := range expired {
		hold := s.reserved[id]
		if hold.settled {
			delete(s.reserved, id)
			continue
		}
		for _, line := range hold.plan.Lines {
			if line.Limit <= 0 {
				continue
			}
			s.held[line.Key] -= line.Reserve
		}
		delete(s.reserved, id)
	}
	return len(expired)
}

// Used reports consumed amounts. Tests and reconciliation use it.
func (s *MemoryStore) Used(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.used[key]
}

// Held reports amounts held by unsettled attempts.
func (s *MemoryStore) Held(key string) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.held[key]
}

// PendingIDs lists reservations that are still being held.
func (s *MemoryStore) PendingIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, len(s.reserved))
	for id, hold := range s.reserved {
		if !hold.settled {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
