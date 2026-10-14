package quota

import (
	"time"

	"github.com/daqiaoliang-coder/ai-gateway/internal/policy"
)

// NewSharedStore builds the store the contract asks for.
//
// In shared mode the gateway refuses to start when Redis cannot be reached,
// because silently running with process-local counters would look healthy while
// letting every replica hand out the tenant's full budget. Failures after startup
// are handled by the manager's degradation policy instead.
func NewSharedStore(contract *policy.Contract) (Store, error) {
	if contract == nil {
		contract = policy.Default()
	}
	if contract.Quota.Store.Mode == "local" {
		return NewMemoryStore(), nil
	}
	cfg := RedisConfig{
		Addr:        contract.Quota.Store.Redis.Addr,
		PasswordEnv: contract.Quota.Store.Redis.PasswordEnv,
		DB:          contract.Quota.Store.Redis.DB,
		PoolSize:    contract.Quota.Store.Redis.PoolSize,
		KeyPrefix:   contract.Quota.Store.Redis.KeyPrefix,
	}
	cfg.DialTimeout = parseOrZero(contract.Quota.Store.Redis.DialTimeout, 2*time.Second)
	cfg.ReadTimeout = parseOrZero(contract.Quota.Store.Redis.ReadTimeout, 2*time.Second)
	cfg.WriteTimeout = parseOrZero(contract.Quota.Store.Redis.WriteTimeout, 2*time.Second)
	return NewRedisStore(cfg)
}

// NewManagerFromContract wires the shared store, its local fallback and the
// degradation policy into one manager.
func NewManagerFromContract(contract *policy.Contract, counters *Counters) (*Manager, error) {
	shared, err := NewSharedStore(contract)
	if err != nil {
		return nil, err
	}
	return NewManager(shared, NewMemoryStore(), contract.Quota.Store.OnUnavailable, contract.Quota.Store.DegradeFactor, counters), nil
}

func parseOrZero(raw string, fallback time.Duration) time.Duration {
	if raw == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return fallback
	}
	return parsed
}
