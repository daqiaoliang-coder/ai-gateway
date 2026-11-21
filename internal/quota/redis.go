package quota

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// All decision logic lives in these Lua scripts so that a multi-line check and
// update happens atomically inside Redis. Doing read-modify-write from Go would
// let two replicas grant the same last unit of budget.
//
// Every counter key has three satellites:
//
//	<key>:u  consumed amount, incremented at settle time
//	<key>:h  amount held by unsettled attempts
//	<key>:p  sorted set of unsettled attempt ids scored by expiry
//
// A global pending index enables cold-start sweeping; these scripts require standalone Redis.
const (
	scriptReserve = `
local resKey = KEYS[1]
local raw = ARGV[1]
local m = cjson.decode(raw)
local st = redis.call('HGET', resKey, 'st')
if st then
  return cjson.encode({allowed = true, duplicate = true, state = st})
end
local i = 1
while i <= table.getn(m.lines) do
  local l = m.lines[i]
  if l.limit > 0 then
    local used = tonumber(redis.call('GET', l.u) or '0')
    local held = tonumber(redis.call('GET', l.h) or '0')
    if used + held + l.reserve > l.limit then
      return cjson.encode({allowed = false, u = l.u, remaining = l.limit - used - held})
    end
  end
  i = i + 1
end
i = 1
while i <= table.getn(m.lines) do
  local l = m.lines[i]
  if l.limit > 0 then
    redis.call('INCRBY', l.h, l.reserve)
    redis.call('ZADD', l.p, m.now_ms + m.ttl_ms, m.id)
    redis.call('PERSIST', l.h)
    redis.call('PERSIST', l.p)
  end
  i = i + 1
end
redis.call('HSET', resKey, 'st', 'held', 'm', raw)
redis.call('PERSIST', resKey)
redis.call('ZADD', KEYS[2], m.now_ms + m.ttl_ms, m.id)
return cjson.encode({allowed = true})
`

	scriptSettle = `
local resKey = KEYS[1]
local a = cjson.decode(ARGV[1])
local st = redis.call('HGET', resKey, 'st')
if not st then return cjson.encode({unknown = true}) end
if st == 'settled' then return cjson.encode({duplicate = true}) end
local m = cjson.decode(redis.call('HGET', resKey, 'm'))
local i = 1
while i <= table.getn(m.lines) do
  local l = m.lines[i]
  if l.limit > 0 then
    local actual = 0
    if a.actuals[i] then actual = a.actuals[i] end
    redis.call('DECRBY', l.h, l.reserve)
    if actual > 0 then redis.call('INCRBY', l.u, actual) end
    redis.call('ZREM', l.p, m.id)
    if redis.call('ZCARD', l.p) == 0 and l.ttl > 0 then
      redis.call('PEXPIRE', l.h, l.ttl)
    end
  end
  i = i + 1
end
redis.call('ZREM', KEYS[2], m.id)
redis.call('HSET', resKey, 'st', 'settled', 'a', ARGV[1])
redis.call('PEXPIRE', resKey, 86400000)
return cjson.encode({ok = true})
`

	scriptAbandon = `
local resKey = KEYS[1]
local st = redis.call('HGET', resKey, 'st')
if not st then return cjson.encode({unknown = true}) end
if st == 'settled' then return cjson.encode({duplicate = true}) end
local m = cjson.decode(redis.call('HGET', resKey, 'm'))
local i = 1
while i <= table.getn(m.lines) do
  local l = m.lines[i]
  if l.limit > 0 then
    redis.call('DECRBY', l.h, l.reserve)
    redis.call('ZREM', l.p, m.id)
    if redis.call('ZCARD', l.p) == 0 and l.ttl > 0 then
      redis.call('PEXPIRE', l.h, l.ttl)
    end
  end
  i = i + 1
end
redis.call('ZREM', KEYS[2], m.id)
redis.call('DEL', resKey)
return cjson.encode({ok = true})
`

	scriptSweep = `
local a = cjson.decode(ARGV[1])
local pendKey = KEYS[1]
local expired = redis.call('ZRANGEBYSCORE', pendKey, '-inf', a.now_ms, 'LIMIT', 0, 200)
local released = 0
local i = 1
while i <= table.getn(expired) do
  local id = expired[i]
  local rk = a.prefix .. id
  local st = redis.call('HGET', rk, 'st')
  local raw = redis.call('HGET', rk, 'm')
  if st == 'held' and raw then
    local m = cjson.decode(raw)
    local j = 1
    while j <= table.getn(m.lines) do
      local l = m.lines[j]
      if l.limit > 0 then
        redis.call('DECRBY', l.h, l.reserve)
        redis.call('ZREM', l.p, m.id)
        if redis.call('ZCARD', l.p) == 0 and l.ttl > 0 then
          redis.call('PEXPIRE', l.h, l.ttl)
        end
      end
      j = j + 1
    end
    redis.call('DEL', rk)
    released = released + 1
  end
  redis.call('ZREM', pendKey, id)
  i = i + 1
end
return released
`
)

type manifestLine struct {
	Used    string `json:"u"`
	Held    string `json:"h"`
	Pend    string `json:"p"`
	Limit   int64  `json:"limit"`
	Reserve int64  `json:"reserve"`
	TTLMS   int64  `json:"ttl"`
}

type manifest struct {
	ID    string         `json:"id"`
	NowMS int64          `json:"now_ms"`
	TTLMS int64          `json:"ttl_ms"`
	Lines []manifestLine `json:"lines"`
}

// RedisConfig configures the shared store.
type RedisConfig struct {
	Addr         string
	PasswordEnv  string
	DB           int
	PoolSize     int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	KeyPrefix    string
}

type RedisStore struct {
	pool   *redisPool
	prefix string
	// Active holds must survive until finalization, regardless of this cleanup TTL.
	counterTTL time.Duration
}

// NewRedisStore connects to Redis and verifies it answers before returning, so a
// misconfigured deployment fails at startup instead of silently degrading.
func NewRedisStore(cfg RedisConfig) (*RedisStore, error) {
	if cfg.Addr == "" {
		return nil, errors.New("redis address is required for a shared quota store")
	}
	password := ""
	if cfg.PasswordEnv != "" {
		password = strings.TrimSpace(os.Getenv(cfg.PasswordEnv))
	}
	pool := newRedisPool(redisConfig{
		addr:         cfg.Addr,
		password:     password,
		db:           cfg.DB,
		poolSize:     cfg.PoolSize,
		dialTimeout:  cfg.DialTimeout,
		readTimeout:  cfg.ReadTimeout,
		writeTimeout: cfg.WriteTimeout,
	})
	if err := pool.Ping(); err != nil {
		_ = pool.Close()
		return nil, err
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "ai-gateway"
	}
	return &RedisStore{pool: pool, prefix: cfg.KeyPrefix + ":", counterTTL: 48 * time.Hour}, nil
}

func (s *RedisStore) counterKeys(line Line) (used, held, pend string) {
	base := s.prefix + line.Key
	return base + ":u", base + ":h", base + ":p"
}

func (s *RedisStore) resKey(id string) string { return s.prefix + "res:" + id }
func (s *RedisStore) pendingKey() string      { return s.prefix + "pending" }

// Reserve grants every line atomically, or refuses without consuming anything.
func (s *RedisStore) Reserve(ctx context.Context, plan Plan) (Decision, error) {
	if len(plan.Lines) == 0 {
		return Decision{Allowed: true, Reason: "no_limits_configured"}, nil
	}
	now := time.Now()
	value := manifest{
		ID:    plan.ID,
		NowMS: now.UnixMilli(),
		TTLMS: plan.TTL.Milliseconds(),
		Lines: make([]manifestLine, 0, len(plan.Lines)),
	}
	for _, line := range plan.Lines {
		used, held, pend := s.counterKeys(line)
		value.Lines = append(value.Lines, manifestLine{
			Used:    used,
			Held:    held,
			Pend:    pend,
			Limit:   line.Limit,
			Reserve: line.Reserve,
			TTLMS:   int64(s.counterTTL / time.Millisecond),
		})
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return Decision{}, err
	}
	reply, err := s.pool.evalScript(scriptReserve, 2, []string{s.resKey(plan.ID), s.pendingKey()}, []string{string(raw)})
	if err != nil {
		return Decision{}, classifyRedisError(err)
	}
	if reply.IsNil() {
		return Decision{}, ErrUnavailable
	}
	var result struct {
		Allowed   bool   `json:"allowed"`
		DeniedKey string `json:"u"`
		Remaining int64  `json:"remaining"`
		Duplicate bool   `json:"duplicate"`
	}
	if err := json.Unmarshal([]byte(reply.Text()), &result); err != nil {
		return Decision{}, fmt.Errorf("decode reserve reply: %w", err)
	}
	if result.Allowed {
		return Decision{Allowed: true, Duplicate: result.Duplicate, Reason: "granted"}, nil
	}
	var deniedKind Kind
	for _, line := range plan.Lines {
		used, _, _ := s.counterKeys(line)
		if used == result.DeniedKey {
			deniedKind = line.Kind
			break
		}
	}
	return Decision{
		Allowed:    false,
		DeniedKey:  result.DeniedKey,
		DeniedKind: deniedKind,
		Remaining:  result.Remaining,
		Reason:     "limit_exhausted",
	}, nil
}

// Settle converts held amounts into consumed amounts using measured usage.
func (s *RedisStore) Settle(ctx context.Context, settlement Settlement) error {
	payload, err := json.Marshal(map[string]any{"actuals": settlement.Actuals})
	if err != nil {
		return err
	}
	reply, err := s.pool.evalScript(scriptSettle, 2, []string{s.resKey(settlement.ID), s.pendingKey()}, []string{string(payload)})
	if err != nil {
		return classifyRedisError(err)
	}
	return resultError(reply)
}

// Abandon returns held amounts untouched.
func (s *RedisStore) Abandon(ctx context.Context, id string) error {
	reply, err := s.pool.evalScript(scriptAbandon, 2, []string{s.resKey(id), s.pendingKey()}, nil)
	if err != nil {
		return classifyRedisError(err)
	}
	return resultError(reply)
}

// The shared index enables cold-start recovery without unbounded keyspace scans.
func (s *RedisStore) Sweep(ctx context.Context, _ []string, now time.Time) (int, error) {
	payload, err := json.Marshal(map[string]any{"now_ms": now.UnixMilli(), "prefix": s.prefix + "res:"})
	if err != nil {
		return 0, err
	}
	reply, err := s.pool.evalScript(scriptSweep, 1, []string{s.pendingKey()}, []string{string(payload)})
	if err != nil {
		return 0, classifyRedisError(err)
	}
	if reply.IsNil() {
		return 0, nil
	}
	released, err := strconv.ParseInt(reply.Text(), 10, 64)
	if err != nil {
		return 0, nil
	}
	return int(released), nil
}

// Counters reports the consumed and held amounts for one window. Operators use
// it to inspect a tenant's position without guessing at key names.
func (s *RedisStore) Counters(lineKey string) (used, held int64, err error) {
	base := s.prefix + lineKey
	usedReply, err := s.pool.run([]string{"GET", base + ":u"})
	if err != nil && !usedReply.IsNil() {
		return 0, 0, err
	}
	heldReply, err := s.pool.run([]string{"GET", base + ":h"})
	if err != nil && !heldReply.IsNil() {
		return 0, 0, err
	}
	if !usedReply.IsNil() {
		used, _ = strconv.ParseInt(usedReply.Text(), 10, 64)
	}
	if !heldReply.IsNil() {
		held, _ = strconv.ParseInt(heldReply.Text(), 10, 64)
	}
	return used, held, nil
}

func (s *RedisStore) HasReservation(ctx context.Context, id string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	reply, err := s.pool.run([]string{"EXISTS", s.resKey(id)})
	if err != nil {
		return false, classifyRedisError(err)
	}
	if reply.typ != ':' {
		return false, fmt.Errorf("unexpected reservation lookup response")
	}
	return reply.Int() != 0, nil
}

// Close releases the connection pool.
func (s *RedisStore) Close() error { return s.pool.Close() }

func classifyRedisError(err error) error {
	if errors.Is(err, ErrUnavailable) {
		return ErrUnavailable
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	// Transportation failures are the ones the manager can degrade around;
	// script errors are bugs and must surface.
	return ErrUnavailable
}

func resultError(reply redisValue) error {
	if reply.IsNil() {
		return ErrUnavailable
	}
	var result struct {
		Unknown   bool `json:"unknown"`
		Duplicate bool `json:"duplicate"`
	}
	_ = json.Unmarshal([]byte(reply.Text()), &result)
	if result.Unknown {
		return ErrUnknownReservation
	}
	return nil
}
