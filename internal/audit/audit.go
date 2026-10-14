// Package audit emits structured usage and quota events for reconciliation.
//
// The recorder is asynchronous and never blocks the request path: events go onto
// a bounded queue and are written by a background worker. When the queue is full
// the recorder drops events and counts them, because slowing down or failing a
// request is worse than losing an audit line. Dropped counts are observable.
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// UsageEvent is one settled request. Together these records let operators trace
// spend by tenant, model alias and provider, and reconcile it against upstream
// invoices.
type UsageEvent struct {
	Timestamp       time.Time `json:"timestamp"`
	RequestID       string    `json:"request_id"`
	AttemptID       string    `json:"attempt_id"`
	Tenant          string    `json:"tenant"`
	Client          string    `json:"client_id"`
	Model           string    `json:"model"`
	UpstreamModel   string    `json:"upstream_model"`
	Provider        string    `json:"provider"`
	Status          int       `json:"status"`
	DurationMS      int64     `json:"duration_ms"`
	InputTokens     int64     `json:"input_tokens"`
	OutputTokens    int64     `json:"output_tokens"`
	UsageReported   bool      `json:"usage_reported"`
	Reconciliation  string    `json:"reconciliation_status"`
	StreamComplete  string    `json:"stream_completion"`
	Currency        string    `json:"currency,omitempty"`
	PriceVersion    string    `json:"price_version,omitempty"`
	PriceFound      bool      `json:"price_found"`
	TotalMicros     int64     `json:"total_micros"`
	InputCostMicros int64     `json:"input_cost_micros"`
	OutputCostMicro int64     `json:"output_cost_micros"`
}

// QuotaEvent records a quota decision, which is what makes an overrun or a false
// rejection explainable after the fact.
type QuotaEvent struct {
	Timestamp   time.Time `json:"timestamp"`
	RequestID   string    `json:"request_id"`
	AttemptID   string    `json:"attempt_id"`
	Tenant      string    `json:"tenant"`
	Model       string    `json:"model"`
	Provider    string    `json:"provider"`
	Action      string    `json:"action"`
	Denied      bool      `json:"denied"`
	WindowKey   string    `json:"window_key,omitempty"`
	LimitMicros int64     `json:"limit_micros"`
	UsedMicros  int64     `json:"used_micros"`
	Reason      string    `json:"reason"`
	Degraded    bool      `json:"degraded"`
}

// Sink receives events. Implementations must be safe for concurrent use.
type Sink interface {
	WriteUsage(ctx context.Context, event UsageEvent) error
	WriteQuota(ctx context.Context, event QuotaEvent) error
	Close() error
}

// Recorder fans events out to sinks from a single background worker.
type Recorder struct {
	sinks   []Sink
	usageCh chan UsageEvent
	quotaCh chan QuotaEvent

	wg       sync.WaitGroup
	stopOnce sync.Once
	done     chan struct{}

	dropped atomic.Uint64
	written atomic.Uint64
	failed  atomic.Uint64
}

// NewRecorder builds a recorder. Call Start to begin draining the queues.
func NewRecorder(sinks []Sink, buffer int) *Recorder {
	if buffer <= 0 {
		buffer = 1024
	}
	return &Recorder{
		sinks:   sinks,
		usageCh: make(chan UsageEvent, buffer),
		quotaCh: make(chan QuotaEvent, buffer),
		done:    make(chan struct{}),
	}
}

// Start launches the worker goroutine.
func (r *Recorder) Start() {
	r.wg.Add(1)
	go r.run()
}

func (r *Recorder) run() {
	defer r.wg.Done()
	for {
		select {
		case <-r.done:
			r.drain()
			return
		case event := <-r.usageCh:
			r.emitUsage(event)
		case event := <-r.quotaCh:
			r.emitQuota(event)
		}
	}
}

func (r *Recorder) emitUsage(event UsageEvent) {
	for _, sink := range r.sinks {
		if err := sink.WriteUsage(context.Background(), event); err != nil {
			r.failed.Add(1)
			continue
		}
		r.written.Add(1)
	}
}

func (r *Recorder) emitQuota(event QuotaEvent) {
	for _, sink := range r.sinks {
		if err := sink.WriteQuota(context.Background(), event); err != nil {
			r.failed.Add(1)
			continue
		}
		r.written.Add(1)
	}
}

func (r *Recorder) drain() {
	for {
		select {
		case event := <-r.usageCh:
			r.emitUsage(event)
		case event := <-r.quotaCh:
			r.emitQuota(event)
		default:
			return
		}
	}
}

// Usage queues a usage event without blocking the caller.
func (r *Recorder) Usage(event UsageEvent) {
	select {
	case r.usageCh <- event:
	default:
		r.dropped.Add(1)
	}
}

// Quota queues a quota event without blocking the caller.
func (r *Recorder) Quota(event QuotaEvent) {
	select {
	case r.quotaCh <- event:
	default:
		r.dropped.Add(1)
	}
}

// Dropped returns how many events were discarded because the queue was full.
func (r *Recorder) Dropped() uint64 { return r.dropped.Load() }

// Written returns how many sink writes succeeded.
func (r *Recorder) Written() uint64 { return r.written.Load() }

// Failed returns how many sink writes errored.
func (r *Recorder) Failed() uint64 { return r.failed.Load() }

// Stop drains what is queued and closes every sink.
func (r *Recorder) Stop() error {
	r.stopOnce.Do(func() { close(r.done) })
	r.wg.Wait()
	var errs []error
	for _, sink := range r.sinks {
		if err := sink.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// StdoutSink writes newline-delimited JSON to a writer.
type StdoutSink struct {
	mu      sync.Mutex
	encoder *json.Encoder
	out     io.Writer
}

// NewStdoutSink writes to the supplied writer, usually os.Stdout.
func NewStdoutSink(out io.Writer) *StdoutSink {
	return &StdoutSink{encoder: json.NewEncoder(out), out: out}
}

func (s *StdoutSink) WriteUsage(_ context.Context, event UsageEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.encoder.Encode(event)
}

func (s *StdoutSink) WriteQuota(_ context.Context, event QuotaEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.encoder.Encode(event)
}

func (s *StdoutSink) Close() error { return nil }

// FileSink appends newline-delimited JSON to a file. It is the built-in durable
// option: durable enough to query later, and replaceable by a queue or warehouse
// sink without touching the gateway.
type FileSink struct {
	mu   sync.Mutex
	file *os.File
}

// NewFileSink opens the audit file for appending, creating it if needed.
func NewFileSink(path string) (*FileSink, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &FileSink{file: file}, nil
}

func (s *FileSink) WriteUsage(_ context.Context, event UsageEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.NewEncoder(s.file).Encode(event)
}

func (s *FileSink) WriteQuota(_ context.Context, event QuotaEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.NewEncoder(s.file).Encode(event)
}

func (s *FileSink) Close() error { return s.file.Close() }

// MultiSink writes each event to every sink it wraps.
type MultiSink struct{ sinks []Sink }

// NewMultiSink combines sinks.
func NewMultiSink(sinks ...Sink) *MultiSink { return &MultiSink{sinks: sinks} }

func (m *MultiSink) WriteUsage(ctx context.Context, event UsageEvent) error {
	var errs []error
	for _, sink := range m.sinks {
		errs = append(errs, sink.WriteUsage(ctx, event))
	}
	return errors.Join(errs...)
}

func (m *MultiSink) WriteQuota(ctx context.Context, event QuotaEvent) error {
	var errs []error
	for _, sink := range m.sinks {
		errs = append(errs, sink.WriteQuota(ctx, event))
	}
	return errors.Join(errs...)
}

func (m *MultiSink) Close() error {
	var errs []error
	for _, sink := range m.sinks {
		errs = append(errs, sink.Close())
	}
	return errors.Join(errs...)
}
