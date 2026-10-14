package audit

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

type recordingSink struct {
	mu    sync.Mutex
	usage []UsageEvent
	quota []QuotaEvent
}

func (s *recordingSink) WriteUsage(_ context.Context, event UsageEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.usage = append(s.usage, event)
	return nil
}

func (s *recordingSink) WriteQuota(_ context.Context, event QuotaEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quota = append(s.quota, event)
	return nil
}

func (s *recordingSink) Close() error { return nil }

func (s *recordingSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.usage) + len(s.quota)
}

type blockingSink struct {
	gate chan struct{}
}

func newBlockingSink() *blockingSink { return &blockingSink{gate: make(chan struct{})} }

func (s *blockingSink) WriteUsage(context.Context, UsageEvent) error {
	<-s.gate
	return nil
}
func (s *blockingSink) WriteQuota(context.Context, QuotaEvent) error { return nil }
func (s *blockingSink) Close() error                                 { return nil }
func (s *blockingSink) release()                                     { close(s.gate) }

func TestRecorderWritesUsageEvents(t *testing.T) {
	var sink recordingSink
	recorder := NewRecorder([]Sink{&sink}, 8)
	recorder.Start()
	defer func() { _ = recorder.Stop() }()

	recorder.Usage(UsageEvent{RequestID: "r1", Tenant: "t1", Model: "fast", Provider: "openai-primary", TotalMicros: 42})
	recorder.Usage(UsageEvent{RequestID: "r2", Tenant: "t1", Model: "fast", Provider: "compatible-backup", TotalMicros: 7})

	deadline := time.Now().Add(2 * time.Second)
	for sink.count() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := len(sink.usage); got != 2 {
		t.Fatalf("usage events = %d, want 2", got)
	}
	if sink.usage[0].TotalMicros != 42 {
		t.Fatalf("total = %d, want 42", sink.usage[0].TotalMicros)
	}
}

// The recorder must never block a request: a full queue drops and says so.
func TestRecorderDropsInsteadOfBlocking(t *testing.T) {
	sink := newBlockingSink()
	recorder := NewRecorder([]Sink{sink}, 1)
	recorder.Start()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 50; i++ {
			recorder.Usage(UsageEvent{RequestID: "r"})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("queuing blocked, which would stall requests")
	}
	if recorder.Dropped() == 0 {
		t.Fatal("expected drops with a blocked sink")
	}
	sink.release()
	if err := recorder.Stop(); err != nil {
		t.Fatalf("stop: %v", err)
	}
}

func TestFileSinkAppendsJSONLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	sink, err := NewFileSink(path)
	if err != nil {
		t.Fatalf("file sink: %v", err)
	}
	if err := sink.WriteUsage(context.Background(), UsageEvent{RequestID: "r1", TotalMicros: 5}); err != nil {
		t.Fatalf("write usage: %v", err)
	}
	if err := sink.WriteQuota(context.Background(), QuotaEvent{RequestID: "r1", Action: "reserve", Denied: true}); err != nil {
		t.Fatalf("write quota: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines = %d, want 2: %q", len(lines), raw)
	}
	var event UsageEvent
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatalf("decode %q: %v", lines[0], err)
	}
	if event.RequestID != "r1" || event.TotalMicros != 5 {
		t.Fatalf("event = %+v, want r1 with 5 micros", event)
	}
}

func TestStdoutSinkEncodesEveryEvent(t *testing.T) {
	var buffer strings.Builder
	sink := NewStdoutSink(&buffer)
	if err := sink.WriteUsage(context.Background(), UsageEvent{RequestID: "r1"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if err := sink.WriteQuota(context.Background(), QuotaEvent{RequestID: "r1", Action: "reserve"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if lines := strings.Count(strings.TrimSpace(buffer.String()), "\n"); lines != 1 {
		t.Fatalf("expected 2 newline-delimited events, got %d newlines", lines)
	}
}

func TestMultiSinkFansOut(t *testing.T) {
	first := &recordingSink{}
	second := &recordingSink{}
	sink := NewMultiSink(first, second)
	if err := sink.WriteUsage(context.Background(), UsageEvent{RequestID: "r1"}); err != nil {
		t.Fatalf("write: %v", err)
	}
	if first.count() != 1 || second.count() != 1 {
		t.Fatalf("counts = %d/%d, want 1/1", first.count(), second.count())
	}
}
