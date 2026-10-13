package gateway

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestReadEventLineReturnsCompleteLine(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("data: first\ndata: second\n"))
	line, err := readEventLine(reader, 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(line) != "data: first\n" {
		t.Fatalf("line = %q, want %q", line, "data: first\n")
	}
}

func TestReadEventLineRefusesToBufferOversizedEvent(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader(strings.Repeat("x", 100000) + "\n"))

	line, err := readEventLine(reader, 4096)

	if !errors.Is(err, errEventTooLarge) {
		t.Fatalf("err = %v, want %v", err, errEventTooLarge)
	}
	if line != nil {
		t.Fatalf("line = %q, want no buffered bytes", line)
	}
}

func TestReadEventLineReturnsPartialDataAtEOF(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("data: tail"))

	line, err := readEventLine(reader, 1024)

	if !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want io.EOF", err)
	}
	if string(line) != "data: tail" {
		t.Fatalf("line = %q, want %q", line, "data: tail")
	}
}

func TestEventData(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"data: {\"a\":1}\n", "{\"a\":1}"},
		{"data:[DONE]\n", "[DONE]"},
		{"data: [DONE]\n", "[DONE]"},
		{"event: ping\n", ""},
		{"\n", ""},
		{": comment\n", ""},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if got := eventData([]byte(tt.input)); got != tt.want {
				t.Fatalf("eventData(%q) = %q, want %q", tt.input, got, tt.want)
			}
		})
	}
}
