package gateway

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/daqiaoliang-coder/ai-gateway/internal/policy"
)

var (
	errEventTooLarge   = errors.New("upstream SSE event exceeded the configured limit")
	errStreamTooLarge  = errors.New("upstream SSE response exceeded the configured limit")
	errNoTerminalEvent = errors.New("upstream stream ended without a terminal event")
)

// copyFailure records who caused a failed response copy. It decides whether the
// circuit breaker may blame the provider.
type copyFailure int

const (
	failNone copyFailure = iota
	failUpstream
	failClient
	failGateway
)

// streamResult is the outcome of forwarding a streamed response.
type streamResult struct {
	input      int64
	output     int64
	completion policy.StreamCompletion
	fail       copyFailure
	err        error
}

// copyStream forwards an SSE response with hard limits on a single event and on
// the whole stream. The event limit is enforced while reading, so a single
// oversized event can never be buffered whole.
func (g *Gateway) copyStream(w http.ResponseWriter, ctx context.Context, body io.Reader) streamResult {
	reader := bufio.NewReaderSize(body, 8192)
	var input, output, total int64
	sawDone := false

	for {
		line, readErr := readEventLine(reader, g.streamMaxEvent)
		if len(line) > 0 {
			total += int64(len(line))
			if total > g.streamMaxBytes {
				return streamResult{input, output, policy.StreamTruncated, failUpstream, errStreamTooLarge}
			}
			switch data := eventData(line); {
			case data == "[DONE]":
				sawDone = true
			case data != "":
				if in, out := extractUsage([]byte(data)); in > 0 || out > 0 {
					input, output = in, out
				}
			}
			if _, writeErr := w.Write(line); writeErr != nil {
				return streamResult{input, output, policy.StreamAbortedClient, failClient, writeErr}
			}
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		if readErr == nil {
			continue
		}
		if errors.Is(readErr, io.EOF) {
			if sawDone {
				return streamResult{input, output, policy.StreamCompleted, failNone, nil}
			}
			// A stream that never reached its terminal event is incomplete and
			// must not be counted as a successful request.
			return streamResult{input, output, policy.StreamAbortedUpstream, failUpstream, errNoTerminalEvent}
		}
		if errors.Is(readErr, errEventTooLarge) {
			return streamResult{input, output, policy.StreamTruncated, failUpstream, readErr}
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			if errors.Is(ctxErr, context.DeadlineExceeded) {
				return streamResult{input, output, policy.StreamAbortedGateway, failGateway, ctxErr}
			}
			return streamResult{input, output, policy.StreamAbortedClient, failClient, ctxErr}
		}
		return streamResult{input, output, policy.StreamAbortedUpstream, failUpstream, readErr}
	}
}

// readEventLine reads one SSE line while refusing to buffer more than max
// bytes. It returns the bytes read so far together with the read error.
func readEventLine(reader *bufio.Reader, max int64) ([]byte, error) {
	var line []byte
	for {
		chunk, err := reader.ReadSlice('\n')
		line = append(line, chunk...)
		if int64(len(line)) > max {
			return nil, errEventTooLarge
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		return line, err
	}
}

func eventData(line []byte) string {
	trimmed := strings.TrimSpace(string(line))
	if !strings.HasPrefix(trimmed, "data:") {
		return ""
	}
	return strings.TrimSpace(strings.TrimPrefix(trimmed, "data:"))
}
