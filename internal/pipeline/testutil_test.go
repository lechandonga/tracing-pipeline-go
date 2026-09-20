package pipeline

import (
	"github.com/lechandonga/tracing-pipeline-go/internal/model"
)

// testTraceID builds a valid trace id with the given final hex nibble.
func testTraceID(nibble byte) model.TraceID {
	b := make([]byte, 32)
	for i := range b {
		b[i] = '0'
	}
	b[31] = nibble
	return model.TraceID(b)
}

func testSpanID(s string) model.SpanID {
	b := make([]byte, 16)
	for i := range b {
		b[i] = '0'
	}
	copy(b, s)
	return model.SpanID(b)
}
