// Package model defines the core trace data model used by the local tracing
// pipeline: identifiers, span payloads and the report envelope.
package model

import (
	"encoding/hex"
	"errors"
)

// TraceID is a 16-byte identifier rendered as 32 lowercase hex characters.
// The all-zero value is reserved and invalid.
type TraceID string

// SpanID is an 8-byte identifier rendered as 16 lowercase hex characters.
// The all-zero value is reserved and invalid.
type SpanID string

// SpanKind mirrors the OpenTelemetry span kind enumeration.
type SpanKind int

const (
	KindInternal SpanKind = iota
	KindServer
	KindClient
	KindProducer
	KindConsumer
)

// Valid reports whether k is one of the documented span kinds.
func (k SpanKind) Valid() bool { return k >= KindInternal && k <= KindConsumer }

// StatusCode is the span status.
type StatusCode int

const (
	StatusUnset StatusCode = iota
	StatusOK
	StatusError
)

// Valid reports whether the status code is known.
func (s StatusCode) Valid() bool { return s >= StatusUnset && s <= StatusError }

// Span is a single unit of work in a trace. Field omission degrades to
// defaults rather than failing ingestion unless the identifiers are invalid.
type Span struct {
	TraceID       TraceID
	SpanID        SpanID
	ParentSpanID  SpanID // empty means a root span; otherwise must resolve later.
	Service       string
	Name          string
	Kind          SpanKind
	StartUnixNano int64
	EndUnixNano   int64
	Attributes    map[string]string
	Status        StatusCode
	StatusMessage string
}

// Report is one inbound report from a local data source: the optional trace
// context header plus the span payload carried in the body.
type Report struct {
	// ContextHeader is the W3C-style "traceparent" header. Empty means the
	// report carries no context header at all.
	ContextHeader string
	// HeaderVersion, when non-empty, is the version tag announced by the data
	// source alongside the header (used for future-version detection).
	HeaderVersion string
	Span          Span
}

// Identity validation errors. Callers compare with errors.Is to decide the
// degradation path.
var (
	ErrBadTraceID = errors.New("invalid trace id")
	ErrBadSpanID  = errors.New("invalid span id")
)

// ParseTraceID validates a 32-char lowercase, non-all-zero hex trace id.
func ParseTraceID(s string) (TraceID, error) {
	if len(s) != 32 {
		return "", ErrBadTraceID
	}
	if _, err := hex.DecodeString(s); err != nil {
		return "", ErrBadTraceID
	}
	if isAllZero(s) {
		return "", ErrBadTraceID
	}
	return TraceID(s), nil
}

// ParseSpanID validates a 16-char lowercase, non-all-zero hex span id.
func ParseSpanID(s string) (SpanID, error) {
	if len(s) != 16 {
		return "", ErrBadSpanID
	}
	if _, err := hex.DecodeString(s); err != nil {
		return "", ErrBadSpanID
	}
	if isAllZero(s) {
		return "", ErrBadSpanID
	}
	return SpanID(s), nil
}

// ValidateSpan applies the body-level validity rules:
//   - trace id and span id must be well formed;
//   - parent span id, when present, must be well formed;
//   - timestamps must be non-negative with end >= start.
//
// Missing optional fields (service, name, kind, status) degrade to defaults
// and are never an error.
func ValidateSpan(s Span) error {
	if _, err := ParseTraceID(string(s.TraceID)); err != nil {
		return err
	}
	if _, err := ParseSpanID(string(s.SpanID)); err != nil {
		return err
	}
	if s.ParentSpanID != "" {
		if _, err := ParseSpanID(string(s.ParentSpanID)); err != nil {
			return err
		}
	}
	if s.StartUnixNano < 0 || s.EndUnixNano < 0 || s.EndUnixNano < s.StartUnixNano {
		return errors.New("invalid span timestamps")
	}
	return nil
}

// Normalize fills degraded defaults and copies the attribute map so callers
// can mutate the report body afterwards without affecting stored state.
func (s Span) Normalize() Span {
	if !s.Kind.Valid() {
		s.Kind = KindInternal
	}
	if !s.Status.Valid() {
		s.Status = StatusUnset
	}
	if s.Attributes != nil {
		attrs := make(map[string]string, len(s.Attributes))
		for k, v := range s.Attributes {
			attrs[k] = v
		}
		s.Attributes = attrs
	} else {
		s.Attributes = map[string]string{}
	}
	return s
}

// Latency returns the span latency in nanoseconds.
func (s Span) Latency() int64 { return s.EndUnixNano - s.StartUnixNano }

func isAllZero(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}
