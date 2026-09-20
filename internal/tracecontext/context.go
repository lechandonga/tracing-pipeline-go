// Package tracecontext parses trace context headers (W3C traceparent style)
// and resolves them against the span body under explicit, testable rules:
//
//   - no header: the span body is authoritative, no degradation;
//   - malformed header or unsupported version: the header is rejected and the
//     body remains authoritative, flagged Degraded with a recorded reason;
//   - trace-id mismatch between header and body: a hard error, the report is
//     rejected so no span is ever attached to the wrong trace;
//   - parent-id mismatch: the body parent is authoritative; when the body has
//     no parent but the header names a different, valid span id, that id is
//     adopted as an inferred parent and the result is flagged degraded;
//   - version "01" is treated as a newer minor version: its four standard
//     fields are parsed and unknown trailing fields are ignored (downgrade);
//   - any other version (including reserved "ff") is unsupported: identity
//     falls back to the body and the decision is recorded.
package tracecontext

import (
	"encoding/hex"
	"errors"
	"strings"

	"github.com/lechandonga/tracing-pipeline-go/internal/model"
)

// Reason codes recorded on degraded resolutions and hard failures.
const (
	ReasonNoHeader              = "no-context-header"
	ReasonHeaderMalformed       = "context-header-malformed"
	ReasonVersionUnsupported    = "context-version-unsupported"
	ReasonMinorVersionDowngrade = "context-minor-version-downgrade"
	ReasonParentMismatch        = "context-parent-mismatch-body-kept"
	ReasonParentInferred        = "context-parent-inferred-from-header"
)

// Hard errors: callers must reject the report rather than guess an identity.
var (
	ErrIdentityConflict = errors.New("trace context conflicts with span body")
)

// Parsed is the result of resolving a report's context against its body.
type Parsed struct {
	TraceID model.TraceID
	SpanID  model.SpanID
	// ParentSpanID is the effective parent: body parent when present,
	// otherwise the inferred header parent (see ParentInferred).
	ParentSpanID model.SpanID
	// ParentInferred reports whether the parent came from the context
	// header rather than the span body.
	ParentInferred bool
	// HintPresent reports whether a valid flags byte was seen, so callers
	// can distinguish "no hint" from "hint says drop".
	HintPresent bool
	// Sampled is the caller's sampling hint (flags bit 0). It is advisory
	// only and can never change a trace's sticky decision.
	Sampled bool
	// Degraded is true when any non-fatal rule fired.
	Degraded bool
	// Reason is the machine-readable determination basis.
	Reason string
}

// Parse resolves identity for one report. body is assumed to already have
// passed model.ValidateSpan. A non-nil error denotes an unrecoverable
// conflict; otherwise Parsed always carries a safe identity and a reason.
func Parse(header string, body model.Span) (Parsed, error) {
	p := Parsed{TraceID: body.TraceID, SpanID: body.SpanID, ParentSpanID: body.ParentSpanID}

	if header == "" {
		p.Reason = ReasonNoHeader
		return p, nil
	}

	version, fields, ok := splitHeader(header)
	if !ok {
		// Malformed framing: never trust partial identity.
		p.Degraded = true
		p.Reason = ReasonHeaderMalformed
		return p, nil
	}

	switch version {
	case "00":
		// Version 00 allows exactly the four documented fields. Extra
		// delimited data makes it malformed by spec.
		if len(fields) != 3 {
			p.Degraded = true
			p.Reason = ReasonHeaderMalformed
			return p, nil
		}
	case "01":
		// Newer minor version: tolerate and ignore unknown trailing fields.
		if len(fields) < 3 {
			p.Degraded = true
			p.Reason = ReasonHeaderMalformed
			return p, nil
		}
		fields = fields[:3]
		p.Degraded = true
		p.Reason = ReasonMinorVersionDowngrade
	default:
		// Unknown version (including reserved ff): do not correlate on it.
		p.Degraded = true
		p.Reason = ReasonVersionUnsupported
		return p, nil
	}

	tidRaw, sidRaw, flagsRaw := fields[0], fields[1], fields[2]
	hdrTrace, err1 := model.ParseTraceID(tidRaw)
	hdrSpan, err2 := model.ParseSpanID(sidRaw)
	flags, err3 := hex.DecodeString(flagsRaw)
	if err1 != nil || err2 != nil || err3 != nil || len(flags) != 1 {
		p.Degraded = true
		p.Reason = ReasonHeaderMalformed
		return p, nil
	}

	// A trace-id disagreement is an unrecoverable identity conflict:
	// attaching this span anywhere would be a silent wrong correlation.
	if hdrTrace != body.TraceID {
		return Parsed{}, ErrIdentityConflict
	}

	p.HintPresent = true
	p.Sampled = flags[0]&0x01 == 0x01

	switch {
	case body.ParentSpanID != "" && body.ParentSpanID != hdrSpan:
		// Both sources name a parent and disagree: the span body is the
		// authoritative payload; record the disagreement, keep body parent.
		p.Degraded = true
		p.Reason = ReasonParentMismatch
	case body.ParentSpanID == "" && hdrSpan != body.SpanID:
		// Root-claiming span but the header names an upstream span: adopt it
		// as an inferred, explicitly flagged link rather than guessing.
		p.ParentSpanID = hdrSpan
		p.ParentInferred = true
		p.Degraded = true
		p.Reason = ReasonParentInferred
	}
	return p, nil
}

// splitHeader validates the overall shape and returns the 2-hex-char version
// plus the remaining dash-separated fields (without copying semantics beyond
// what strings provides).
func splitHeader(h string) (version string, fields []string, ok bool) {
	parts := strings.Split(h, "-")
	if len(parts) < 4 {
		return "", nil, false
	}
	version = parts[0]
	if len(version) != 2 || isHex(version) != nil {
		return "", nil, false
	}
	for _, f := range parts[1:] {
		if f == "" {
			return "", nil, false
		}
	}
	return version, parts[1:], true
}

func isHex(s string) error {
	if len(s)%2 != 0 {
		return errors.New("odd hex length")
	}
	_, err := hex.DecodeString(s)
	return err
}
