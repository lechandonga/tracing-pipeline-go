package tracecontext

import (
	"errors"
	"testing"

	"github.com/lechandonga/tracing-pipeline-go/internal/model"
)

const tid = "0102030405060708090a0b0c0d0e0f10"

func body(parent model.SpanID) model.Span {
	return model.Span{
		TraceID: model.TraceID(tid), SpanID: model.SpanID("1111111111111111"),
		ParentSpanID: parent,
	}
}

func TestNoHeader(t *testing.T) {
	p, err := Parse("", body(""))
	if err != nil || p.Degraded || p.Reason != ReasonNoHeader {
		t.Fatalf("no header: %+v err=%v", p, err)
	}
}

func TestValidV0(t *testing.T) {
	h := "00-" + tid + "-2222222222222222-01"
	p, err := Parse(h, body(model.SpanID("2222222222222222")))
	if err != nil || p.Degraded || !p.Sampled || !p.HintPresent {
		t.Fatalf("valid v0 header: %+v err=%v", p, err)
	}
}

func TestTraceIDConflictRejects(t *testing.T) {
	h := "00-ffffffffffffffffffffffffffffffff-2222222222222222-01"
	_, err := Parse(h, body(""))
	if !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("trace id mismatch must be a hard conflict, got %v", err)
	}
}

func TestMalformedAndUnsupportedVersion(t *testing.T) {
	for _, h := range []string{
		"garbage",
		"00-xyz-2222222222222222-01",
		"00-" + tid + "-2222222222222222", // too few fields for v0
	} {
		p, err := Parse(h, body(""))
		if err != nil || !p.Degraded || p.Reason != ReasonHeaderMalformed {
			t.Fatalf("header %q: %+v err=%v", h, p, err)
		}
	}
	p, err := Parse("ff-"+tid+"-2222222222222222-01", body(""))
	if err != nil || !p.Degraded || p.Reason != ReasonVersionUnsupported {
		t.Fatalf("ff must be unsupported: %+v err=%v", p, err)
	}
}

func TestMinorVersionDowngradeIgnoresExtraFields(t *testing.T) {
	h := "01-" + tid + "-2222222222222222-01-unknownvendorstate"
	p, err := Parse(h, body(model.SpanID("2222222222222222")))
	if err != nil || !p.Degraded || p.Reason != ReasonMinorVersionDowngrade {
		t.Fatalf("v01 extra fields must downgrade: %+v err=%v", p, err)
	}
}

func TestParentMismatchKeepsBody(t *testing.T) {
	h := "00-" + tid + "-2222222222222222-00"
	p, err := Parse(h, body(model.SpanID("3333333333333333")))
	if err != nil || !p.Degraded || p.ParentSpanID != "3333333333333333" || p.Reason != ReasonParentMismatch {
		t.Fatalf("body parent must win: %+v err=%v", p, err)
	}
}

func TestParentInferredFromHeader(t *testing.T) {
	h := "00-" + tid + "-2222222222222222-00"
	p, err := Parse(h, body(""))
	if err != nil || !p.ParentInferred || p.ParentSpanID != "2222222222222222" || p.Reason != ReasonParentInferred {
		t.Fatalf("header parent should be inferred: %+v err=%v", p, err)
	}
}
