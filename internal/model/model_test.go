package model

import "testing"

func TestParseIDs(t *testing.T) {
	if _, err := ParseTraceID("00000000000000000000000000000000"); err != ErrBadTraceID {
		t.Fatalf("all-zero trace id must be invalid, got %v", err)
	}
	if _, err := ParseSpanID("0000000000000000"); err != ErrBadSpanID {
		t.Fatalf("all-zero span id must be invalid, got %v", err)
	}
	if _, err := ParseTraceID("abc"); err == nil {
		t.Fatal("short trace id must be invalid")
	}
	if _, err := ParseSpanID("zz00000000000001"); err == nil {
		t.Fatal("non-hex span id must be invalid")
	}
	if _, err := ParseTraceID("0102030405060708090a0b0c0d0e0f10"); err != nil {
		t.Fatalf("valid trace id rejected: %v", err)
	}
}

func TestValidateSpanTimestamps(t *testing.T) {
	base := Span{
		TraceID: "0102030405060708090a0b0c0d0e0f10",
		SpanID:  "0102030405060708",
	}
	if err := ValidateSpan(base); err != nil {
		t.Fatalf("zero timestamps should be valid: %v", err)
	}
	bad := base
	bad.StartUnixNano, bad.EndUnixNano = 100, 50
	if err := ValidateSpan(bad); err == nil {
		t.Fatal("end < start must be invalid")
	}
}

func TestNormalizeDefaultsAndCopy(t *testing.T) {
	s := Span{Attributes: map[string]string{"a": "b"}, Kind: SpanKind(99), Status: StatusCode(7)}
	n := s.Normalize()
	if n.Kind != KindInternal || n.Status != StatusUnset {
		t.Fatal("unknown kind/status must degrade to defaults")
	}
	n.Attributes["a"] = "changed"
	if s.Attributes["a"] != "b" {
		t.Fatal("Normalize must copy attributes, not alias")
	}
}
