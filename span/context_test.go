package span

import "testing"

func TestParseTraceparentV0(t *testing.T) {
	ctx, rej := ParseContext(map[string]string{
		"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
	})
	if rej != nil {
		t.Fatalf("unexpected rejection: %+v", rej)
	}
	if ctx.TraceID != "0af7651916cd43dd8448eb211c80319c" || !ctx.Sampled || ctx.Degraded {
		t.Fatalf("bad ctx: %+v", ctx)
	}
}

func TestParseUnknownVersionDegrades(t *testing.T) {
	ctx, rej := ParseContext(map[string]string{
		"traceparent": "01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01-extrafield",
	})
	if rej != nil {
		t.Fatalf("unknown version should degrade, not reject: %+v", rej)
	}
	if !ctx.Degraded || ctx.TraceID == "" {
		t.Fatalf("expected degraded parse: %+v", ctx)
	}
}

func TestParseRejectsMalformed(t *testing.T) {
	cases := []map[string]string{
		{"traceparent": "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
		{"traceparent": "00-00000000000000000000000000000000-b7ad6b7169203331-01"},
		{"traceparent": "00-zzzz651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
		{"traceparent": "00-tooshort"},
		{},
	}
	for i, raw := range cases {
		if _, rej := ParseContext(raw); rej == nil {
			t.Fatalf("case %d: expected rejection", i)
		} else if rej.Reason != ReasonMalformedContext {
			t.Fatalf("case %d: wrong reason %s", i, rej.Reason)
		}
	}
}

func TestParseFlatFallback(t *testing.T) {
	ctx, rej := ParseContext(map[string]string{
		"trace_id": "abc123", "span_id": "def456", "parent_id": "p1", "sampled": "true",
	})
	if rej != nil || !ctx.Sampled || ctx.ParentID != "p1" {
		t.Fatalf("flat parse failed: %+v rej=%+v", ctx, rej)
	}
}
