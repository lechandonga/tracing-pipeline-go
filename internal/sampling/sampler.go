// Package sampling computes trace-level sampling verdicts.
//
// The decision for a trace depends ONLY on:
//
//  1. ordered attribute rules (first match wins), evaluated against the first
//     span that arrives for that trace; and
//  2. a deterministic hash of the trace id compared against Rate, when no
//     rule matched.
//
// The caller's sampled hint (W3C flags bit) is advisory: it is recorded in
// the reason but can never force or overturn a decision. Because the verdict
// is a pure function of the trace id and the first span, every service in the
// same trace reaches the same verdict regardless of arrival order, and
// replacing the configuration mid-run never changes a decision already made
// (the pipeline pins the decision on the trace state).
package sampling

import (
	"fmt"
	"hash/fnv"

	"github.com/lechandonga/tracing-pipeline-go/internal/model"
)

// Decision is a trace-level sampling verdict.
type Decision int

const (
	Drop Decision = iota
	Keep
)

// String renders the verdict for logs and snapshots.
func (d Decision) String() string {
	switch d {
	case Keep:
		return "keep"
	default:
		return "drop"
	}
}

// Rule is an ordered attribute-based sampling rule. A rule matches a span
// when its service selector matches AND the span carries the attribute.
//
//   - Service == "" matches every service;
//   - Value  == "" matches on key presence alone;
//   - otherwise the attribute must equal Value exactly.
type Rule struct {
	Service string
	Key     string
	Value   string
	Decision
}

// Config configures the sampler. Rate is the keep fraction in [0,1]:
// <= 0 drops everything, >= 1 keeps everything (unless a preceding Drop rule
// matches). Rules are evaluated before Rate.
type Config struct {
	Rate  float64
	Rules []Rule
}

// Sampler makes sampling decisions from an immutable-at-use config.
type Sampler struct {
	cfg Config
}

// New constructs a Sampler.
func New(cfg Config) *Sampler { return &Sampler{cfg: cfg} }

// Decide computes the verdict for the first span of a trace. hintPresent
// reports whether a context header actually carried a sampled flag; hint is
// that flag's value. The hint is advisory: it is recorded in the reason when
// it disagrees but can never force or overturn a decision. The returned
// reason is the machine-readable determination basis.
func (s *Sampler) Decide(traceID model.TraceID, span model.Span, hintPresent, hint bool) (Decision, string) {
	for i, r := range s.cfg.Rules {
		if ruleMatches(r, span) {
			reason := fmt.Sprintf("attribute-rule#%d:service=%q,%s=%q->%s",
				i, r.Service, r.Key, r.Value, r.Decision)
			if hintPresent && hint && r.Decision == Drop {
				reason += ";hint=keep-conflict-ignored"
			}
			return r.Decision, reason
		}
	}

	d := s.hashDecide(traceID)
	reason := fmt.Sprintf("rate:%d-bucket<%.4f->%s", traceBucket(traceID), s.rate(), d)
	if hintPresent && hint != (d == Keep) {
		word := "drop"
		if hint {
			word = "keep"
		}
		reason += ";hint=" + word + "-conflict-ignored"
	}
	return d, reason
}

func (s *Sampler) rate() float64 {
	switch {
	case s.cfg.Rate <= 0:
		return 0
	case s.cfg.Rate >= 1:
		return 1
	default:
		return s.cfg.Rate
	}
}

// hashDecide maps a trace id uniformly onto [0,1) with FNV-1a so the verdict
// is stable across processes and arrival orders.
func (s *Sampler) hashDecide(traceID model.TraceID) Decision {
	r := s.rate()
	if r <= 0 {
		return Drop
	}
	if r >= 1 {
		return Keep
	}
	frac := float64(traceBucket(traceID)) / (1 << 32)
	if frac < r {
		return Keep
	}
	return Drop
}

func traceBucket(traceID model.TraceID) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(string(traceID)))
	return h.Sum32()
}

func ruleMatches(r Rule, sp model.Span) bool {
	if r.Service != "" && r.Service != sp.Service {
		return false
	}
	v, present := sp.Attributes[r.Key]
	if !present {
		return false
	}
	return r.Value == "" || v == r.Value
}
