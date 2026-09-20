package wal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAppendReplay(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	records := []struct {
		t byte
		p []byte
	}{
		{TypeSpan, []byte(`{"a":1}`)},
		{TypeDecision, []byte("keep")},
		{TypeEvict, []byte("ttl")},
	}
	for _, r := range records {
		if err := l.Append(r.t, r.p); err != nil {
			t.Fatal(err)
		}
	}
	var got []struct {
		t byte
		p []byte
	}
	if err := l.Replay(func(t byte, p []byte) error {
		cp := append([]byte(nil), p...)
		got = append(got, struct {
			t byte
			p []byte
		}{t, cp})
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 records, got %d", len(got))
	}
	for i, r := range records {
		if got[i].t != r.t || string(got[i].p) != string(r.p) {
			t.Fatalf("record %d mismatch: %+v", i, got[i])
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTornTailIsIgnored(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(TypeSpan, []byte("good")); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash mid-write: append garbage partial bytes.
	f, err := os.OpenFile(filepath.Join(dir, logName), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("TWA")); err != nil {
		t.Fatal(err)
	}
	f.Close()

	l2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l2.Close()
	n := 0
	if err := l2.Replay(func(t byte, p []byte) error { n++; return nil }); err != nil {
		t.Fatalf("torn tail must be tolerated, got %v", err)
	}
	if n != 1 {
		t.Fatalf("only committed record should replay, got %d", n)
	}
}

func TestCRCMismatchIsFatal(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Append(TypeSpan, []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// Flip a byte inside the payload region (after the 10-byte header).
	p := filepath.Join(dir, logName)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[10] ^= 0xff
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	l2, _ := Open(dir)
	defer l2.Close()
	err = l2.Replay(func(t byte, p []byte) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("committed-frame CRC mismatch must be ErrCorrupt, got %v", err)
	}
}

func TestSnapshotCompaction(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		if err := l.Append(TypeSpan, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.SaveSnapshot([]byte(`{"version":1}`)); err != nil {
		t.Fatal(err)
	}
	data, err := l.LoadSnapshot()
	if err != nil || string(data) != `{"version":1}` {
		t.Fatalf("snapshot load: %q %v", data, err)
	}
	n := 0
	if err := l.Replay(func(t byte, p []byte) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatalf("log must be empty after compaction, got %d records", n)
	}
	// New appends still work after rotation.
	if err := l.Append(TypeEvict, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := l.Replay(func(t byte, p []byte) error { n++; return nil }); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("expected 1 post-compaction record, got %d", n)
	}
}
