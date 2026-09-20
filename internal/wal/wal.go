// Package wal provides an append-only, checksummed write-ahead log and an
// atomically replaced snapshot, backing crash recovery of confirmed spans and
// aggregate state.
//
// On-disk layout under the data directory:
//
//	events.log    framed append-only record log (fsync per confirmed record)
//	snapshot.json full state snapshot; written as snapshot.json.tmp, fsynced
//	              and rename(2)d over the old snapshot, so it is either the
//	              complete previous version or the complete new one
//
// Frame layout (big endian):
//
//	"TWAL" | version(1) | type(1) | payloadLen(4) | payload | crc32(4)
//
// where crc32 covers version|type|payloadLen|payload. A torn final frame
// (short read at EOF, as can result from a crash mid-write) ends replay
// cleanly; any CRC mismatch on a fully read frame is fatal corruption, never
// silently dropped, because it may sit before later confirmed records.
//
// After a durable snapshot the events log is replaced atomically; replaying
// the old log after such a crash is harmless because every record is applied
// idempotently by the recovery code.
package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	magic       = "TWAL"
	version     = byte(1)
	logName     = "events.log"
	logTmpName  = "events.log.tmp"
	snapName    = "snapshot.json"
	snapTmpName = "snapshot.json.tmp"
)

// Record type tags.
const (
	TypeSpan byte = 1 + iota
	TypeEvict
	TypeDecision
)

// Errors returned by open/replay.
var (
	ErrCorrupt  = errors.New("wal: checksum mismatch in committed frame")
	ErrBadFrame = errors.New("wal: unsupported frame version or magic")
)

// SpanRecord is the durable form of one confirmed span.
type SpanRecord struct {
	TraceID        string          `json:"trace_id"`
	Span           json.RawMessage `json:"span"` // model.Span, encoded by caller layer
	ParentInferred bool            `json:"parent_inferred"`
	ArrivalNano    int64           `json:"arrival_nano"`
}

// EvictRecord marks a trace removed by TTL or capacity eviction.
type EvictRecord struct {
	TraceID string `json:"trace_id"`
	Reason  string `json:"reason"`
}

// DecisionRecord pins the sticky sampling verdict.
type DecisionRecord struct {
	TraceID  string `json:"trace_id"`
	Decision string `json:"decision"` // "keep" | "drop"
	Reason   string `json:"reason"`
}

// Log is the write-ahead log handle.
type Log struct {
	dir string
	f   *os.File
}

// Open opens (creating if needed) the WAL directory file in append mode and
// verifies it can be read fully; torn tails are tolerated lazily at Replay.
func Open(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := filepath.Join(dir, logName)
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return &Log{dir: dir, f: f}, nil
}

// Append writes, fsyncs and appends one framed record. The fsync is the
// confirmation boundary: callers treat the span as confirmed only after a
// nil return.
func (l *Log) Append(recType byte, payload []byte) error {
	frame := make([]byte, 0, 4+1+1+4+len(payload)+4)
	frame = append(frame, magic...)
	frame = append(frame, version, recType)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	frame = append(frame, lenBuf[:]...)
	frame = append(frame, payload...)
	var crcBuf [4]byte
	binary.BigEndian.PutUint32(crcBuf[:], crc32.ChecksumIEEE(frame[4:]))
	frame = append(frame, crcBuf[:]...)

	if _, err := l.f.Write(frame); err != nil {
		return err
	}
	return l.f.Sync()
}

// Replay visits every committed frame in order. It stops cleanly on a torn
// final frame but returns ErrCorrupt for a fully framed record whose CRC does
// not match (records beyond it could otherwise be applied over lost data).
func (l *Log) Replay(visit func(recType byte, payload []byte) error) error {
	p := filepath.Join(l.dir, logName)
	f, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()

	r := io.Reader(f)
	header := make([]byte, 4+1+1+4)
	for {
		n, err := io.ReadFull(r, header)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			if n == 0 {
				return nil
			}
			return nil // torn header on the final frame: ignore
		}
		if err != nil {
			return err
		}
		if string(header[:4]) != magic || header[4] != version {
			return ErrBadFrame
		}
		recType := header[5]
		payloadLen := int(binary.BigEndian.Uint32(header[6:10]))
		payload := make([]byte, payloadLen+4)
		if _, err := io.ReadFull(r, payload); err != nil {
			return nil // torn payload/crc tail: ignore final frame
		}
		body, trailer := payload[:payloadLen], payload[payloadLen:]
		want := binary.BigEndian.Uint32(trailer)
		hashed := make([]byte, 0, 6+payloadLen)
		hashed = append(hashed, header[4:]...)
		hashed = append(hashed, body...)
		if crc32.ChecksumIEEE(hashed) != want {
			return ErrCorrupt
		}
		if err := visit(recType, body); err != nil {
			return err
		}
	}
}

// SaveSnapshot writes the snapshot atomically (temp file, fsync, rename,
// fsync directory) and then replaces the event log with a fresh empty one.
// Caller must ensure no concurrent Append: snapshot compaction runs while the
// pipeline holds its ingestion lock.
func (l *Log) SaveSnapshot(data []byte) error {
	tmp := filepath.Join(l.dir, snapTmpName)
	tf, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := tf.Write(data); err != nil {
		tf.Close()
		return err
	}
	if err := tf.Sync(); err != nil {
		tf.Close()
		return err
	}
	if err := tf.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(l.dir, snapName)); err != nil {
		return err
	}
	if err := syncDir(l.dir); err != nil {
		return err
	}
	// Rotate the event log. A crash before this leaves the old log in place;
	// replay after snapshot load is idempotent.
	if err := l.f.Close(); err != nil {
		return err
	}
	ltmp, err := os.OpenFile(filepath.Join(l.dir, logTmpName), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if err := ltmp.Sync(); err != nil {
		ltmp.Close()
		return err
	}
	if err := ltmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(l.dir, logTmpName), filepath.Join(l.dir, logName)); err != nil {
		return err
	}
	if err := syncDir(l.dir); err != nil {
		return err
	}
	nf, err := os.OpenFile(filepath.Join(l.dir, logName), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	l.f = nf
	return nil
}

// LoadSnapshot returns the current snapshot contents, or nil if none exists.
func (l *Log) LoadSnapshot() ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(l.dir, snapName))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	return b, err
}

// Close fsyncs and closes the event log.
func (l *Log) Close() error {
	if l.f == nil {
		return nil
	}
	err := l.f.Sync()
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	l.f = nil
	return err
}

// JSONRecord helpers: typed encode/decode for callers.
func Encode(v any) ([]byte, error) { return json.Marshal(v) }
func Decode(b []byte, v any) error { return json.Unmarshal(b, v) }

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
