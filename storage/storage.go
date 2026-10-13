// Package storage provides the durable stores a Raft node persists to: an
// in-memory one for tests and a crash-safe one backed by files.
//
// Both hold two things: a snapshot, and a write-ahead log (WAL) of opaque
// records describing every change made since that snapshot. The node appends
// a record for each change and rebuilds its state after a restart by
// replaying them in order.
package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

// Mem keeps everything in memory. In tests it stands in for a disk: the
// harness keeps the Mem alive across a simulated crash of its node.
type Mem struct {
	mu       sync.Mutex
	snapshot []byte
	records  [][]byte
}

func NewMem() *Mem { return &Mem{} }

func (m *Mem) Append(record []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, bytes.Clone(record))
	return nil
}

func (m *Mem) Compact(snapshot []byte, records [][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snapshot = bytes.Clone(snapshot)
	m.records = nil
	for _, r := range records {
		m.records = append(m.records, bytes.Clone(r))
	}
	return nil
}

func (m *Mem) Load() (snapshot []byte, records [][]byte, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.records {
		records = append(records, bytes.Clone(r))
	}
	return bytes.Clone(m.snapshot), records, nil
}

func (m *Mem) Close() error { return nil }

// Sizes reports the bytes currently held, so tests can check that compaction
// actually bounds the log.
func (m *Mem) Sizes() (log, snapshot int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, r := range m.records {
		log += len(r)
	}
	return log, len(m.snapshot)
}

const (
	walFile      = "wal"
	snapshotFile = "snapshot"
	frameHeader  = 8 // 4-byte length + 4-byte CRC-32 of the payload
)

// File is a store backed by two files in one directory.
//
// The WAL is only ever appended to: one write and one fsync per record, on a
// file that stays open. A record is framed with its length and a CRC, so a
// record cut short by a crash is detected on the next start and dropped; it
// was never acknowledged, because Append had not returned.
//
// The snapshot is replaced as a whole by writing a temporary file and
// renaming it over the old one, which is atomic.
type File struct {
	dir   string
	fsync bool

	mu  sync.Mutex
	wal *os.File
}

// OpenFile uses dir, creating it if needed. With fsync false, writes are not
// forced to disk: faster, but acknowledged state can be lost on power
// failure, which breaks Raft's safety guarantees. Use false only to measure
// what fsync costs.
func OpenFile(dir string, fsync bool) (*File, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: %w", err)
	}
	f := &File{dir: dir, fsync: fsync}
	if err := f.openWAL(); err != nil {
		return nil, err
	}
	return f, nil
}

// openWAL opens the log positioned at its end. It does not use O_APPEND:
// on Windows a file opened that way cannot be truncated, which Load needs
// to do. All writes go through f.mu, so the file offset is always the end.
func (f *File) openWAL() error {
	wal, err := os.OpenFile(filepath.Join(f.dir, walFile), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	if _, err := wal.Seek(0, io.SeekEnd); err != nil {
		wal.Close()
		return fmt.Errorf("storage: %w", err)
	}
	f.wal = wal
	return nil
}

func frame(record []byte) []byte {
	buf := make([]byte, frameHeader+len(record))
	binary.LittleEndian.PutUint32(buf[0:4], uint32(len(record)))
	binary.LittleEndian.PutUint32(buf[4:8], crc32.ChecksumIEEE(record))
	copy(buf[frameHeader:], record)
	return buf
}

// Append adds one record to the WAL. It returns only once the record is on
// stable storage, which is what lets the caller acknowledge it.
func (f *File) Append(record []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.wal == nil {
		return errors.New("storage: closed")
	}
	// A single write call, so that a crash cannot leave a header without
	// its payload from an otherwise completed write.
	if _, err := f.wal.Write(frame(record)); err != nil {
		return fmt.Errorf("storage: append: %w", err)
	}
	if f.fsync {
		if err := f.wal.Sync(); err != nil {
			return fmt.Errorf("storage: sync: %w", err)
		}
	}
	return nil
}

// Compact replaces the snapshot and the entire WAL.
//
// The snapshot is written first. If a crash separates the two steps, the
// node restarts with a snapshot newer than its WAL, which Raft reconciles by
// discarding the log prefix the snapshot covers. The opposite order could
// leave a WAL that starts after a snapshot which was never written.
func (f *File) Compact(snapshot []byte, records [][]byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.wal == nil {
		return errors.New("storage: closed")
	}
	if err := f.writeAtomic(snapshotFile, frame(snapshot)); err != nil {
		return err
	}

	var wal bytes.Buffer
	for _, r := range records {
		wal.Write(frame(r))
	}
	// Windows cannot rename over a file that is open, so close ours first.
	f.wal.Close()
	f.wal = nil
	if err := f.writeAtomic(walFile, wal.Bytes()); err != nil {
		f.openWAL() // keep the store usable; the old WAL is still valid
		return err
	}
	return f.openWAL()
}

// Load returns the snapshot and every intact WAL record, oldest first. Both
// are empty for a new store.
func (f *File) Load() (snapshot []byte, records [][]byte, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.wal == nil {
		return nil, nil, errors.New("storage: closed")
	}

	raw, err := os.ReadFile(filepath.Join(f.dir, snapshotFile))
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return nil, nil, fmt.Errorf("storage: %w", err)
	default:
		// The snapshot is replaced atomically, so unlike the WAL it can
		// never be legitimately incomplete: damage here is real corruption.
		recs, good := parse(raw)
		if len(recs) != 1 || good != len(raw) {
			return nil, nil, errors.New("storage: snapshot is corrupt")
		}
		snapshot = recs[0]
	}

	if _, err := f.wal.Seek(0, io.SeekStart); err != nil {
		return nil, nil, fmt.Errorf("storage: %w", err)
	}
	raw, err = io.ReadAll(f.wal)
	if err != nil {
		return nil, nil, fmt.Errorf("storage: %w", err)
	}
	records, good := parse(raw)
	if good < len(raw) {
		// A torn final record from a crash mid-append. Cut it off so that
		// new records are not appended behind garbage.
		if err := f.wal.Truncate(int64(good)); err != nil {
			return nil, nil, fmt.Errorf("storage: %w", err)
		}
		if _, err := f.wal.Seek(int64(good), io.SeekStart); err != nil {
			return nil, nil, fmt.Errorf("storage: %w", err)
		}
	}
	return snapshot, records, nil
}

// parse splits framed data into records, stopping at the first frame that is
// incomplete or fails its checksum. good is the length of the valid prefix.
func parse(raw []byte) (records [][]byte, good int) {
	for {
		rest := raw[good:]
		if len(rest) < frameHeader {
			return records, good
		}
		size := int(binary.LittleEndian.Uint32(rest[0:4]))
		sum := binary.LittleEndian.Uint32(rest[4:8])
		if size > len(rest)-frameHeader {
			return records, good
		}
		payload := rest[frameHeader : frameHeader+size]
		if crc32.ChecksumIEEE(payload) != sum {
			return records, good
		}
		records = append(records, payload)
		good += frameHeader + size
	}
}

func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.wal == nil {
		return nil
	}
	err := f.wal.Close()
	f.wal = nil
	return err
}

// writeAtomic replaces a file so that a crash leaves either the old content
// or the new, never a mix.
func (f *File) writeAtomic(name string, data []byte) error {
	path := filepath.Join(f.dir, name)
	tmp := path + ".tmp"

	file, err := os.Create(tmp)
	if err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	_, err = file.Write(data)
	if err == nil && f.fsync {
		err = file.Sync()
	}
	if cerr := file.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return fmt.Errorf("storage: write %s: %w", name, err)
	}
	if err := replace(tmp, path); err != nil {
		return fmt.Errorf("storage: %w", err)
	}
	return nil
}

// replace renames tmp over path, which is atomic on POSIX and on NTFS.
//
// On Windows the rename fails with "access is denied" or a sharing violation
// while any other process has the target open, and virus scanners and
// indexers open freshly written files for a few milliseconds. That is not a
// real failure, so it is retried for a while before giving up.
func replace(tmp, path string) error {
	err := os.Rename(tmp, path)
	if err == nil || runtime.GOOS != "windows" {
		return err
	}
	wait := time.Millisecond
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); {
		time.Sleep(wait)
		if err = os.Rename(tmp, path); err == nil {
			return nil
		}
		if wait < 50*time.Millisecond {
			wait *= 2
		}
	}
	return err
}
