package storage_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/silviacioroiu/raftkv/storage"
)

type store interface {
	Append(record []byte) error
	Compact(snapshot []byte, records [][]byte) error
	Load() (snapshot []byte, records [][]byte, err error)
	Close() error
}

func expect(t *testing.T, s store, wantSnapshot string, wantRecords ...string) {
	t.Helper()
	snapshot, records, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if string(snapshot) != wantSnapshot {
		t.Fatalf("snapshot %q, want %q", snapshot, wantSnapshot)
	}
	if len(records) != len(wantRecords) {
		t.Fatalf("%d records %q, want %d %q", len(records), records, len(wantRecords), wantRecords)
	}
	for i, want := range wantRecords {
		if string(records[i]) != want {
			t.Fatalf("record %d is %q, want %q", i, records[i], want)
		}
	}
}

func exercise(t *testing.T, s store) {
	t.Helper()
	expect(t, s, "")

	for _, r := range []string{"a", "bb", ""} {
		if err := s.Append([]byte(r)); err != nil {
			t.Fatal(err)
		}
	}
	expect(t, s, "", "a", "bb", "")

	if err := s.Compact([]byte("snap"), [][]byte{[]byte("base")}); err != nil {
		t.Fatal(err)
	}
	expect(t, s, "snap", "base")

	// Appends after a compaction must land behind the new log.
	if err := s.Append([]byte("c")); err != nil {
		t.Fatal(err)
	}
	expect(t, s, "snap", "base", "c")
}

func TestMem(t *testing.T) {
	exercise(t, storage.NewMem())
}

func openFile(t *testing.T, dir string) *storage.File {
	t.Helper()
	f, err := storage.OpenFile(dir, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

func TestFile(t *testing.T) {
	exercise(t, openFile(t, t.TempDir()))
}

func TestFileSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	f := openFile(t, dir)
	if err := f.Compact([]byte("snap"), [][]byte{[]byte("base")}); err != nil {
		t.Fatal(err)
	}
	if err := f.Append([]byte("later")); err != nil {
		t.Fatal(err)
	}
	f.Close()

	expect(t, openFile(t, dir), "snap", "base", "later")
}

// A crash in the middle of an append leaves a partial record at the end of
// the WAL. It must be dropped, and later appends must not land behind it.
func TestFileDropsTornTail(t *testing.T) {
	dir := t.TempDir()
	f := openFile(t, dir)
	for _, r := range []string{"one", "two"} {
		if err := f.Append([]byte(r)); err != nil {
			t.Fatal(err)
		}
	}
	f.Close()

	path := filepath.Join(dir, "wal")
	for _, torn := range [][]byte{
		{9, 0, 0},                               // part of a header
		{9, 0, 0, 0, 1, 2, 3, 4, 'x', 'y'},      // header, then too little payload
		{3, 0, 0, 0, 0, 0, 0, 0, 'b', 'a', 'd'}, // complete, but wrong checksum
	} {
		intact, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, append(intact, torn...), 0o644); err != nil {
			t.Fatal(err)
		}

		g := openFile(t, dir)
		expect(t, g, "", "one", "two")
		g.Close()

		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if len(after) != len(intact) {
			t.Fatalf("torn tail was not cut off: wal is %d bytes, want %d", len(after), len(intact))
		}
	}

	g := openFile(t, dir)
	if _, _, err := g.Load(); err != nil {
		t.Fatal(err)
	}
	if err := g.Append([]byte("three")); err != nil {
		t.Fatal(err)
	}
	expect(t, g, "", "one", "two", "three")
}

func TestFileDetectsCorruptSnapshot(t *testing.T) {
	dir := t.TempDir()
	f := openFile(t, dir)
	if err := f.Compact([]byte("important"), nil); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "snapshot")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-1] ^= 0xFF
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, _, err := f.Load(); err == nil {
		t.Fatal("a flipped byte in the snapshot went undetected")
	}
}
