package hrtime

import (
	"testing"
	"time"
)

func TestMonotonicAndFine(t *testing.T) {
	// Count how many distinct readings 2000 back-to-back calls produce. A
	// clock that only moves on a millisecond tick yields a handful.
	distinct := 0
	prev := Now()
	for i := 0; i < 2000; i++ {
		cur := Now()
		if cur < prev {
			t.Fatalf("clock went backwards: %v then %v", prev, cur)
		}
		if cur != prev {
			distinct++
		}
		prev = cur
	}
	if distinct < 100 {
		t.Fatalf("only %d distinct readings in 2000 calls; the clock is too coarse", distinct)
	}
}

func TestTracksWallClock(t *testing.T) {
	begin := Now()
	time.Sleep(50 * time.Millisecond)
	if got := Now() - begin; got < 40*time.Millisecond || got > 500*time.Millisecond {
		t.Fatalf("a 50ms sleep measured as %v", got)
	}
}
