package simnet

import (
	"testing"
	"time"
)

func planSequence(seed int64, count int) []fate {
	n := New(Config{
		Seed:          seed,
		DropRate:      0.2,
		MinDelay:      time.Millisecond,
		MaxDelay:      20 * time.Millisecond,
		ReorderRate:   0.1,
		ReorderDelay:  100 * time.Millisecond,
		DuplicateRate: 0.05,
	})
	defer n.Close()

	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]fate, count)
	for i := range out {
		out[i] = n.plan()
	}
	return out
}

func TestSameSeedSameFaults(t *testing.T) {
	a, b := planSequence(42, 1000), planSequence(42, 1000)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("decision %d differs for the same seed: %+v vs %+v", i, a[i], b[i])
		}
	}
}

func TestDifferentSeedDifferentFaults(t *testing.T) {
	a, b := planSequence(1, 1000), planSequence(2, 1000)
	for i := range a {
		if a[i] != b[i] {
			return
		}
	}
	t.Fatal("two different seeds produced identical fault sequences")
}

func TestPlanRespectsDelayBounds(t *testing.T) {
	n := New(Config{Seed: 7, MinDelay: 5 * time.Millisecond, MaxDelay: 15 * time.Millisecond})
	defer n.Close()

	n.mu.Lock()
	defer n.mu.Unlock()
	for i := 0; i < 1000; i++ {
		f := n.plan()
		if f.delay < 5*time.Millisecond || f.delay > 15*time.Millisecond {
			t.Fatalf("delay %v outside [5ms, 15ms]", f.delay)
		}
		if f.drop || f.dup {
			t.Fatalf("fault injected with zero rates: %+v", f)
		}
	}
}
