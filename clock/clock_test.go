package clock

import "testing"

func TestFakeClock(t *testing.T) {
	f := NewFakeClock(10)
	if f.NowMs() != 10 {
		t.Fatalf("start = %d", f.NowMs())
	}
	f.Advance(5)
	if f.NowMs() != 15 {
		t.Fatalf("after advance = %d", f.NowMs())
	}
	f.Set(100)
	if f.NowMs() != 100 {
		t.Fatalf("after set = %d", f.NowMs())
	}
}

func TestRealClock(t *testing.T) {
	a := NewRealClock().NowMs()
	if a <= 0 {
		t.Fatalf("real clock non-positive: %d", a)
	}
}
