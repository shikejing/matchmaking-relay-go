package audit

import "testing"

func TestRecorderRings(t *testing.T) {
	r := NewRecorder(3)
	s1 := r.Record(Event{Kind: EventTeamSubmitted, Team: "t1"})
	s2 := r.Record(Event{Kind: EventMatched, Team: "t1", Session: "x"})
	if s2 != s1+1 {
		t.Fatal("sequence must be monotonic")
	}
	for i := 0; i < 5; i++ {
		r.Record(Event{Kind: EventTeamRejected})
	}
	g := r.GlobalEvents(10)
	if len(g) != 3 {
		t.Fatalf("global ring cap = %d, want 3", len(g))
	}
	h := r.TeamHistory("t1")
	if len(h) != 2 || h[0].Kind != EventTeamSubmitted || h[1].Kind != EventMatched {
		t.Fatalf("team history wrong: %+v", h)
	}
	if sh := r.SessionHistory("x"); len(sh) != 1 || sh[0].Kind != EventMatched {
		t.Fatalf("session history wrong: %+v", sh)
	}
	if h := r.TeamHistory("missing"); h != nil {
		t.Fatal("unknown object must return nil")
	}
}
