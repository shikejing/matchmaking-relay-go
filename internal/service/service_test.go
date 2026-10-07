package service

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/shikejing/matchmaking-relay-go/internal/audit"
	"github.com/shikejing/matchmaking-relay-go/internal/config"
	"github.com/shikejing/matchmaking-relay-go/internal/match"
	"github.com/shikejing/matchmaking-relay-go/internal/model"
	"github.com/shikejing/matchmaking-relay-go/internal/relay"
	"github.com/shikejing/matchmaking-relay-go/internal/transport"
)

func newTestService(t *testing.T, cfg config.Config) *Service {
	t.Helper()
	svc, err := New(cfg)
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return svc
}

func players(id string, n int, score int) []model.Player {
	out := make([]model.Player, n)
	for i := range out {
		out[i] = model.Player{ID: model.PlayerID(fmt.Sprintf("%s-p%d", id, i)), Score: score}
	}
	return out
}

func enqueueOrFail(t *testing.T, svc *Service, partition, id string, n, score int, rule int) model.TeamID {
	t.Helper()
	team, err := svc.EnqueueTeam(model.PartitionID(partition), model.TeamID(id), players(id, n, score), rule)
	if err != nil {
		t.Fatalf("enqueue %s: %v", id, err)
	}
	return team.ID
}

func TestEndToEndMatchAndRelay(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)

	for i := 0; i < 4; i++ {
		enqueueOrFail(t, svc, "default", fmt.Sprintf("t%d", i), 2, 100+i*10, 0)
	}
	svc.Tick()

	sessions := svc.ListSessions("")
	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sessions))
	}
	for _, s := range sessions {
		if s.State != model.SessionActive || len(s.Members) != 4 {
			t.Fatalf("session %s: state=%d members=%d", s.ID, s.State, len(s.Members))
		}
	}

	// Attach pipes and relay a message.
	s0 := sessions[0]
	pipes := map[model.PlayerID]*transport.Pipe{}
	for _, m := range s0.Members {
		pipe := transport.NewPipe(4)
		svc.AttachPipe(s0.ID, m.PlayerID, pipe)
		pipes[m.PlayerID] = pipe
	}
	if err := svc.SendMessage(s0.ID, s0.Members[0].PlayerID, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	for pid, pipe := range pipes {
		msg, ok := pipe.TryRecv()
		if !ok {
			t.Fatalf("player %s got no message", pid)
		}
		if string(msg.Payload) != "hello" {
			t.Fatalf("player %s got %q", pid, msg.Payload)
		}
	}
}

func TestRuleVersionSwitching(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)

	// Enqueue two teams under v1 explicitly.
	enqueueOrFail(t, svc, "default", "v1-a", 1, 100, 1)
	enqueueOrFail(t, svc, "default", "v1-b", 1, 100, 1)

	// Switch default to v2; already-queued teams must keep v1.
	if err := svc.Rules().SetDefault(2); err != nil {
		t.Fatal(err)
	}
	team, _ := svc.QueryTeam("v1-a")
	if team.RuleVersion != 1 {
		t.Fatalf("queued team rule version = %d, want 1", team.RuleVersion)
	}

	// New enqueues use v2.
	enqueueOrFail(t, svc, "default", "v2-a", 1, 100, 0)
	enqueueOrFail(t, svc, "default", "v2-b", 1, 100, 0)

	svc.Tick()
	sessions := svc.ListSessions("")
	if len(sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(sessions))
	}
	versions := map[int]int{}
	for _, s := range sessions {
		versions[s.RuleVersion]++
	}
	if versions[1] != 1 || versions[2] != 1 {
		t.Fatalf("session rule versions = %v, want one v1 and one v2", versions)
	}

	// Audit must record the rule version of each match decision.
	recs := svc.Audit().Query(audit.Filter{Types: []audit.EventType{audit.EventMatchFormed}})
	if len(recs) != 2 {
		t.Fatalf("match records = %d, want 2", len(recs))
	}
	for _, r := range recs {
		if r.RuleVersion == 0 {
			t.Fatal("match record missing rule version")
		}
		if r.Input == "" || r.Decision == "" {
			t.Fatal("match record missing input/decision basis")
		}
	}
}

func TestReconnectFlow(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)

	enqueueOrFail(t, svc, "default", "t1", 1, 100, 1)
	enqueueOrFail(t, svc, "default", "t2", 1, 100, 1)
	svc.Tick()
	sessions := svc.ListSessions("")
	sid := sessions[0].ID
	p1 := sessions[0].Members[0].PlayerID

	// Disconnect and reconnect within the v1 window (100 ticks).
	if err := svc.Disconnect(sid, p1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 50; i++ {
		svc.Tick()
	}
	if err := svc.Reconnect(sid, p1); err != nil {
		t.Fatalf("reconnect within window: %v", err)
	}
	got, _ := svc.QuerySession(sid)
	if got.ActiveMembers != 2 {
		t.Fatalf("active after reconnect = %d, want 2", got.ActiveMembers)
	}

	// Disconnect again and let the window expire: v1 ends the session.
	if err := svc.Disconnect(sid, p1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 101; i++ {
		svc.Tick()
	}
	got, _ = svc.QuerySession(sid)
	if got.State != model.SessionEnded {
		t.Fatalf("state after retention timeout = %d, want ended", got.State)
	}
	// The end cause must be distinguishable from a backfill timeout.
	if !strings.HasPrefix(got.EndReason, relay.EndCodeRetentionTimeout) {
		t.Fatalf("end reason = %q, want prefix %q", got.EndReason, relay.EndCodeRetentionTimeout)
	}
	if err := svc.Reconnect(sid, p1); err == nil {
		t.Fatal("reconnect after session end should fail")
	}
}

func TestBackfillFlow(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)

	// v2 session with two 1-player teams.
	enqueueOrFail(t, svc, "default", "t1", 1, 100, 2)
	enqueueOrFail(t, svc, "default", "t2", 1, 100, 2)
	svc.Tick()
	sid := svc.ListSessions("")[0].ID
	p1 := svc.ListSessions("")[0].Members[0].PlayerID

	// Disconnect p1 and wait past v2 retention (200 ticks).
	if err := svc.Disconnect(sid, p1); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 201; i++ {
		svc.Tick()
	}
	got, _ := svc.QuerySession(sid)
	if got.State != model.SessionActive {
		t.Fatalf("session should stay active during backfill, state=%d", got.State)
	}
	if got.ActiveMembers != 1 {
		t.Fatalf("active = %d, want 1", got.ActiveMembers)
	}

	// A replacement team (1 player) queues and is backfilled.
	enqueueOrFail(t, svc, "default", "t3", 1, 100, 2)
	svc.Tick()
	got, _ = svc.QuerySession(sid)
	if got.ActiveMembers != 2 {
		t.Fatalf("active after backfill = %d, want 2", got.ActiveMembers)
	}
	found := false
	for _, m := range got.Members {
		if m.PlayerID == "t3-p0" && m.State == model.MemberJoined {
			found = true
		}
	}
	if !found {
		t.Fatal("replacement team t3 not backfilled into session")
	}
}

func TestRejectionReasonsDistinguishable(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 1
	svc := newTestService(t, cfg)

	enqueueOrFail(t, svc, "default", "t1", 1, 100, 0)
	_, err := svc.EnqueueTeam("default", "t2", players("t2", 1, 100), 0)
	var me *match.Error
	if !errors.As(err, &me) || me.Code != match.RejectQueueFull {
		t.Fatalf("expected QUEUE_FULL, got %v", err)
	}

	// Duplicate submission of the same team.
	_, err = svc.EnqueueTeam("default", "t1", players("t1", 1, 100), 0)
	if !errors.As(err, &me) || me.Code != match.RejectDuplicateTeam {
		t.Fatalf("expected DUPLICATE_TEAM, got %v", err)
	}

	// Unknown partition.
	_, err = svc.EnqueueTeam("nope", "t3", players("t3", 1, 100), 0)
	if !errors.As(err, &me) || me.Code != match.RejectPartitionNotFound {
		t.Fatalf("expected PARTITION_NOT_FOUND, got %v", err)
	}
}

func TestAuditTraceability(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)

	enqueueOrFail(t, svc, "default", "t1", 1, 100, 0)
	enqueueOrFail(t, svc, "default", "t2", 1, 140, 0)
	svc.Tick()

	recs := svc.Audit().Query(audit.Filter{Types: []audit.EventType{audit.EventMatchFormed}})
	if len(recs) != 1 {
		t.Fatalf("match records = %d, want 1", len(recs))
	}
	r := recs[0]
	if r.SessionID == "" {
		t.Fatal("match record not linked to session")
	}
	// Input must contain the matched teams and their scores.
	for _, want := range []string{"t1", "t2", "100", "140"} {
		if !contains(r.Input, want) {
			t.Fatalf("match input %q missing %q", r.Input, want)
		}
	}
	// Decision basis must be documented.
	if !contains(r.Decision, "anchor") {
		t.Fatalf("decision basis missing: %q", r.Decision)
	}
}

func TestBackgroundLoopMatches(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	cfg.TickInterval = 20 * time.Millisecond
	svc := newTestService(t, cfg)
	svc.Start()
	defer svc.Stop()

	for i := 0; i < 4; i++ {
		enqueueOrFail(t, svc, "default", fmt.Sprintf("bg%d", i), 1, 100, 0)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(svc.ListSessions("")) == 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("background loop did not form 2 sessions, got %d", len(svc.ListSessions("")))
}

func TestPartitionIsolationBackground(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	cfg.TickInterval = 20 * time.Millisecond
	cfg.Partitions = []string{"fast", "slow"}
	svc := newTestService(t, cfg)
	svc.Start()
	defer svc.Stop()

	// Teams in both partitions must match independently.
	for i := 0; i < 2; i++ {
		enqueueOrFail(t, svc, "fast", fmt.Sprintf("f%d", i), 1, 100, 0)
		enqueueOrFail(t, svc, "slow", fmt.Sprintf("s%d", i), 1, 100, 0)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		fast := len(svc.ListSessions("fast"))
		slow := len(svc.ListSessions("slow"))
		if fast == 1 && slow == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("partitions did not both match: fast=%d slow=%d", len(svc.ListSessions("fast")), len(svc.ListSessions("slow")))
}

func TestSnapshotIsCopy(t *testing.T) {
	cfg := config.Default()
	cfg.QueueCapacity = 10
	svc := newTestService(t, cfg)
	enqueueOrFail(t, svc, "default", "t1", 1, 100, 0)
	enqueueOrFail(t, svc, "default", "t2", 1, 100, 0)
	svc.Tick()
	sid := svc.ListSessions("")[0].ID

	view, _ := svc.QuerySession(sid)
	view.Members[0].PlayerID = "hacked"
	again, _ := svc.QuerySession(sid)
	if again.Members[0].PlayerID == "hacked" {
		t.Fatal("session snapshot aliases internal state")
	}

	q, _ := svc.QueryQueue("default")
	if q.Waiting != 0 {
		t.Fatalf("queue waiting = %d, want 0", q.Waiting)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
