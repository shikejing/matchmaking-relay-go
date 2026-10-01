// Command demo runs an entirely in-process end-to-end scenario:
// team submission -> deterministic match -> in-memory relay ->
// disconnect -> reconnect within the hold window.
package main

import (
	"fmt"

	"github.com/shikejing/matchmaking-relay-go/clock"
	"github.com/shikejing/matchmaking-relay-go/config"
	"github.com/shikejing/matchmaking-relay-go/engine"
	"github.com/shikejing/matchmaking-relay-go/transport"
	"github.com/shikejing/matchmaking-relay-go/types"
)

func main() {
	cfg := config.Default()
	cfg.TickIntervalMs = 0 // manual ticks for a reproducible demo
	cfg.RandomSeed = 42
	cfg.Partitions[0].TargetTeamSize = 4
	cfg.Partitions[0].ScoreWindow = 500

	e, err := engine.New(cfg, clock.NewRealClock())
	if err != nil {
		panic(err)
	}

	submit := func(id string, score int, players ...string) {
		ps := make([]types.Player, len(players))
		for i, p := range players {
			ps[i] = types.Player{ID: types.PlayerID(p), Score: score}
		}
		r := e.SubmitTeam("default", types.Team{ID: types.TeamID(id), Players: ps})
		if !r.Accepted {
			panic(fmt.Sprintf("submit %s rejected: %s (%s)", id, r.Reason, r.Detail))
		}
		fmt.Printf("submitted team %-5s seq=%d\n", id, r.Sequence)
	}

	submit("alpha", 100, "A1", "A2")
	submit("bravo", 110, "B1", "B2")

	sids := e.TickPartition("default")
	fmt.Printf("matched sessions: %v\n", sids)
	sid := sids[0]

	peers := map[string]*transport.Peer{}
	for _, p := range []string{"A1", "A2", "B1", "B2"} {
		peer, r := e.Dial(sid, types.PlayerID(p))
		if !r.Accepted {
			panic(fmt.Sprintf("dial %s: %s %s", p, r.Reason, r.Detail))
		}
		peers[p] = peer
	}
	fmt.Printf("session state after all joins: %s\n", e.Sessions()[0].State)

	// A1 broadcasts; B1 receives over the in-memory loopback pipe.
	peers["A1"].Send(transport.Message{From: "A1", Payload: []byte("hello relay")})
	if msg, ok := <-peers["B1"].Recv(); ok {
		fmt.Printf("B1 received from %s: %s\n", msg.From, msg.Payload)
	}

	// Simulate a network drop and an in-window reconnect. Dropping the
	// peer mirrors a physical disconnect; we then use the session's
	// synchronous Disconnect (the same transition the relay pump reaches
	// once it observes the closed pipe) for a deterministic demo.
	peers["A1"].Disconnect()
	if sess, ok := e.Manager().Get(sid); ok {
		sess.Disconnect("A1")
	}
	fmt.Printf("session state after A1 disconnect: %s\n", e.Sessions()[0].State)
	newPeer, r := e.Dial(sid, "A1")
	if !r.Accepted {
		panic(fmt.Sprintf("reconnect: %s %s", r.Reason, r.Detail))
	}
	peers["A1"] = newPeer
	fmt.Printf("session state after A1 reconnect:  %s\n", e.Sessions()[0].State)

	// Audit: every transition is traceable per session and per team.
	fmt.Println("recent session audit events:")
	for _, ev := range e.Audit().SessionHistory(sid) {
		fmt.Printf("  %-20s player=%-3s team=%-5s %s\n", ev.Kind, ev.Player, ev.Team, ev.Detail)
	}
}
