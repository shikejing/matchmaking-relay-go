package transport

import (
	"testing"

	"github.com/shikejing/matchmaking-relay-go/types"
)

func TestPipeRoundTrip(t *testing.T) {
	b := NewBroker(2)
	peer, sp := b.Dial("p1")
	if peer.Player() != "p1" || sp.Player() != "p1" {
		t.Fatal("player id mismatch")
	}
	if !peer.Send(Message{Payload: []byte("up")}) {
		t.Fatal("client send failed")
	}
	m := <-sp.In()
	if string(m.Payload) != "up" || m.From != "p1" {
		t.Fatalf("server got %+v", m)
	}
	if !sp.Send(Message{Payload: []byte("down")}) {
		t.Fatal("server send failed")
	}
	if got := <-peer.Recv(); string(got.Payload) != "down" {
		t.Fatalf("client got %q", got.Payload)
	}
}

func TestPipeDisconnect(t *testing.T) {
	b := NewBroker(1)
	peer, sp := b.Dial("p1")
	peer.Disconnect()
	if !peer.IsClosed() || !sp.Closed() {
		t.Fatal("both halves must observe close")
	}
	if peer.Send(Message{}) {
		t.Fatal("send on closed pipe must fail")
	}
	if _, ok := <-peer.Recv(); ok {
		t.Fatal("client channel must be closed")
	}
	if _, ok := <-sp.In(); ok {
		t.Fatal("server channel must be closed")
	}
}

func TestPipeBoundedNonBlocking(t *testing.T) {
	b := NewBroker(0) // unbuffered
	peer, _ := b.Dial(types.PlayerID("p"))
	if peer.Send(Message{Payload: []byte("x")}) {
		t.Fatal("send into full pipe must return false, not block")
	}
}
