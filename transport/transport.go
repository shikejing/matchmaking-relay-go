// Package transport provides in-process connection simulation:
// clients connect over in-memory loopback pipes, so the whole chain
// depends on no external service.
//
// Each Dial produces two paired halves:
//
//	Peer       - held by the (simulated) client
//	ServerPipe - held by the relay session
//
// A pipe has bounded buffers; sends to a full/disconnected pipe fail
// rather than block forever, bounding memory under load.
package transport

import (
	"sync"

	"github.com/shikejing/matchmaking-relay-go/types"
)

// Message is a relay envelope between session members.
type Message struct {
	From    types.PlayerID
	To      types.PlayerID // empty means broadcast
	Payload []byte
}

// pipe holds the shared state of a paired client/server connection.
type pipe struct {
	mu     sync.Mutex
	closed bool
	// client-bound messages written by the server.
	toClient chan Message
	// server-bound messages written by the client.
	toServer chan Message
}

func newPipe(buf int) *pipe {
	return &pipe{toClient: make(chan Message, buf), toServer: make(chan Message, buf)}
}

func (p *pipe) closeLocked() {
	if !p.closed {
		p.closed = true
		close(p.toClient)
		close(p.toServer)
	}
}

// trySend attempts a non-blocking send; it reports false when the
// buffer is full or the pipe is closed.
func (p *pipe) trySend(ch chan Message, m Message) bool {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return false
	}
	p.mu.Unlock()
	select {
	case ch <- m:
		return true
	default:
		return false
	}
}

// Peer is the client half of an in-memory loopback connection.
type Peer struct {
	player types.PlayerID
	p      *pipe
}

// Send pushes a client->server message; false when full or disconnected.
func (p *Peer) Send(m Message) bool {
	m.From = p.player
	return p.p.trySend(p.p.toServer, m)
}

// Recv returns the client-side receive channel; after Disconnect it is
// closed and receives yield the zero value with ok==false.
func (p *Peer) Recv() <-chan Message { return p.p.toClient }

// Disconnect simulates an abrupt network drop: both directions close.
func (p *Peer) Disconnect() {
	p.p.mu.Lock()
	defer p.p.mu.Unlock()
	p.p.closeLocked()
}

// IsClosed reports whether this connection has dropped.
func (p *Peer) IsClosed() bool {
	p.p.mu.Lock()
	defer p.p.mu.Unlock()
	return p.p.closed
}

// Player returns the peer's player id.
func (p *Peer) Player() types.PlayerID { return p.player }

// ServerPipe is the session-side half of a connection.
type ServerPipe struct {
	player types.PlayerID
	p      *pipe
}

// In returns the channel of messages sent by the client.
func (s *ServerPipe) In() <-chan Message { return s.p.toServer }

// Send pushes a server->client message; false when full/disconnected.
func (s *ServerPipe) Send(m Message) bool { return s.p.trySend(s.p.toClient, m) }

// Closed reports whether the client side dropped.
func (s *ServerPipe) Closed() bool {
	s.p.mu.Lock()
	defer s.p.mu.Unlock()
	return s.p.closed
}

// Player returns this pipe's player id.
func (s *ServerPipe) Player() types.PlayerID { return s.player }

// Broker mints in-memory loopback connections on demand.
type Broker struct {
	pipeBuffer int
}

// NewBroker creates a Broker; pipeBuffer bounds every per-direction
// channel (negative values are treated as zero).
func NewBroker(pipeBuffer int) *Broker {
	if pipeBuffer < 0 {
		pipeBuffer = 0
	}
	return &Broker{pipeBuffer: pipeBuffer}
}

// Dial creates a paired client Peer and server ServerPipe for a player.
func (b *Broker) Dial(player types.PlayerID) (*Peer, *ServerPipe) {
	p := newPipe(b.pipeBuffer)
	return &Peer{player: player, p: p}, &ServerPipe{player: player, p: p}
}
