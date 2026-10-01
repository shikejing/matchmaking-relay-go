package engine

import (
	"github.com/shikejing/matchmaking-relay-go/transport"
	"github.com/shikejing/matchmaking-relay-go/types"
)

type transportPeer struct{ *transport.Peer }

func (p *transportPeer) send(from types.PlayerID, to types.PlayerID, payload []byte) {
	p.Peer.Send(transport.Message{From: from, To: to, Payload: payload})
}
