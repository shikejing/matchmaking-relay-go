// Package transport provides in-process loopback pipes that simulate client
// connections without any network or external service.
package transport

import (
	"github.com/shikejing/matchmaking-relay-go/internal/model"
)

// Pipe is a client's loopback connection. The relay writes to Inbox; the
// client reads from it. Client->relay messages are delivered through the
// service API directly (in-process), so a pipe is one-directional relay->
// client, which is the direction the relay exercises.
type Pipe struct {
	Inbox chan model.Message
}

// NewPipe creates a loopback pipe with the given buffer size.
func NewPipe(bufferSize int) *Pipe {
	return &Pipe{Inbox: make(chan model.Message, bufferSize)}
}

// Recv blocks until a message arrives or the pipe is closed.
func (p *Pipe) Recv() (model.Message, bool) {
	msg, ok := <-p.Inbox
	return msg, ok
}

// TryRecv non-blockingly receives a message.
func (p *Pipe) TryRecv() (model.Message, bool) {
	select {
	case msg, ok := <-p.Inbox:
		return msg, ok
	default:
		return model.Message{}, false
	}
}
