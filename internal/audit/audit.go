// Package audit records an append-only, bounded history of every state
// transition and rejection, so any match or session can be traced back to its
// inputs and decision basis.
package audit

import (
	"sort"
	"sync"

	"github.com/shikejing/matchmaking-relay-go/internal/model"
)

// EventType identifies the kind of audited event.
type EventType string

const (
	EventEnqueue            EventType = "ENQUEUE"
	EventCancel             EventType = "CANCEL"
	EventReject             EventType = "REJECT"
	EventMatchFormed        EventType = "MATCH_FORMED"
	EventSessionCreated     EventType = "SESSION_CREATED"
	EventMemberJoined       EventType = "MEMBER_JOINED"
	EventMemberDisconnected EventType = "MEMBER_DISCONNECTED"
	EventMemberReconnected  EventType = "MEMBER_RECONNECTED"
	EventMemberLeft         EventType = "MEMBER_LEFT"
	EventSessionEnded       EventType = "SESSION_ENDED"
	EventBackfillStarted    EventType = "BACKFILL_STARTED"
	EventBackfillSucceeded  EventType = "BACKFILL_SUCCEEDED"
	EventBackfillTimeout    EventType = "BACKFILL_TIMEOUT"
)

// Record is one audited event.
type Record struct {
	Seq         int64 // monotonic, global order
	Tick        int64 // logical clock at event time
	Type        EventType
	Partition   model.PartitionID
	TeamID      model.TeamID
	SessionID   model.SessionID
	PlayerID    model.PlayerID
	RuleVersion int
	Reason      string // rejection reason / end reason
	Input       string // JSON snapshot of the decision inputs
	Decision    string // human-readable decision basis
}

// Filter selects records. Zero values mean "no constraint".
type Filter struct {
	Types     []EventType
	Partition model.PartitionID
	TeamID    model.TeamID
	SessionID model.SessionID
	SinceSeq  int64
	Limit     int
}

// Log is a bounded, concurrency-safe ring buffer of audit records.
type Log struct {
	mu       sync.RWMutex
	records  []Record
	capacity int
	nextSeq  int64
}

// NewLog creates an audit log retaining at most capacity records.
func NewLog(capacity int) *Log {
	return &Log{records: make([]Record, 0, capacity), capacity: capacity}
}

// Append stores a record, assigning the next sequence number.
func (l *Log) Append(r Record) Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	r.Seq = l.nextSeq
	l.nextSeq++
	if len(l.records) < l.capacity {
		l.records = append(l.records, r)
	} else {
		// Ring buffer: overwrite the oldest slot.
		idx := int(r.Seq % int64(l.capacity))
		l.records[idx] = r
	}
	return r
}

// Query returns copies of records matching the filter, in sequence order.
func (l *Log) Query(f Filter) []Record {
	l.mu.RLock()
	defer l.mu.RUnlock()
	out := make([]Record, 0, len(l.records))
	for _, r := range l.records {
		if f.SinceSeq > 0 && r.Seq < f.SinceSeq {
			continue
		}
		if f.Partition != "" && r.Partition != f.Partition {
			continue
		}
		if f.TeamID != "" && r.TeamID != f.TeamID {
			continue
		}
		if f.SessionID != "" && r.SessionID != f.SessionID {
			continue
		}
		if len(f.Types) > 0 {
			ok := false
			for _, t := range f.Types {
				if r.Type == t {
					ok = true
					break
				}
			}
			if !ok {
				continue
			}
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out
}

// Len returns the number of records currently retained.
func (l *Log) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.records)
}

// NextSeq returns the sequence number that will be assigned next.
func (l *Log) NextSeq() int64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.nextSeq
}
