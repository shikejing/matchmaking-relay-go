// Package rule defines versioned matchmaking/relay rules.
//
// Rules are immutable and registered in a Registry. A team is stamped with the
// rule version at enqueue time and keeps that version for its whole lifetime;
// a session likewise records the version that created it. Changing the default
// rule never affects teams already queued or sessions already established.
package rule

import (
	"fmt"
	"sort"
	"sync"

	"github.com/shikejing/matchmaking-relay-go/internal/model"
)

// Rule is the contract for a rule version.
type Rule interface {
	// Version returns the immutable version number.
	Version() int
	// Name returns a human-readable rule name.
	Name() string
	// MergeScore merges member scores into the team's snapshot score.
	MergeScore(members []model.Player) int
	// EffectiveScore returns the score used for pairing decisions. It may
	// incorporate wait time; it must be a pure function of the arguments.
	EffectiveScore(team *model.Team, tick int64) int
	// RetentionTicks is how long a disconnected member is retained before
	// the timeout policy fires.
	RetentionTicks() int
	// BackfillEnabled reports whether timed-out slots are backfilled from the
	// queue (true) or the session is ended (false).
	BackfillEnabled() bool
	// BackfillTimeoutTicks is how long a backfill ticket stays active before
	// the session is ended.
	BackfillTimeoutTicks() int
	// Describe returns a human-readable description of the rule behavior.
	Describe() string
}

// DefaultVersion is the rule version used when no version is specified.
const DefaultVersion = 1

// Registry holds all known rule versions.
type Registry struct {
	mu     sync.RWMutex
	rules  map[int]Rule
	defVer int
}

// NewRegistry creates a registry with the given default version.
func NewRegistry(defaultVersion int) (*Registry, error) {
	r := &Registry{rules: map[int]Rule{}, defVer: defaultVersion}
	if err := r.Register(NewV1()); err != nil {
		return nil, err
	}
	if err := r.Register(NewV2()); err != nil {
		return nil, err
	}
	if _, ok := r.rules[defaultVersion]; !ok {
		return nil, fmt.Errorf("rule: default version %d is not registered", defaultVersion)
	}
	return r, nil
}

// Register adds a rule version. Version numbers must be unique.
func (r *Registry) Register(rule Rule) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.rules[rule.Version()]; ok {
		return fmt.Errorf("rule: version %d already registered", rule.Version())
	}
	r.rules[rule.Version()] = rule
	return nil
}

// Get returns the rule for a version, or an error if unknown.
func (r *Registry) Get(version int) (Rule, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rule, ok := r.rules[version]
	if !ok {
		return nil, fmt.Errorf("rule: version %d is not registered", version)
	}
	return rule, nil
}

// Default returns the registry's default rule.
func (r *Registry) Default() Rule {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.rules[r.defVer]
}

// DefaultVersion returns the default version number.
func (r *Registry) DefaultVersion() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.defVer
}

// SetDefault changes the default version. Existing teams/sessions are
// unaffected; only newly enqueued teams use the new default.
func (r *Registry) SetDefault(version int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.rules[version]; !ok {
		return fmt.Errorf("rule: cannot set default to unregistered version %d", version)
	}
	r.defVer = version
	return nil
}

// Versions returns all registered version numbers in ascending order.
func (r *Registry) Versions() []int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]int, 0, len(r.rules))
	for v := range r.rules {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}

// DescribeAll returns human-readable descriptions of every rule version.
func (r *Registry) DescribeAll() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	vs := make([]int, 0, len(r.rules))
	for v := range r.rules {
		vs = append(vs, v)
	}
	sort.Ints(vs)
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, r.rules[v].Describe())
	}
	return out
}
