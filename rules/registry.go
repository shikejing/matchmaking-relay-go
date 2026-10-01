package rules

import (
	"errors"
	"fmt"

	"github.com/shikejing/matchmaking-relay-go/types"
)

// Registry holds known rule versions in registration order; lookups
// never mutate state.
type Registry struct {
	order []types.RuleVersion
	byID  map[types.RuleVersion]Rules
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{byID: map[types.RuleVersion]Rules{}}
}

// Register adds a ruleset version; duplicates are rejected.
func (r *Registry) Register(rs Rules) error {
	if rs == nil {
		return errors.New("rules: nil ruleset")
	}
	v := rs.Version()
	if v == "" {
		return errors.New("rules: empty version")
	}
	if _, exists := r.byID[v]; exists {
		return fmt.Errorf("rules: version already registered: %s", v)
	}
	r.byID[v] = rs
	r.order = append(r.order, v)
	return nil
}

// Get returns the ruleset for a version.
func (r *Registry) Get(v types.RuleVersion) (Rules, bool) {
	rs, ok := r.byID[v]
	return rs, ok
}

// Versions returns all registered versions in registration order.
func (r *Registry) Versions() []types.RuleVersion {
	out := make([]types.RuleVersion, len(r.order))
	copy(out, r.order)
	return out
}
