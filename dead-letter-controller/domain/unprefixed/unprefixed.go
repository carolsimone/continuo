// Package unprefixed decides when the unprefixed key of a stream may be
// deleted. That key is the bare contract name (task.dispatch:v1), the key a
// stream had before every key moved under the continuo: prefix; no consumer
// reads it, and it may be deleted once its consumer groups have finished with
// it.
package unprefixed

import (
	"fmt"
	"slices"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/trim"
)

// Group is one consumer group on an unprefixed key.
type Group struct {
	Name string
	// Pending is the number of entries the group was delivered and has not
	// acknowledged.
	Pending int64
	// LastDelivered is the last entry the group was delivered.
	LastDelivered trim.StreamID
}

// Key is the state of the unprefixed key of one stream.
type Key struct {
	// Groups is every consumer group the key holds.
	Groups []Group
	// Last is the key's last entry; nil when the key holds no entry.
	Last *trim.StreamID
}

// Held returns, for each group of contractGroups that has not finished with k,
// why: it has entries pending, or it was not delivered k's last entry. A group
// k holds that contractGroups does not name is ignored: no consumer reads it,
// so it never finishes. An empty result means k may be deleted.
func Held(k Key, contractGroups []string) []string {
	var held []string
	for _, g := range k.Groups {
		if !slices.Contains(contractGroups, g.Name) {
			continue
		}
		switch {
		case g.Pending > 0:
			held = append(held, fmt.Sprintf("%s: %d pending", g.Name, g.Pending))
		case k.Last != nil && g.LastDelivered.Less(*k.Last):
			held = append(held, fmt.Sprintf("%s: delivered up to %s, the last entry is %s", g.Name, g.LastDelivered, *k.Last))
		}
	}
	return held
}
