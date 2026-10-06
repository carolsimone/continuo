package trim

// Group is one contract consumer group present in Redis: the last entry it was
// delivered and, when it has pending entries, the oldest of them.
type Group struct {
	Name          string
	LastDelivered StreamID
	OldestPending *StreamID
}

// Need is the oldest entry the group has not finished: its oldest pending entry,
// or else the entry after its last delivered one.
func (g Group) Need() StreamID {
	next := g.LastDelivered.Next()
	if g.OldestPending != nil && g.OldestPending.Less(next) {
		return *g.OldestPending
	}
	return next
}

// Snapshot is one stream's consumer state. Present holds the contract groups
// that exist in Redis; Missing names the contract groups that do not (their
// service has not started yet, or is disabled).
type Snapshot struct {
	Stream  string
	Present []Group
	Missing []string
}

// Work asks for the entries Group still needs below Cutoff to be quarantined:
// every entry after LastDelivered, and, when ScanPending, its pending entries.
type Work struct {
	Group         string
	LastDelivered StreamID
	Cutoff        StreamID
	ScanPending   bool
}

// Plan says what to quarantine and where to trim: entries below MinID may be
// removed once every Work item is stored. A zero MinID trims nothing.
type Plan struct {
	Quarantine []Work
	MinID      StreamID
}

// PlanTrim plans one stream's trim against the retention cutoff. A stream is
// trimmed up to the oldest entry any present group needs. Entries below the
// cutoff that a present group still needs are quarantined first, and the trim
// then reaches the cutoff. When a contract group is missing from Redis, or no
// contract group is present, the stream keeps every entry newer than the
// cutoff and nothing is quarantined for the absent groups.
func PlanTrim(s Snapshot, cutoff StreamID) Plan {
	if len(s.Present) == 0 {
		return Plan{MinID: cutoff}
	}
	floor := s.Present[0].Need()
	var work []Work
	for _, g := range s.Present {
		need := g.Need()
		if need.Less(floor) {
			floor = need
		}
		if need.Less(cutoff) {
			work = append(work, Work{
				Group: g.Name, LastDelivered: g.LastDelivered, Cutoff: cutoff,
				ScanPending: g.OldestPending != nil && g.OldestPending.Less(cutoff),
			})
		}
	}
	if len(s.Missing) == 0 && !floor.Less(cutoff) {
		return Plan{MinID: floor}
	}
	return Plan{Quarantine: work, MinID: cutoff}
}
