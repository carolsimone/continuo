package redis

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/trim"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	goredis "github.com/redis/go-redis/v9"
)

// StreamInspector reads consumer-group state from Redis streams and trims them.
type StreamInspector struct{ rdb *goredis.Client }

var _ ports.StreamInspector = (*StreamInspector)(nil)

// NewStreamInspector returns an inspector over rdb.
func NewStreamInspector(rdb *goredis.Client) *StreamInspector { return &StreamInspector{rdb: rdb} }

// Snapshot returns the state of the contract groups of stream. A group the
// contract lists but Redis does not hold is Missing; a group Redis holds but the
// contract does not list is returned in unknown and excluded from the snapshot.
func (s *StreamInspector) Snapshot(ctx context.Context, stream string, contractGroups []string) (trim.Snapshot, []string, bool, error) {
	infos, err := s.rdb.XInfoGroups(ctx, stream).Result()
	if err != nil {
		if strings.Contains(err.Error(), "no such key") {
			return trim.Snapshot{}, nil, false, nil
		}
		return trim.Snapshot{}, nil, false, fmt.Errorf("xinfo groups %s: %w", stream, err)
	}
	inContract := make(map[string]bool, len(contractGroups))
	for _, g := range contractGroups {
		inContract[g] = true
	}
	snap := trim.Snapshot{Stream: stream}
	var unknown []string
	seen := make(map[string]bool, len(infos))
	for _, info := range infos {
		seen[info.Name] = true
		if !inContract[info.Name] {
			unknown = append(unknown, info.Name)
			continue
		}
		last, err := trim.ParseID(info.LastDeliveredID)
		if err != nil {
			return trim.Snapshot{}, nil, false, fmt.Errorf("group %s of %s: %w", info.Name, stream, err)
		}
		g := trim.Group{Name: info.Name, LastDelivered: last}
		if info.Pending > 0 {
			pending, err := s.rdb.XPending(ctx, stream, info.Name).Result()
			if err != nil {
				return trim.Snapshot{}, nil, false, fmt.Errorf("xpending %s %s: %w", stream, info.Name, err)
			}
			if pending.Count > 0 {
				oldest, err := trim.ParseID(pending.Lower)
				if err != nil {
					return trim.Snapshot{}, nil, false, fmt.Errorf("group %s of %s: %w", info.Name, stream, err)
				}
				g.OldestPending = &oldest
			}
		}
		snap.Present = append(snap.Present, g)
	}
	for _, g := range contractGroups {
		if !seen[g] {
			snap.Missing = append(snap.Missing, g)
		}
	}
	return snap, unknown, true, nil
}

// NeededEntries returns, in id order and at most limit long, the entries of
// stream below cutoff that group has pending or has not been delivered. A
// pending entry already removed from the stream is skipped, and so is an entry
// addressed to another group by a redrive (its redrive_group names that group):
// group ignores it when consuming, so it is not group's to quarantine.
func (s *StreamInspector) NeededEntries(ctx context.Context, stream, group string, lastDelivered, cutoff trim.StreamID, limit int) ([]trim.Entry, error) {
	if limit <= 0 {
		return nil, nil
	}
	end := "(" + cutoff.String()
	pending, err := s.pendingEntries(ctx, stream, group, end, limit)
	if err != nil {
		return nil, err
	}
	undelivered, err := s.rdb.XRangeN(ctx, stream, "("+lastDelivered.String(), end, int64(limit)).Result()
	if err != nil {
		return nil, fmt.Errorf("xrange %s: %w", stream, err)
	}
	byID := make(map[trim.StreamID]trim.Entry, len(pending)+len(undelivered))
	for _, m := range append(pending, undelivered...) {
		id, err := trim.ParseID(m.ID)
		if err != nil {
			return nil, err
		}
		fields := stringFields(m.Values)
		if target, addressed := fields[pkgredis.RedriveGroupField]; addressed && target != group {
			continue
		}
		byID[id] = trim.Entry{ID: id, Fields: fields}
	}
	out := make([]trim.Entry, 0, len(byID))
	for _, e := range byID {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID.Less(out[j].ID) })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// pendingEntries returns the entries group has pending below end (an exclusive
// range bound), oldest first, up to limit existing ones. Pending ids whose entry
// was already removed from the stream are skipped, so it pages through the
// pending list until it has limit entries or the list is exhausted.
func (s *StreamInspector) pendingEntries(ctx context.Context, stream, group, end string, limit int) ([]goredis.XMessage, error) {
	var out []goredis.XMessage
	start := "-"
	for len(out) < limit {
		page, err := s.rdb.XPendingExt(ctx, &goredis.XPendingExtArgs{
			Stream: stream, Group: group, Start: start, End: end, Count: int64(limit),
		}).Result()
		if err != nil {
			return nil, fmt.Errorf("xpending %s %s: %w", stream, group, err)
		}
		if len(page) == 0 {
			break
		}
		pipe := s.rdb.Pipeline()
		cmds := make([]*goredis.XMessageSliceCmd, len(page))
		for i, p := range page {
			cmds[i] = pipe.XRange(ctx, stream, p.ID, p.ID)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("xrange pending %s: %w", stream, err)
		}
		for _, c := range cmds {
			out = append(out, c.Val()...)
		}
		if len(page) < limit {
			break
		}
		start = "(" + page[len(page)-1].ID
	}
	return out, nil
}

// TrimBefore removes the entries of stream with an id below minID. It trims
// exactly: an approximate trim (MINID ~) only drops whole macro-nodes and so
// leaves up to a node of expired entries behind, which a small stream would
// never shed.
func (s *StreamInspector) TrimBefore(ctx context.Context, stream string, minID trim.StreamID) (int64, error) {
	n, err := s.rdb.XTrimMinID(ctx, stream, minID.String()).Result()
	if err != nil {
		return 0, fmt.Errorf("xtrim %s: %w", stream, err)
	}
	return n, nil
}

// DeleteIfExists deletes stream and reports whether it existed.
func (s *StreamInspector) DeleteIfExists(ctx context.Context, stream string) (bool, error) {
	n, err := s.rdb.Del(ctx, stream).Result()
	if err != nil {
		return false, fmt.Errorf("del %s: %w", stream, err)
	}
	return n == 1, nil
}
