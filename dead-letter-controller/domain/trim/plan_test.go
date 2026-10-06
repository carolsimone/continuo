package trim

import (
	"reflect"
	"testing"
)

func id(ms, seq uint64) StreamID   { return StreamID{Ms: ms, Seq: seq} }
func pid(ms, seq uint64) *StreamID { v := id(ms, seq); return &v }

func TestPlanTrim(t *testing.T) {
	cutoff := id(1000, 0)
	cases := []struct {
		name      string
		snap      Snapshot
		wantMin   StreamID
		wantWork  []string // groups to quarantine for
		wantScanP []bool
	}{
		{"no groups at all: age cap", Snapshot{}, cutoff, nil, nil},
		{"all caught up after cutoff: trim to the oldest need",
			Snapshot{Present: []Group{{Name: "a", LastDelivered: id(2000, 0)}, {Name: "b", LastDelivered: id(1500, 0)}}},
			id(1500, 1), nil, nil},
		{"oldest pending wins over last delivered",
			Snapshot{Present: []Group{{Name: "a", LastDelivered: id(2000, 0), OldestPending: pid(1200, 0)}}},
			id(1200, 0), nil, nil},
		{"lagging group: quarantine its pre-cutoff entries, then trim to the cutoff",
			Snapshot{Present: []Group{{Name: "a", LastDelivered: id(2000, 0)}, {Name: "lag", LastDelivered: id(10, 0)}}},
			cutoff, []string{"lag"}, []bool{false}},
		{"old pending entry: quarantine with a pending scan",
			Snapshot{Present: []Group{{Name: "a", LastDelivered: id(2000, 0), OldestPending: pid(5, 0)}}},
			cutoff, []string{"a"}, []bool{true}},
		{"missing_group_age_caps_without_quarantine",
			Snapshot{Present: []Group{{Name: "a", LastDelivered: id(2000, 0)}}, Missing: []string{"never-started"}},
			cutoff, nil, nil},
		{"missing group and nothing present: age cap", Snapshot{Missing: []string{"x"}}, cutoff, nil, nil},
		{"need at the cutoff: nothing to quarantine",
			Snapshot{Present: []Group{{Name: "a", LastDelivered: id(1000, 0)}}},
			id(1000, 1), nil, nil},
		{"need one entry below the cutoff: quarantine",
			Snapshot{Present: []Group{{Name: "a", LastDelivered: id(999, 99)}}},
			cutoff, []string{"a"}, []bool{false}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := PlanTrim(c.snap, cutoff)
			if p.MinID != c.wantMin {
				t.Fatalf("MinID = %v, want %v", p.MinID, c.wantMin)
			}
			var got []string
			var scans []bool
			for _, w := range p.Quarantine {
				got = append(got, w.Group)
				scans = append(scans, w.ScanPending)
				if w.Cutoff != cutoff {
					t.Errorf("work cutoff = %v", w.Cutoff)
				}
			}
			if !reflect.DeepEqual(got, c.wantWork) || !reflect.DeepEqual(scans, c.wantScanP) {
				t.Fatalf("work = %v %v, want %v %v", got, scans, c.wantWork, c.wantScanP)
			}
		})
	}
}
