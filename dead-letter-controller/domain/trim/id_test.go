package trim

import (
	"math"
	"testing"
	"time"
)

func TestParseAndOrder(t *testing.T) {
	a, err := ParseID("1759665600000-3")
	if err != nil || a.Ms != 1759665600000 || a.Seq != 3 || a.String() != "1759665600000-3" {
		t.Fatalf("a = %+v %v", a, err)
	}
	b, _ := ParseID("1759665600000-4")
	c, _ := ParseID("1759665600001-0")
	if !a.Less(b) || !b.Less(c) || c.Less(a) || a.Less(a) {
		t.Fatal("ordering")
	}
	if a.Next() != b {
		t.Fatalf("next = %v", a.Next())
	}
	if (StreamID{Ms: 5, Seq: math.MaxUint64}).Next() != (StreamID{Ms: 6}) {
		t.Fatal("seq overflow must carry into ms")
	}
	for _, bad := range []string{"", "x", "1-", "-1", "1-2-3"} {
		if _, err := ParseID(bad); err == nil {
			t.Errorf("ParseID(%q) accepted", bad)
		}
	}
}

func TestStreamIDTimePreservesUnsignedMilliseconds(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   string
		sec  int64
		nsec int
	}{
		{name: "epoch", id: "0-0", sec: 0, nsec: 0},
		{name: "millisecond", id: "1-0", sec: 0, nsec: 1_000_000},
		{name: "last millisecond", id: "999-0", sec: 0, nsec: 999_000_000},
		{name: "whole second", id: "1000-0", sec: 1, nsec: 0},
		{name: "ordinary timestamp", id: "1759665600123-3", sec: 1_759_665_600, nsec: 123_000_000},
		{name: "signed maximum", id: "9223372036854775807-0", sec: 9_223_372_036_854_775, nsec: 807_000_000},
		{name: "above signed maximum", id: "9223372036854775808-0", sec: 9_223_372_036_854_775, nsec: 808_000_000},
		{name: "unsigned maximum", id: "18446744073709551615-18446744073709551615", sec: 18_446_744_073_709_551, nsec: 615_000_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, err := ParseID(tc.id)
			if err != nil {
				t.Fatal(err)
			}
			got := id.Time()
			if got.Unix() != tc.sec || got.Nanosecond() != tc.nsec {
				t.Fatalf("Time(%s) = (%d, %d), want (%d, %d)", tc.id, got.Unix(), got.Nanosecond(), tc.sec, tc.nsec)
			}
			if got.Location() != time.UTC {
				t.Fatalf("Time(%s) location = %v, want UTC", tc.id, got.Location())
			}
		})
	}
}
