package trim

import (
	"math"
	"testing"
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
