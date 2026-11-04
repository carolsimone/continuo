package unprefixed

import (
	"slices"
	"testing"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/trim"
)

func id(ms, seq uint64) trim.StreamID   { return trim.StreamID{Ms: ms, Seq: seq} }
func pid(ms, seq uint64) *trim.StreamID { v := id(ms, seq); return &v }

var contract = []string{"svc-a", "svc-b"}

func TestHeldIsEmptyWhenEveryGroupFinished(t *testing.T) {
	k := Key{
		Groups: []Group{
			{Name: "svc-a", LastDelivered: id(9, 0)},
			// The last entry was deleted after svc-b read past it.
			{Name: "svc-b", LastDelivered: id(12, 0)},
		},
		Last: pid(9, 0),
	}
	if held := Held(k, contract); len(held) != 0 {
		t.Fatalf("Held = %v, want none", held)
	}
}

func TestHeldNamesAGroupWithPendingEntries(t *testing.T) {
	k := Key{Groups: []Group{{Name: "svc-a", Pending: 2, LastDelivered: id(9, 0)}}, Last: pid(9, 0)}
	held := Held(k, contract)
	if !slices.Equal(held, []string{"svc-a: 2 pending"}) {
		t.Fatalf("Held = %v", held)
	}
}

func TestHeldNamesAGroupNotDeliveredTheLastEntry(t *testing.T) {
	k := Key{Groups: []Group{{Name: "svc-b", LastDelivered: id(5, 0)}}, Last: pid(9, 1)}
	held := Held(k, contract)
	if !slices.Equal(held, []string{"svc-b: delivered up to 5-0, the last entry is 9-1"}) {
		t.Fatalf("Held = %v", held)
	}
}

func TestHeldNamesEachGroupThatHasNotFinished(t *testing.T) {
	k := Key{
		Groups: []Group{
			{Name: "svc-a", Pending: 1, LastDelivered: id(5, 0)},
			{Name: "svc-b", LastDelivered: id(5, 0)},
		},
		Last: pid(9, 0),
	}
	held := Held(k, contract)
	want := []string{"svc-a: 1 pending", "svc-b: delivered up to 5-0, the last entry is 9-0"}
	if !slices.Equal(held, want) {
		t.Fatalf("Held = %v, want %v", held, want)
	}
}

// A group the contract no longer declares has no consumer, so what it has not
// read never will be: it does not hold the key.
func TestHeldIgnoresAGroupOutsideTheContract(t *testing.T) {
	k := Key{
		Groups: []Group{
			{Name: "renamed-group", Pending: 3, LastDelivered: id(1, 0)},
			{Name: "svc-a", LastDelivered: id(9, 0)},
		},
		Last: pid(9, 0),
	}
	if held := Held(k, contract); len(held) != 0 {
		t.Fatalf("Held = %v, want none", held)
	}
}

func TestHeldOfAKeyWithNoEntry(t *testing.T) {
	if held := Held(Key{Groups: []Group{{Name: "svc-a"}}}, contract); len(held) != 0 {
		t.Fatalf("Held = %v, want none for an empty key", held)
	}
	// Entries trimmed while pending still count until the group acknowledges them.
	held := Held(Key{Groups: []Group{{Name: "svc-a", Pending: 1}}}, contract)
	if !slices.Equal(held, []string{"svc-a: 1 pending"}) {
		t.Fatalf("Held = %v", held)
	}
}
