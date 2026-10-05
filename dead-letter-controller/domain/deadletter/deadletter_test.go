package deadletter

import (
	"errors"
	"testing"
	"time"

	"github.com/carolsimone/continuo/pkg/domain/model"
)

var t0 = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

func open(original time.Time) DeadLetter {
	return DeadLetter{Source: SourceConsumer, Group: "g", Fields: map[string]string{"k": "v"}, Redrivable: true,
		OriginalAt: original, Status: StatusOpen}
}

func TestCheckRedrive(t *testing.T) {
	cases := []struct {
		name string
		dl   DeadLetter
		want error
	}{
		{"fresh", open(t0.Add(-time.Hour)), nil},
		{"one second before the horizon", open(t0.Add(-model.ReplayHorizon + time.Second)), nil},
		{"exactly at the horizon", open(t0.Add(-model.ReplayHorizon)), ErrExpired},
		{"not redrivable", func() DeadLetter { d := open(t0); d.Redrivable = false; return d }(), ErrNotRedrivable},
		{"already redriven and expired", func() DeadLetter {
			d := open(t0.Add(-40 * 24 * time.Hour)); d.Status = StatusRedriven; return d
		}(), nil},
	}
	for _, c := range cases {
		if got := c.dl.CheckRedrive(t0); !errors.Is(got, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func TestMarkRedriven_IsIdempotent(t *testing.T) {
	d := open(t0)
	if !d.MarkRedriven("alice", "fixed", t0) {
		t.Fatal("first mark should change the status")
	}
	if d.MarkRedriven("bob", "again", t0.Add(time.Hour)) {
		t.Fatal("second mark must be a no-op")
	}
	if d.Redrive.Actor != "alice" || d.Redrive.Reason != "fixed" || !d.Redrive.At.Equal(t0) {
		t.Fatalf("redrive = %+v", d.Redrive)
	}
}

func TestTargetGroup(t *testing.T) {
	if got := (DeadLetter{Source: SourceOutbox, Group: "x"}).TargetGroup(); got != "" {
		t.Fatalf("outbox target = %q, want every group", got)
	}
	if got := (DeadLetter{Source: SourceQuarantine, Group: "g"}).TargetGroup(); got != "g" {
		t.Fatalf("quarantine target = %q", got)
	}
}

func TestFilterNormalize(t *testing.T) {
	f, err := Filter{}.Normalize()
	if err != nil || f.Status != StatusOpen || f.Limit != 50 {
		t.Fatalf("defaults = %+v %v", f, err)
	}
	if f, _ := (Filter{Limit: 10000}).Normalize(); f.Limit != 500 {
		t.Fatalf("limit not capped: %d", f.Limit)
	}
	if _, err := (Filter{Source: "nope"}).Normalize(); !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("bad source accepted: %v", err)
	}
	if _, err := (Filter{Status: "nope"}).Normalize(); !errors.Is(err, ErrInvalidFilter) {
		t.Fatalf("bad status accepted: %v", err)
	}
}

func TestKeysAreDistinctPerSource(t *testing.T) {
	keys := map[string]bool{
		ConsumerKey("s", "g", "1-0"):   true,
		QuarantineKey("s", "g", "1-0"): true,
		OutboxKey("t", "1-0"):          true,
		UnreadableKey("s", "1-0"):      true,
	}
	if len(keys) != 4 {
		t.Fatalf("keys collide: %v", keys)
	}
}
