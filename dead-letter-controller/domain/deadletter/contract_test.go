package deadletter

import (
	"errors"
	"strings"
	"testing"
)

// testContract declares a stream two groups read, a stream no group reads and
// a retired stream.
var testContract = NewContract(map[string][]string{
	"orders.changed:v1": {"billing", "shipping"},
	"orders.audited:v1": {},
}, []string{"orders.created:v1"})

// on is an open, publishable consumer dead letter of stream for group.
func on(stream, group string) DeadLetter {
	d := open(t0)
	d.Stream, d.Group = stream, group
	return d
}

func TestRedriveRefusal(t *testing.T) {
	outbox := on("orders.audited:v1", "")
	outbox.Source = SourceOutbox
	unpublishable := on("orders.changed:v1", "billing")
	unpublishable.Publishable = false
	cases := []struct {
		name string
		dl   DeadLetter
		want string
	}{
		{"a group the contract declares", on("orders.changed:v1", "billing"), ""},
		{"an outbox dead letter of a stream no group reads", outbox, ""},
		{"fields that are not a message", unpublishable, "its fields are not a publishable message"},
		{"a retired stream", on("orders.created:v1", "billing"), "stream orders.created:v1 is retired"},
		{"a stream the contract does not name", on("orders.deleted:v1", "billing"), "stream orders.deleted:v1 is not in the contract"},
		{"a group the contract does not name", on("orders.changed:v1", "returns"), "group returns does not read stream orders.changed:v1"},
	}
	for _, c := range cases {
		if got := c.dl.RedriveRefusal(testContract); got != c.want {
			t.Errorf("%s: refusal = %q, want %q", c.name, got, c.want)
		}
	}
}

func TestCheckRedrive_RefusalCarriesItsReason(t *testing.T) {
	err := on("orders.created:v1", "billing").CheckRedrive(t0, testContract)
	if !errors.Is(err, ErrNotRedrivable) {
		t.Fatalf("err = %v, want ErrNotRedrivable", err)
	}
	if !strings.Contains(err.Error(), "stream orders.created:v1 is retired") {
		t.Fatalf("err = %q, want the reason", err)
	}
}

func TestCheckRedrive_AlreadyRedrivenOnARetiredStreamIsANoOp(t *testing.T) {
	d := on("orders.created:v1", "billing")
	d.Status = StatusRedriven
	if err := d.CheckRedrive(t0, testContract); err != nil {
		t.Fatalf("err = %v, want nil: redriving it again changes nothing", err)
	}
}

func TestViewUnder(t *testing.T) {
	if v := on("orders.changed:v1", "billing").ViewUnder(testContract); v.Refusal != "" {
		t.Fatalf("refusal = %q, want none", v.Refusal)
	}
	v := on("orders.created:v1", "billing").ViewUnder(testContract)
	if v.Refusal != "stream orders.created:v1 is retired" || v.Stream != "orders.created:v1" {
		t.Fatalf("view = %+v", v)
	}
}

// A view answers whether the dead letter can be redriven; its stored flag only
// says whether the fields are publishable.
func TestView_RedrivableFollowsTheContract(t *testing.T) {
	retired := on("orders.created:v1", "billing").ViewUnder(testContract)
	if retired.Redrivable() || !retired.Publishable {
		t.Fatalf("retired stream: Redrivable() = %v, Publishable = %v; want false, true", retired.Redrivable(), retired.Publishable)
	}
	if live := on("orders.changed:v1", "billing").ViewUnder(testContract); !live.Redrivable() {
		t.Fatalf("declared group: Redrivable() = false, refusal %q", live.Refusal)
	}
}
