package deadletter

import "fmt"

// Contract is what a redrive may address: every stream the streams contract
// declares, with the consumer groups that read it, and the streams retired from
// it. The open dead letters of a stream or group the contract retires or drops
// can never be redriven: a redrive would publish to a stream or a group that
// nothing reads, report success and lose the message.
type Contract struct {
	groups  map[string]map[string]bool
	retired map[string]bool
}

// NewContract builds a Contract from the contract's stream-to-groups map and
// its retired stream names.
func NewContract(groups map[string][]string, retired []string) Contract {
	c := Contract{groups: make(map[string]map[string]bool, len(groups)), retired: make(map[string]bool, len(retired))}
	for stream, names := range groups {
		set := make(map[string]bool, len(names))
		for _, g := range names {
			set[g] = true
		}
		c.groups[stream] = set
	}
	for _, stream := range retired {
		c.retired[stream] = true
	}
	return c
}

// RedriveRefusal returns why d can never be redriven under c, or "" when it
// can. Age is not considered: CheckRedrive refuses a dead letter past the
// replay horizon.
func (d DeadLetter) RedriveRefusal(c Contract) string {
	if !d.Publishable {
		return "its fields are not a publishable message"
	}
	if c.retired[d.Stream] {
		return fmt.Sprintf("stream %s is retired", d.Stream)
	}
	groups, declared := c.groups[d.Stream]
	if !declared {
		return fmt.Sprintf("stream %s is not in the contract", d.Stream)
	}
	if g := d.TargetGroup(); g != "" && !groups[g] {
		return fmt.Sprintf("group %s does not read stream %s", g, d.Stream)
	}
	return ""
}

// View is a dead letter as a reader sees it under a contract. Refusal says why
// it can never be redriven; it is empty when it can be.
type View struct {
	DeadLetter
	Refusal string
}

// Redrivable reports whether the dead letter can be redriven under the
// contract it was viewed under: its fields are publishable and the contract
// still addresses its stream and group. Its age is checked when it is
// redriven (CheckRedrive).
func (v View) Redrivable() bool { return v.Refusal == "" }

// ViewUnder returns d as seen under c.
func (d DeadLetter) ViewUnder(c Contract) View {
	return View{DeadLetter: d, Refusal: d.RedriveRefusal(c)}
}
