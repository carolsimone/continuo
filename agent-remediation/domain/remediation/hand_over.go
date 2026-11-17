package remediation

import (
	"sort"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/pkg/domain/model"
)

// HandOver is one node a remediation leaves to a human: the node, the reason
// it was handed over for, and the message that says in words what the
// operator finds and does.
type HandOver struct {
	NodeID  string
	Code    model.HandoverReason
	Message string
}

// HandOvers is every node of an attempt's outcomes whose outcome carries a
// code, sorted by node id, each with its outcome's reason as the message. A
// node whose outcome carries no code, such as one a verified fix covers, is
// not handed over.
func HandOvers(outcomes map[string]proposal.NodeOutcome) []HandOver {
	var out []HandOver
	for id, o := range outcomes {
		if o.Code == "" {
			continue
		}
		out = append(out, HandOver{NodeID: id, Code: o.Code, Message: o.Reason})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}
