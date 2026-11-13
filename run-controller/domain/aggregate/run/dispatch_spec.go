package run

import (
	"github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/seedreload"
)

// DispatchSpec is what every dispatch of one task carries besides its node and
// attempt: the dbt verb the task applies, why the plan chose it, and the check
// a seed's pod runs before a load that drops its table. The task stores it
// when its run is planned, and its first dispatch, each retry, each question
// about an attempt and the grant of its node's lease all read it from there,
// so no dispatch of a seed's load goes out without its preflight.
type DispatchSpec struct {
	Operation model.Operation
	// Reason is empty when the plan states none.
	Reason string
	// Preflight is nil on every task but a seed whose load drops its table.
	Preflight *seedreload.Preflight
}
