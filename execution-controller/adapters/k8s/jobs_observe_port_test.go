package k8s

import (
	"testing"

	"github.com/carolsimone/continuo/execution-controller/service/ports"
)

// The observe side of the client is consumed by the job-status handler through
// ports.JobObserver. A missing method surfaces here, not at wiring time.
func TestK8sClientImplementsJobObserver(t *testing.T) {
	var _ ports.JobObserver = (*K8sClient)(nil)
}
