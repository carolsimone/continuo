package watch

import (
	"sort"

	"github.com/carolsimone/continuo/execution-controller/adapters/k8s"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// observation maps a cached Job (nil when the cache holds none) and its pods
// onto the domain's view of an attempt. A Job's success is its Complete or
// SuccessCriteriaMet condition, its failure its Failed or FailureTarget one:
// Kubernetes sets the latter two while the pods still terminate.
func observation(job *batchv1.Job, pods []*corev1.Pod) model.JobObservation {
	var o model.JobObservation
	if job != nil {
		o.JobPresent = true
		o.JobTaskID, o.JobAttempt = job.Labels[k8s.LabelTaskID], job.Labels[k8s.LabelAttempt]
		o.JobDeleting = job.DeletionTimestamp != nil
		for _, c := range job.Status.Conditions {
			if c.Status != corev1.ConditionTrue {
				continue
			}
			switch c.Type {
			case batchv1.JobComplete, batchv1.JobSuccessCriteriaMet:
				o.JobSucceeded = true
			case batchv1.JobFailed, batchv1.JobFailureTarget:
				o.JobFailed = true
				if o.JobFailureReason == "" {
					o.JobFailureReason = c.Reason
				}
			}
		}
	}
	sorted := append([]*corev1.Pod(nil), pods...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	for _, p := range sorted {
		o.Pods = append(o.Pods, podObservation(p))
	}
	return o
}

func podObservation(p *corev1.Pod) model.PodObservation {
	po := model.PodObservation{Name: p.Name, Phase: model.PodPhase(p.Status.Phase),
		Reason: p.Status.Reason, Message: p.Status.Message}
	if p.DeletionTimestamp != nil {
		t := p.DeletionTimestamp.Time
		po.DeletingSince = &t
	}
	if len(p.Status.ContainerStatuses) > 0 {
		po.Main = containerObservation(p.Status.ContainerStatuses[0])
	}
	for _, s := range p.Status.InitContainerStatuses {
		po.Inits = append(po.Inits, containerObservation(s))
	}
	return po
}

func containerObservation(s corev1.ContainerStatus) model.ContainerObservation {
	c := model.ContainerObservation{Name: s.Name}
	switch {
	case s.State.Running != nil:
		t := s.State.Running.StartedAt.Time
		c.StartedAt = &t
	case s.State.Terminated != nil:
		term := s.State.Terminated
		if !term.StartedAt.IsZero() {
			t := term.StartedAt.Time
			c.StartedAt = &t
		}
		if !term.FinishedAt.IsZero() {
			t := term.FinishedAt.Time
			c.FinishedAt = &t
		}
		c.Terminated, c.ExitCode = true, term.ExitCode
		c.TerminatedReason, c.TerminatedMessage = term.Reason, term.Message
	case s.State.Waiting != nil:
		c.WaitingReason, c.WaitingMessage = s.State.Waiting.Reason, s.State.Waiting.Message
	}
	return c
}
