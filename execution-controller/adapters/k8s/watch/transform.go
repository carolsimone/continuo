package watch

import (
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// trim keeps of a Job or pod only what an observation reads, before it enters
// the cache: identity, labels, annotations, deletion timestamp and status.
// The pod spec, which carries env values, is not kept. Trimming a trimmed
// object returns an equal one.
func trim(obj any) (any, error) {
	switch o := obj.(type) {
	case *batchv1.Job:
		return &batchv1.Job{
			ObjectMeta: trimMeta(o.ObjectMeta),
			Status: batchv1.JobStatus{
				Conditions: o.Status.Conditions,
				StartTime:  o.Status.StartTime, CompletionTime: o.Status.CompletionTime,
				Active: o.Status.Active, Succeeded: o.Status.Succeeded, Failed: o.Status.Failed,
			},
		}, nil
	case *corev1.Pod:
		return &corev1.Pod{
			ObjectMeta: trimMeta(o.ObjectMeta),
			Status: corev1.PodStatus{
				Phase: o.Status.Phase, Reason: o.Status.Reason, Message: o.Status.Message,
				ContainerStatuses:     o.Status.ContainerStatuses,
				InitContainerStatuses: o.Status.InitContainerStatuses,
			},
		}, nil
	default:
		return obj, nil
	}
}

func trimMeta(m metav1.ObjectMeta) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name: m.Name, Namespace: m.Namespace, UID: m.UID, ResourceVersion: m.ResourceVersion,
		Labels: m.Labels, Annotations: m.Annotations,
		CreationTimestamp: m.CreationTimestamp, DeletionTimestamp: m.DeletionTimestamp,
	}
}
