// Package watch observes every Job execution-controller creates, and the
// Job's pods, through shared informers on objects labelled app=dbt-job in the
// Job namespace. Each object is trimmed before it enters the cache, and every
// change to a Job or one of its pods queues the Job's name for the observer.
package watch

import (
	"context"
	"fmt"

	"github.com/carolsimone/continuo/execution-controller/adapters/k8s"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/service/ports"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	batchlisters "k8s.io/client-go/listers/batch/v1"
	"k8s.io/client-go/tools/cache"
)

// legacyJobNameLabel is the label the Job controller sets on a Job's pods
// beside batchv1.JobNameLabel.
const legacyJobNameLabel = "job-name"

// byJob indexes pods by the name of the Job that created them.
const byJob = "job"

// Watch is the ports.JobCache over shared Job and pod informers, and the
// ports.JobLookup that reads the same Jobs and pods from the API directly.
type Watch struct {
	cs         kubernetes.Interface
	factory    informers.SharedInformerFactory
	jobs       batchlisters.JobLister
	pods       cache.Indexer
	jobsSynced cache.InformerSynced
	podsSynced cache.InformerSynced
	namespace  string
}

var (
	_ ports.JobCache  = (*Watch)(nil)
	_ ports.JobLookup = (*Watch)(nil)
)

// New builds a Watch on namespace whose changes queue Job names on q. Start
// runs it.
func New(cs kubernetes.Interface, namespace string, q ports.ObservationQueue) (*Watch, error) {
	factory := informers.NewSharedInformerFactoryWithOptions(cs, 0,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = k8s.JobSelector }),
		informers.WithTransform(trim))
	jobInformer := factory.Batch().V1().Jobs().Informer()
	podInformer := factory.Core().V1().Pods().Informer()
	if err := podInformer.AddIndexers(cache.Indexers{byJob: func(obj any) ([]string, error) {
		if name := jobNameOfPod(obj); name != "" {
			return []string{name}, nil
		}
		return nil, nil
	}}); err != nil {
		return nil, fmt.Errorf("index pods by job: %w", err)
	}
	enqueue := func(name func(any) string) cache.ResourceEventHandlerFuncs {
		add := func(obj any) {
			if n := name(obj); n != "" {
				q.Add(n)
			}
		}
		return cache.ResourceEventHandlerFuncs{
			AddFunc:    add,
			UpdateFunc: func(_, obj any) { add(obj) },
			DeleteFunc: add,
		}
	}
	_, _ = jobInformer.AddEventHandler(enqueue(jobNameOfJob))
	_, _ = podInformer.AddEventHandler(enqueue(jobNameOfPod))
	return &Watch{
		cs: cs, factory: factory, jobs: factory.Batch().V1().Jobs().Lister(), pods: podInformer.GetIndexer(),
		jobsSynced: jobInformer.HasSynced, podsSynced: podInformer.HasSynced, namespace: namespace,
	}, nil
}

// Start runs the informers until ctx ends.
func (w *Watch) Start(ctx context.Context) { w.factory.Start(ctx.Done()) }

// WaitForCacheSync blocks until both caches have listed every object once, or
// ctx ends; it reports whether they synced.
func (w *Watch) WaitForCacheSync(ctx context.Context) bool {
	return cache.WaitForCacheSync(ctx.Done(), w.jobsSynced, w.podsSynced)
}

// Synced reports whether both caches have listed every object once.
func (w *Watch) Synced() bool { return w.jobsSynced() && w.podsSynced() }

// Observe returns what the cache holds of jobName's Job and pods.
func (w *Watch) Observe(jobName string) model.JobObservation {
	job, err := w.jobs.Jobs(w.namespace).Get(jobName)
	if err != nil {
		job = nil // not in the cache
	}
	objs, _ := w.pods.ByIndex(byJob, jobName)
	pods := make([]*corev1.Pod, 0, len(objs))
	for _, o := range objs {
		if p, ok := o.(*corev1.Pod); ok {
			pods = append(pods, p)
		}
	}
	return observation(job, pods)
}

// ObserveLive reads jobName's Job and its pods from the API, past the cache: a
// GET of the Job and a LIST of the pods its job-name label names (the current
// label, else the legacy one). The observer reads it before it settles an
// attempt, since the pod watch can trail the Job watch.
func (w *Watch) ObserveLive(ctx context.Context, jobName string) (model.JobObservation, error) {
	job, err := w.cs.BatchV1().Jobs(w.namespace).Get(ctx, jobName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		job, err = nil, nil
	}
	if err != nil {
		return model.JobObservation{}, fmt.Errorf("read Job %s: %w", jobName, err)
	}
	pods, err := w.livePods(ctx, batchv1.JobNameLabel, jobName)
	if err == nil && len(pods) == 0 {
		pods, err = w.livePods(ctx, legacyJobNameLabel, jobName)
	}
	if err != nil {
		return model.JobObservation{}, fmt.Errorf("list the pods of Job %s: %w", jobName, err)
	}
	return observation(job, pods), nil
}

// livePods lists the pods whose label names jobName.
func (w *Watch) livePods(ctx context.Context, label, jobName string) ([]*corev1.Pod, error) {
	list, err := w.cs.CoreV1().Pods(w.namespace).List(ctx, metav1.ListOptions{LabelSelector: label + "=" + jobName})
	if err != nil {
		return nil, err
	}
	pods := make([]*corev1.Pod, 0, len(list.Items))
	for i := range list.Items {
		pods = append(pods, &list.Items[i])
	}
	return pods, nil
}

// jobNameOfJob is a Job's name, unwrapping a deletion the informer missed.
func jobNameOfJob(obj any) string {
	if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = t.Obj
	}
	if j, ok := obj.(*batchv1.Job); ok {
		return j.Name
	}
	return ""
}

// jobNameOfPod is the name of the Job that created a pod.
func jobNameOfPod(obj any) string {
	if t, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = t.Obj
	}
	p, ok := obj.(*corev1.Pod)
	if !ok {
		return ""
	}
	if name := p.Labels[batchv1.JobNameLabel]; name != "" {
		return name
	}
	return p.Labels[legacyJobNameLabel]
}
