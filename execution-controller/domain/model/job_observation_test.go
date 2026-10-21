package model_test

import (
	"testing"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/pkg/parsecache"
	"github.com/stretchr/testify/assert"
)

func running() model.PodObservation {
	return model.PodObservation{Name: "p", Phase: model.PodRunning,
		Main: model.ContainerObservation{Name: "dbt-job", StartedAt: at(4)}}
}

func finished(phase model.PodPhase, exit int32, reason string) model.PodObservation {
	return model.PodObservation{Name: "p", Phase: phase, Main: model.ContainerObservation{
		Name: "dbt-job", StartedAt: at(4), Terminated: true, ExitCode: exit, TerminatedReason: reason, FinishedAt: at(31)}}
}

func TestJobObservation_Ended(t *testing.T) {
	deleting := running()
	deleting.DeletingSince = at(20)
	cases := []struct {
		name string
		obs  model.JobObservation
		want bool
	}{
		{"the main container runs", model.JobObservation{JobPresent: true, Pods: []model.PodObservation{running()}}, false},
		{"the pod finished and the Job failed", model.JobObservation{JobPresent: true, JobFailed: true,
			Pods: []model.PodObservation{finished(model.PodFailed, 1, "Error")}}, true},
		{"the pod finished, the Job has not caught up", model.JobObservation{JobPresent: true,
			Pods: []model.PodObservation{finished(model.PodSucceeded, 0, "Completed")}}, false},
		{"a pod being deleted is not finished", model.JobObservation{JobPresent: true, JobFailed: true,
			Pods: []model.PodObservation{deleting}}, false},
		{"the Job is created, its pod is not", model.JobObservation{JobPresent: true}, false},
		{"the Job is deleting and its pods are gone", model.JobObservation{JobPresent: true, JobDeleting: true}, true},
		{"the Job and its pods are gone", model.JobObservation{}, true},
		{"the Job is gone, a pod still runs", model.JobObservation{Pods: []model.PodObservation{running()}}, false},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, c.obs.Ended(), c.name)
	}
}

func TestJobObservation_StartError(t *testing.T) {
	waiting := func(reason string, init bool) model.JobObservation {
		p := model.PodObservation{Name: "p", Phase: model.PodPending,
			Main: model.ContainerObservation{Name: "dbt-job", WaitingReason: "PodInitializing"}}
		c := model.ContainerObservation{Name: "c", WaitingReason: reason, WaitingMessage: "details"}
		if init {
			p.Inits = []model.ContainerObservation{c}
		} else {
			p.Main = c
		}
		return model.JobObservation{JobPresent: true, Pods: []model.PodObservation{p}}
	}
	cases := []struct {
		reason string
		init   bool
		want   pkgmodel.FailureReason
	}{
		{"ErrImagePull", false, pkgmodel.FailureReasonImagePull},
		{"ImagePullBackOff", true, pkgmodel.FailureReasonImagePull},
		{"InvalidImageName", false, pkgmodel.FailureReasonInvalidImage},
		{"CreateContainerConfigError", false, pkgmodel.FailureReasonMissingConfig},
		{"CreateContainerError", true, pkgmodel.FailureReasonCreateContainer},
	}
	for _, c := range cases {
		reason, msg, ok := waiting(c.reason, c.init).StartError()
		assert.True(t, ok, c.reason)
		assert.Equal(t, c.want, reason, c.reason)
		assert.Equal(t, c.reason+": details", msg, c.reason)
	}
	_, _, ok := waiting("ContainerCreating", false).StartError()
	assert.False(t, ok, "a container still being created is not a start error")
}

func TestJobObservation_AwaitingStop(t *testing.T) {
	deleting := running()
	deleting.DeletingSince = at(20)
	unknown := running()
	unknown.Phase = model.PodUnknown
	finishedDeleting := finished(model.PodFailed, 1, "Error")
	finishedDeleting.DeletingSince = at(40)
	assert.True(t, model.JobObservation{Pods: []model.PodObservation{deleting}}.AwaitingStop())
	assert.True(t, model.JobObservation{Pods: []model.PodObservation{unknown}}.AwaitingStop())
	assert.False(t, model.JobObservation{Pods: []model.PodObservation{finishedDeleting}}.AwaitingStop(), "a finished pod never runs again")
	assert.False(t, model.JobObservation{Pods: []model.PodObservation{running()}}.AwaitingStop())
}

func TestJobObservation_Facts(t *testing.T) {
	oom := finished(model.PodFailed, 137, "OOMKilled")
	oom.Main.TerminatedMessage = "out of memory"
	oom.Inits = []model.ContainerObservation{{Name: parsecache.ContainerName, Terminated: true, TerminatedMessage: parsecache.Hydrated}}
	f := model.JobObservation{JobPresent: true, JobFailed: true, JobFailureReason: "BackoffLimitExceeded",
		Pods: []model.PodObservation{oom}}.Facts(*at(60))
	assert.Equal(t, at(4), f.StartedAt)
	assert.Equal(t, at(31), f.FinishedAt)
	assert.Equal(t, code(137), f.ExitCode)
	assert.Equal(t, "dbt-job", f.FailedContainer)
	assert.Equal(t, "OOMKilled", f.TerminationReason)
	assert.Equal(t, "OOMKilled: out of memory", f.Message)
	assert.Equal(t, "BackoffLimitExceeded", f.JobFailureReason)
	assert.Equal(t, "hydrated", f.ParseCache)
	assert.Nil(t, f.AwaitingStopSince)

	initFailed := model.PodObservation{Name: "p", Phase: model.PodFailed,
		Main:  model.ContainerObservation{Name: "upload", WaitingReason: "PodInitializing"},
		Inits: []model.ContainerObservation{{Name: "compile", Terminated: true, ExitCode: 2, TerminatedReason: "Error", FinishedAt: at(12)}}}
	f = model.JobObservation{JobPresent: true, JobFailed: true, Pods: []model.PodObservation{initFailed}}.Facts(*at(60))
	assert.Equal(t, code(2), f.ExitCode, "the failed init container's exit code stands in for the main container's")
	assert.Equal(t, "compile", f.FailedContainer)
	assert.Equal(t, at(12), f.FinishedAt)
	assert.Nil(t, f.StartedAt)

	evicted := running()
	evicted.Phase, evicted.Reason, evicted.Message = model.PodFailed, "Evicted", "The node was low on resource: memory."
	f = model.JobObservation{JobPresent: true, Pods: []model.PodObservation{evicted}}.Facts(*at(60))
	assert.Equal(t, "Evicted", f.PodReason)
	assert.Equal(t, "The node was low on resource: memory.", f.Message)

	degraded := running()
	degraded.Inits = []model.ContainerObservation{{Name: parsecache.ContainerName, Terminated: true, TerminatedMessage: parsecache.DegradedPrefix + "no artifact"}}
	degraded.DeletingSince = at(50)
	f = model.JobObservation{JobPresent: true, JobSucceeded: true, Pods: []model.PodObservation{degraded}}.Facts(*at(60))
	assert.Equal(t, "degraded", f.ParseCache)
	assert.Equal(t, "no artifact", f.ParseCacheReason)
	assert.True(t, f.JobSucceeded)
	assert.Equal(t, at(60), f.AwaitingStopSince, "a pod being deleted starts the stop clock")
}

func TestJobObservation_Started(t *testing.T) {
	pending := model.PodObservation{Name: "p", Phase: model.PodPending,
		Main: model.ContainerObservation{Name: "dbt-job", WaitingReason: "ErrImagePull"}}
	assert.False(t, model.JobObservation{Pods: []model.PodObservation{pending}}.Started(), "no container ran")
	assert.False(t, model.JobObservation{}.Started(), "no pod exists")
	assert.True(t, model.JobObservation{Pods: []model.PodObservation{running()}}.Started())

	compiling := model.PodObservation{Name: "p", Phase: model.PodPending,
		Main:  model.ContainerObservation{Name: "upload", WaitingReason: "PodInitializing"},
		Inits: []model.ContainerObservation{{Name: "compile", StartedAt: at(3)}}}
	obs := model.JobObservation{Pods: []model.PodObservation{compiling}}
	assert.True(t, obs.Started(), "an init container running dbt has started the pod")
	assert.False(t, obs.MainStarted(), "the main container has not")

	initFailed := pending
	initFailed.Inits = []model.ContainerObservation{{Name: "compile", Terminated: true, ExitCode: 2}}
	assert.True(t, model.JobObservation{Pods: []model.PodObservation{initFailed}}.Started(), "a terminated init container ran")
}
