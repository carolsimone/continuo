// Package observer follows every attempt execution-controller started to its
// end. A watch on Jobs and pods queues a Job's name whenever either changes
// (and every existing Job's when it starts), and a resync queues the Job of
// every started deployment each minute.
// Workers take names off the queue, record what the cache shows of the
// attempt, end an attempt that cannot start, and report the attempt's result
// once every pod of it is finished or gone.
package observer

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/model"
	"github.com/carolsimone/continuo/execution-controller/domain/repository"
	"github.com/carolsimone/continuo/execution-controller/service/outcomes"
	"github.com/carolsimone/continuo/execution-controller/service/ports"
	"github.com/carolsimone/continuo/execution-controller/service/uow"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
)

// Defaults for the zero fields of Config.
const (
	defaultMissingGrace   = 2 * time.Minute
	defaultStopAlertAfter = 15 * time.Minute
	defaultWorkers        = 8
	defaultResync         = time.Minute
	defaultLogIOTimeout   = 20 * time.Second

	// processTimeout bounds the processing of one name: a log read and its
	// uploads, one transaction and a few Kubernetes calls.
	processTimeout = time.Minute

	// liveRecheck is when an attempt the cache shows ended, but a live read
	// does not, is checked again; the watch event that catches the cache up
	// usually comes sooner.
	liveRecheck = 5 * time.Second
)

// Config holds the observer's clocks and limits. Every zero duration and a
// zero Workers select the defaults above; StartTimeout has no default.
type Config struct {
	// StartTimeout is how long a Job's pod may take, after the Job is created,
	// to start any container, init or main; past it the attempt is ended. Work
	// an init container does once it runs does not count.
	StartTimeout time.Duration
	// MissingGrace is how long a deployment must have been starting or
	// running before a Job absent from the cache can count as missing.
	MissingGrace time.Duration
	// StopAlertAfter is how long a pod may await its stop before the alert.
	StopAlertAfter time.Duration
	// Workers is how many names are processed at once.
	Workers int
	// ResyncInterval is how often the Job of every started deployment is
	// queued again.
	ResyncInterval time.Duration
	// ErrorMessageMaxLen truncates the error message reported for a failed
	// attempt; LogTailLines is how many trailing log lines feed it.
	ErrorMessageMaxLen int
	LogTailLines       int64
	// LogIOTimeout bounds one pod-log read and its uploads.
	LogIOTimeout time.Duration
}

// Deps are the observer's collaborators.
type Deps struct {
	NewUoW     func() uow.UnitOfWork
	Observed   repository.ObservedDeployments
	Cache      ports.JobCache
	Lookup     ports.JobLookup
	Terminator ports.JobTerminator
	Logs       ports.PodLogReader
	Uploader   ports.LogUploader
	Queue      ports.ObservationQueue
	Legacy     ports.LegacyCheckQueue
	Outcomes   *outcomes.Recorder
	Metrics    ports.ObserverMetrics
	Logger     *slog.Logger
}

// Observer follows attempts to their end; see the package documentation.
type Observer struct {
	cfg  Config
	deps Deps
	now  func() time.Time
}

// New builds an Observer; zero fields of cfg select the defaults.
func New(cfg Config, deps Deps) *Observer {
	if cfg.MissingGrace == 0 {
		cfg.MissingGrace = defaultMissingGrace
	}
	if cfg.StopAlertAfter == 0 {
		cfg.StopAlertAfter = defaultStopAlertAfter
	}
	if cfg.Workers == 0 {
		cfg.Workers = defaultWorkers
	}
	if cfg.ResyncInterval == 0 {
		cfg.ResyncInterval = defaultResync
	}
	if cfg.LogIOTimeout == 0 {
		cfg.LogIOTimeout = defaultLogIOTimeout
	}
	return &Observer{cfg: cfg, deps: deps, now: time.Now}
}

// Run starts the workers and resyncs now and every ResyncInterval until ctx
// ends; it then shuts the queue down and waits for the workers.
func (o *Observer) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	for range o.cfg.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for o.processNext(ctx) {
			}
		}()
	}
	ticker := time.NewTicker(o.cfg.ResyncInterval)
	defer ticker.Stop()
	for {
		if err := o.Resync(ctx); err != nil && ctx.Err() == nil {
			o.deps.Logger.Error("Queueing the started deployments failed", "error", err)
		}
		select {
		case <-ctx.Done():
			o.deps.Queue.ShutDown()
			wg.Wait()
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Resync queues the Job of every started deployment, so no attempt waits on a
// watch event that was missed, and deletes the status-check delay queue a
// previous version's pod may have written. A row done without its outcome is
// not listed: only its Job can still tell its result, and the watch delivers
// every existing Job when it starts.
func (o *Observer) Resync(ctx context.Context) error {
	names, err := o.deps.Observed.ListObservedJobNames(ctx)
	if err != nil {
		return err
	}
	for _, name := range names {
		o.deps.Queue.Add(name)
	}
	n, err := o.deps.Legacy.Clear(ctx)
	if err != nil {
		return err
	}
	if n > 0 {
		o.deps.Logger.Info("Deleted the status-check delay queue a previous version wrote", "keys", n)
	}
	return nil
}

// processNext processes one queued name; it reports false once the queue is
// shut down. A failed name is queued again after a growing backoff.
func (o *Observer) processNext(ctx context.Context) bool {
	name, shutdown := o.deps.Queue.Get()
	if shutdown {
		return false
	}
	defer o.deps.Queue.Done(name)
	if err := o.Process(ctx, name); err != nil {
		if ctx.Err() == nil {
			o.deps.Logger.Warn("Observing a Job failed; it is retried", "job_name", name, "error", err)
			o.deps.Queue.Retry(name)
		}
		return true
	}
	o.deps.Queue.Forget(name)
	return true
}

// Process observes the attempt of jobName's deployment once. A name whose
// deployment awaits no result is ignored, and so is a row done without its
// outcome whose Job is gone: its result can no longer be learned, so nothing is
// reported and no next attempt is queued. Until the cache has synced, the
// name is queued again a second later: an absence from it means nothing yet.
func (o *Observer) Process(ctx context.Context, jobName string) error {
	if !o.deps.Cache.Synced() {
		o.deps.Queue.AddAfter(jobName, time.Second)
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, processTimeout)
	defer cancel()

	dep, err := o.deps.Observed.GetByJobName(ctx, jobName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if !dep.AwaitingResult() {
		o.deps.Metrics.AwaitingPodStop(jobName, false)
		return nil
	}
	obs := o.deps.Cache.Observe(jobName)
	now := o.now()
	if dep.Status() == model.StatusDone {
		// An older version released this row before recording its result. Its
		// slot is free, so it is never ended here: it settles once its Job has
		// ended, records what the Job shows meanwhile, and is left alone once
		// the Job is gone with the result.
		switch {
		case !obs.JobPresent:
			o.deps.Metrics.AwaitingPodStop(jobName, false)
			return nil
		case obs.Ended():
			return o.finish(ctx, dep, obs, model.AttemptFacts{}, now)
		default:
			return o.record(ctx, dep, obs, now)
		}
	}

	switch {
	case dep.Status() == model.StatusTerminating:
		return o.processTerminating(ctx, dep, obs, now)
	case !obs.JobPresent:
		return o.processAbsent(ctx, dep, obs, now)
	case obs.Ended():
		return o.finish(ctx, dep, obs, model.AttemptFacts{}, now)
	}
	if reason, msg, ok := obs.StartError(); ok {
		return o.terminate(ctx, dep, obs, reason, msg, now)
	}
	if deployed := dep.DeployedAt(); deployed != nil && !obs.Started() && dep.Facts().StartedAt == nil {
		waited := now.Sub(*deployed)
		if waited >= o.cfg.StartTimeout {
			return o.terminate(ctx, dep, obs, pkgmodel.FailureReasonStartTimeout,
				fmt.Sprintf("no container of the pod had started %s after the Job was created", o.cfg.StartTimeout), now)
		}
		o.deps.Queue.AddAfter(jobName, o.cfg.StartTimeout-waited)
	}
	return o.record(ctx, dep, obs, now)
}

// processTerminating waits for the pods of an attempt execution-controller
// ended, deleting its Job again if the deletion did not take, and reports the
// attempt once they are gone.
func (o *Observer) processTerminating(ctx context.Context, dep *model.Deployment, obs model.JobObservation, now time.Time) error {
	if obs.JobPresent && !obs.JobDeleting {
		if err := o.deps.Terminator.DeleteJob(ctx, dep.JobName()); err != nil {
			return err
		}
	}
	if obs.Ended() {
		return o.finish(ctx, dep, obs, model.AttemptFacts{}, now)
	}
	return o.record(ctx, dep, obs, now)
}

// processAbsent handles a started deployment whose Job the cache does not
// hold. The cache trails the API, so the Job counts as missing only once the
// deployment has been starting or running for MissingGrace and a live read
// finds no Job; the attempt is then reported once every pod the live read
// lists has finished, from what was seen of it, or as job_missing when nothing
// seen shows how it ended.
func (o *Observer) processAbsent(ctx context.Context, dep *model.Deployment, obs model.JobObservation, now time.Time) error {
	age, err := o.deps.Observed.StateAge(ctx, dep.ID())
	if err != nil {
		return err
	}
	if age < o.cfg.MissingGrace {
		o.deps.Queue.AddAfter(dep.JobName(), o.cfg.MissingGrace-age)
		return nil
	}
	live, err := o.deps.Lookup.ObserveLive(ctx, dep.JobName())
	if err != nil {
		return err
	}
	if live.JobPresent {
		o.deps.Queue.AddAfter(dep.JobName(), o.cfg.MissingGrace)
		return nil
	}
	if !live.Ended() {
		// A pod of the missing Job has not finished: wait for it.
		return o.record(ctx, dep, live, now)
	}
	var missing model.AttemptFacts
	if f, _ := dep.Facts().Merge(live.Facts(now)); f.ExitCode == nil && !f.JobSucceeded {
		missing = model.AttemptFacts{Reason: pkgmodel.FailureReasonJobMissing,
			Message: "the Job is gone from Kubernetes and nothing seen of the attempt shows how it ended"}
	}
	return o.settleEnded(ctx, dep, obs, live, missing, now)
}

// record stores what obs shows of an attempt that has not ended. A starting
// deployment whose main container started moves to running and, for a
// production task, is reported running. The log of a pod awaiting its stop is
// read at once, since the pod may vanish.
func (o *Observer) record(ctx context.Context, dep *model.Deployment, obs model.JobObservation, now time.Time) error {
	var logFacts model.AttemptFacts
	if obs.AwaitingStop() && dep.Facts().LogKey == "" && obs.Started() {
		logFacts = o.readLogs(ctx, dep)
	}
	locked, err := o.inTx(ctx, dep.JobName(), func(u uow.UnitOfWork, d *model.Deployment) error {
		changed := d.Observe(obs.Facts(now))
		if d.Observe(logFacts) {
			changed = true
		}
		if obs.MainStarted() && d.Status() == model.StatusStarting {
			if _, err := d.MarkRunning(); err != nil {
				return err
			}
			changed = true
			if err := o.reportRunning(ctx, u, d); err != nil {
				return err
			}
		}
		if !changed {
			return nil
		}
		return u.DeploymentsRepo().Save(ctx, d)
	})
	if err != nil || locked == nil {
		return err
	}
	o.checkStop(locked, obs, now)
	return nil
}

// terminate ends an attempt that cannot run: one transaction records why and
// moves the deployment to terminating, which keeps its slot; the Job is then
// deleted with its pods. The attempt is reported once they are gone.
func (o *Observer) terminate(ctx context.Context, dep *model.Deployment, obs model.JobObservation,
	reason pkgmodel.FailureReason, message string, now time.Time) error {
	locked, err := o.inTx(ctx, dep.JobName(), func(u uow.UnitOfWork, d *model.Deployment) error {
		d.Observe(obs.Facts(now))
		if err := d.BeginTermination(reason, message); err != nil {
			return err
		}
		return u.DeploymentsRepo().Save(ctx, d)
	})
	if err != nil || locked == nil {
		return err
	}
	o.deps.Logger.Warn("Ending an attempt that cannot run; its Job is deleted",
		"job_name", dep.JobName(), "failure_reason", reason, "message", message)
	return o.deps.Terminator.DeleteJob(ctx, dep.JobName())
}

// finish reports an attempt the cache shows ended, once a live read confirms
// it: the cache trails the API (the pod watch can trail the Job watch), so a
// pod it has not delivered yet may still run. Until the API shows every pod of
// the attempt finished or gone, what the read shows is recorded and the
// attempt is checked again.
func (o *Observer) finish(ctx context.Context, dep *model.Deployment, obs model.JobObservation, extra model.AttemptFacts, now time.Time) error {
	live, err := o.deps.Lookup.ObserveLive(ctx, dep.JobName())
	if err != nil {
		return err
	}
	if !live.Ended() {
		o.deps.Queue.AddAfter(dep.JobName(), liveRecheck)
		return o.record(ctx, dep, live, now)
	}
	return o.settleEnded(ctx, dep, obs, live, extra, now)
}

// settleEnded settles an attempt a live read shows ended: it reads the log if
// none is kept yet and, in one transaction, records what the cache and the
// live read show, classifies the attempt and writes its result.
func (o *Observer) settleEnded(ctx context.Context, dep *model.Deployment, obs, live model.JobObservation,
	extra model.AttemptFacts, now time.Time) error {
	if dep.Facts().LogKey == "" && (obs.Started() || live.Started()) {
		extra, _ = extra.Merge(o.readLogs(ctx, dep))
	}
	_, err := o.inTx(ctx, dep.JobName(), func(u uow.UnitOfWork, d *model.Deployment) error {
		d.Observe(obs.Facts(now))
		d.Observe(live.Facts(now))
		d.Observe(extra)
		verdict := model.Classify(d.Facts())
		if !verdict.Succeeded {
			d.Observe(model.AttemptFacts{Reason: verdict.Reason, Message: fallbackMessage(verdict.Reason)})
		}
		return o.settle(ctx, u, d, verdict, now)
	})
	o.deps.Metrics.AwaitingPodStop(dep.JobName(), false)
	return err
}

// fallbackMessage is the message of a failure no container or pod explained.
func fallbackMessage(r pkgmodel.FailureReason) string {
	if r == pkgmodel.FailureReasonNodeLost {
		return "the pod was gone before its main container terminated"
	}
	return ""
}

// checkStop raises the alert on an attempt whose pod has awaited its stop
// for StopAlertAfter, and queues the attempt to be checked again until then.
func (o *Observer) checkStop(dep *model.Deployment, obs model.JobObservation, now time.Time) {
	since := dep.Facts().AwaitingStopSince
	if !obs.AwaitingStop() || since == nil {
		o.deps.Metrics.AwaitingPodStop(dep.JobName(), false)
		return
	}
	waited := now.Sub(*since)
	if waited < o.cfg.StopAlertAfter {
		o.deps.Queue.AddAfter(dep.JobName(), o.cfg.StopAlertAfter-waited)
		return
	}
	o.deps.Metrics.AwaitingPodStop(dep.JobName(), true)
	for _, p := range obs.Pods {
		if !p.Finished() {
			o.deps.Logger.Error("A pod has not stopped and its attempt waits for it. If its node is lost, "+
				"run `kubectl delete pod --force --grace-period=0` on it in the Job namespace: the attempt then ends as node_lost",
				"job_name", dep.JobName(), "pod", p.Name, "waited", waited.Round(time.Second).String())
		}
	}
}

// inTx runs fn on jobName's deployment, locked, in one transaction, and
// returns the deployment fn saw. It returns nil, and runs nothing, when the
// deployment is gone or awaits no result: another worker or replica already
// settled it.
func (o *Observer) inTx(ctx context.Context, jobName string, fn func(uow.UnitOfWork, *model.Deployment) error) (*model.Deployment, error) {
	u := o.deps.NewUoW()
	if err := u.Begin(ctx); err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			_ = u.Rollback()
		}
	}()
	d, err := u.DeploymentsRepo().LoadByJobName(ctx, jobName)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if !d.AwaitingResult() {
		return nil, nil
	}
	if err := fn(u, d); err != nil {
		return nil, err
	}
	if err := u.Commit(); err != nil {
		return nil, err
	}
	committed = true
	return d, nil
}
