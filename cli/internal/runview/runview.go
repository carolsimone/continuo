// Package runview builds the view of one run that `continuo run show` and
// `continuo schedule status` both print: the run itself and, for each of its
// tasks, the status and the failure of the task's latest attempt.
package runview

import (
	"context"
	"fmt"
	"io"
	"time"

	runv1 "github.com/carolsimone/continuo/cli/proto/run/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// PageSize is the page size of the task and attempt sweeps. 200 is the largest
// page the server serves for attempts.
const PageSize = 200

// Reader is the part of the run service the view reads.
type Reader interface {
	GetRun(ctx context.Context, runID string) (*runv1.RunResponse, error)
	ListTasks(ctx context.Context, runID, status string, pageSize, pageOffset int32) (*runv1.ListTasksResponse, error)
	ListRunAttempts(ctx context.Context, runID, taskID string, pageSize, pageOffset int32) (*runv1.ListRunAttemptsResponse, error)
}

// Run is the JSON form of a run. Fields the run does not have yet (a start
// time before its first task starts, a cancel that was never requested) are
// omitted; timestamps are RFC3339 in UTC.
type Run struct {
	RunID              string `json:"run_id"`
	ScheduleName       string `json:"schedule_name"`
	Kind               string `json:"kind"`
	Operation          string `json:"operation"`
	Status             string `json:"status"`
	FailureReason      string `json:"failure_reason,omitempty"`
	ReleaseID          string `json:"release_id,omitempty"`
	PromotionSeq       int64  `json:"promotion_seq"`
	InitiatedBy        string `json:"initiated_by,omitempty"`
	CreatedAt          string `json:"created_at,omitempty"`
	StartedAt          string `json:"started_at,omitempty"`
	CompletedAt        string `json:"completed_at,omitempty"`
	LastProgressAt     string `json:"last_progress_at,omitempty"`
	CancelledAt        string `json:"cancelled_at,omitempty"`
	CancelledBy        string `json:"cancelled_by,omitempty"`
	CancellationReason string `json:"cancellation_reason,omitempty"`
}

// Task is one node of a run. FailureClass and FailureReason are those of the
// task's latest attempt and are empty when that attempt did not fail or the
// task has no attempt. WaitReason and WaitingOnRunID are empty when the task
// waits for nothing besides its upstreams.
type Task struct {
	Node           string `json:"node"`
	Status         string `json:"status"`
	Attempt        int32  `json:"attempt"`
	FailureClass   string `json:"failure_class"`
	FailureReason  string `json:"failure_reason"`
	WaitReason     string `json:"wait_reason"`
	WaitingOnRunID string `json:"waiting_on_run_id"`
}

// View is a run and its tasks. Run is nil, and so prints as null, for a
// schedule that never ran.
type View struct {
	Run   *Run   `json:"run"`
	Tasks []Task `json:"tasks"`
}

// Empty is the view of a schedule that never ran.
func Empty() View { return View{Tasks: []Task{}} }

// Load reads the run and pages through all of its tasks and all of its
// attempts. Every RPC gets its own deadline of timeout. A gRPC error is
// returned as it came, for the caller to translate.
func Load(ctx context.Context, r Reader, timeout time.Duration, runID string) (View, error) {
	getCtx, cancel := context.WithTimeout(ctx, timeout)
	resp, err := r.GetRun(getCtx, runID)
	cancel()
	if err != nil {
		return View{}, err
	}

	tasks, err := loadTasks(ctx, r, timeout, runID)
	if err != nil {
		return View{}, err
	}
	latest, err := loadLatestAttempts(ctx, r, timeout, runID)
	if err != nil {
		return View{}, err
	}

	view := View{Run: ToRun(resp.GetRun()), Tasks: make([]Task, 0, len(tasks))}
	for _, t := range tasks {
		task := Task{
			Node:           Node(t.GetServiceName(), t.GetSchemaName(), t.GetTableName()),
			Status:         t.GetStatus(),
			Attempt:        t.GetAttempt(),
			WaitReason:     t.GetWaitReason(),
			WaitingOnRunID: t.GetWaitingOnRunId(),
		}
		if a, ok := latest[t.GetTaskId()]; ok {
			task.FailureClass = a.GetFailureClass()
			task.FailureReason = a.GetFailureReason()
		}
		view.Tasks = append(view.Tasks, task)
	}
	return view, nil
}

// Node names a node as "<service>.<schema>.<table>", the form of a run graph
// edge's node ids.
func Node(service, schema, table string) string {
	return service + "." + schema + "." + table
}

// Timestamp renders ts as RFC3339 in UTC, or "" when it is unset.
func Timestamp(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().UTC().Format(time.RFC3339)
}

func loadTasks(ctx context.Context, r Reader, timeout time.Duration, runID string) ([]*runv1.Task, error) {
	var all []*runv1.Task
	for offset := int32(0); ; offset += PageSize {
		pageCtx, cancel := context.WithTimeout(ctx, timeout)
		resp, err := r.ListTasks(pageCtx, runID, "", PageSize, offset)
		cancel()
		if err != nil {
			return nil, err
		}
		all = append(all, resp.GetTasks()...)
		// TotalCount (int32) is widened to int, so the comparison cannot overflow.
		if len(resp.GetTasks()) == 0 || len(all) >= int(resp.GetTotalCount()) {
			return all, nil
		}
	}
}

// loadLatestAttempts pages through every attempt of the run and returns, for
// each task id, the attempt with the highest number.
func loadLatestAttempts(ctx context.Context, r Reader, timeout time.Duration, runID string) (map[string]*runv1.TaskAttempt, error) {
	latest := map[string]*runv1.TaskAttempt{}
	seen := 0
	for offset := int32(0); ; offset += PageSize {
		pageCtx, cancel := context.WithTimeout(ctx, timeout)
		resp, err := r.ListRunAttempts(pageCtx, runID, "", PageSize, offset)
		cancel()
		if err != nil {
			return nil, err
		}
		for _, a := range resp.GetAttempts() {
			if cur, ok := latest[a.GetTaskId()]; !ok || a.GetAttempt() > cur.GetAttempt() {
				latest[a.GetTaskId()] = a
			}
		}
		seen += len(resp.GetAttempts())
		if len(resp.GetAttempts()) == 0 || seen >= int(resp.GetTotalCount()) {
			return latest, nil
		}
	}
}

// ToRun renders a run as its JSON form.
func ToRun(r *runv1.Run) *Run {
	return &Run{
		RunID:              r.GetRunId(),
		ScheduleName:       r.GetScheduleName(),
		Kind:               r.GetKind(),
		Operation:          r.GetOperation(),
		Status:             r.GetStatus(),
		FailureReason:      r.GetFailureReason(),
		ReleaseID:          r.GetReleaseId(),
		PromotionSeq:       r.GetPromotionSeq(),
		InitiatedBy:        r.GetInitiatedBy(),
		CreatedAt:          Timestamp(r.GetCreatedAt()),
		StartedAt:          Timestamp(r.GetStartedAt()),
		CompletedAt:        Timestamp(r.GetCompletedAt()),
		LastProgressAt:     Timestamp(r.GetLastProgressAt()),
		CancelledAt:        Timestamp(r.GetCancelledAt()),
		CancelledBy:        r.GetCancelledBy(),
		CancellationReason: r.GetCancellationReason(),
	}
}

// WriteHuman writes a text summary of the view: the run on one line, then one
// line per task.
func (v View) WriteHuman(w io.Writer) error {
	if v.Run == nil {
		_, err := fmt.Fprintln(w, "no run")
		return err
	}
	if _, err := fmt.Fprintf(w, "run %s  %s  %s  %d tasks\n", v.Run.RunID, v.Run.ScheduleName, v.Run.Status, len(v.Tasks)); err != nil {
		return err
	}
	for _, t := range v.Tasks {
		line := fmt.Sprintf("  %s  %s  attempt %d", t.Node, t.Status, t.Attempt)
		if t.FailureClass != "" {
			line += fmt.Sprintf("  %s %s", t.FailureClass, t.FailureReason)
		}
		if t.WaitReason != "" {
			line += fmt.Sprintf("  waiting: %s on run %s", t.WaitReason, t.WaitingOnRunID)
		}
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
	}
	return nil
}
