package reports

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/carolsimone/continuo/execution-controller/domain/command"
	"github.com/carolsimone/continuo/execution-controller/domain/event"
	"github.com/carolsimone/continuo/execution-controller/domain/model"
	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingOutbox struct {
	outbox.Repository
	created []*outbox.Entry
}

func (r *recordingOutbox) Create(_ context.Context, e *outbox.Entry) error {
	r.created = append(r.created, e)
	return nil
}

func payloadOf(t *testing.T, e *outbox.Entry) pkgevents.TaskStatusReport {
	t.Helper()
	var p pkgevents.TaskStatusReport
	require.NoError(t, json.Unmarshal(e.Payload, &p))
	return p
}

// A report is kept as the attempt's last report and written to
// task.status.updated:v2 as its payload.
func TestWrite_KeepsTheReportAndWritesItsPayload(t *testing.T) {
	cmd := command.DeployTask{TaskID: uuid.NewString(), RunID: uuid.NewString(), Attempt: 1, JobName: "svc-sch-tbl-7f3e1a2b-r1"}
	d := model.NewDeployment(cmd, nil, time.Now())
	out := &recordingOutbox{}

	require.NoError(t, Write(context.Background(), out, d, pkgmodel.AttemptStatusQueued, ""))

	require.Len(t, out.created, 1)
	e := out.created[0]
	assert.Equal(t, event.EventTypeTaskStatusReport, e.EventType)
	assert.Equal(t, streams.TaskStatusUpdatedV2, e.StreamName)
	assert.Equal(t, cmd.TaskID, e.AggregateID.String(), "a task's reports are published in the order they were written")
	p := payloadOf(t, e)
	assert.Equal(t, pkgmodel.AttemptStatusQueued, p.Status)
	assert.Equal(t, int32(1), p.Attempt)
	assert.Equal(t, cmd.RunID, p.RunID)
	require.NotNil(t, d.LastReport())
	assert.Equal(t, pkgmodel.AttemptStatusQueued, d.LastReport().Status)
}

// A repeated dispatch is answered with the attempt's last report, unchanged.
func TestResend_WritesTheLastReportAgain(t *testing.T) {
	cmd := command.DeployTask{TaskID: uuid.NewString(), RunID: uuid.NewString()}
	d := model.NewDeployment(cmd, nil, time.Now())
	d.RecordReport(model.Report{Status: pkgmodel.AttemptStatusRunning})
	out := &recordingOutbox{}

	require.NoError(t, Resend(context.Background(), out, d))

	require.Len(t, out.created, 1)
	assert.Equal(t, pkgmodel.AttemptStatusRunning, payloadOf(t, out.created[0]).Status)
}

// A message longer than the limit is cut to it and marked; a shorter one, or
// any message without a limit, is kept whole.
func TestTruncateMessage(t *testing.T) {
	assert.Equal(t, "abcd...[truncated]", TruncateMessage("abcdefgh", 4))
	assert.Equal(t, "abcd", TruncateMessage("abcd", 4))
	assert.Equal(t, "abcdefgh", TruncateMessage("abcdefgh", 0))
}
