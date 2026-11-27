package redis

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	pkgevents "github.com/carolsimone/continuo/pkg/events"
	"github.com/carolsimone/continuo/pkg/outbox"
	"github.com/carolsimone/continuo/pkg/streams"
	"github.com/carolsimone/continuo/remediation/domain/repository"
	"github.com/carolsimone/continuo/remediation/service/handlers"
	"github.com/carolsimone/continuo/remediation/service/ports"
	"github.com/carolsimone/continuo/remediation/service/uow"
)

// retryRecorder is a unit of work that records what ClassifyRejection writes.
type retryRecorder struct {
	decisions []repository.ClassificationDecision
	entries   []*outbox.Entry
	begun     bool
	committed bool
}

func (r *retryRecorder) Begin(context.Context) error { r.begun = true; return nil }
func (r *retryRecorder) Commit() error               { r.committed = true; return nil }
func (r *retryRecorder) Rollback() error             { return nil }
func (r *retryRecorder) DecisionRepo() repository.ClassificationDecisionRepository {
	return r
}
func (r *retryRecorder) OutboxRepo() outbox.Repository { return recorderOutbox{r: r} }

func (r *retryRecorder) Upsert(_ context.Context, d repository.ClassificationDecision) (bool, error) {
	r.decisions = append(r.decisions, d)
	return true, nil
}

// recorderOutbox implements the one outbox method the handler calls.
type recorderOutbox struct {
	outbox.Repository
	r *retryRecorder
}

func (o recorderOutbox) Create(_ context.Context, e *outbox.Entry) error {
	o.r.entries = append(o.r.entries, e)
	return nil
}

type fixedLog string

func (l fixedLog) Fetch(context.Context, string) (string, error) { return string(l), nil }

type fixedClock struct{}

func (fixedClock) Now() time.Time { return time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC) }

var _ ports.Clock = fixedClock{}

func retryDeps(r *retryRecorder, log string) handlers.Deps {
	return handlers.Deps{
		NewUoW:    func() uow.UnitOfWork { return r },
		LogReader: fixedLog(log),
		Clock:     fixedClock{},
		Logger:    slog.New(slog.DiscardHandler),
	}
}

// retryMessage renders a round.requested:v1 entry as the stream
// holds it, under tenantID.
func retryMessage(t *testing.T, tenantID string, p pkgevents.RoundRequested) goredis.XMessage {
	t.Helper()
	fields, err := pkgevents.RoundRequestedFields("evt-1", tenantID, "release-controller",
		time.Date(2026, 6, 23, 0, 0, 0, 0, time.UTC), p)
	require.NoError(t, err)
	return goredis.XMessage{ID: "5-0", Values: fields}
}

const retryRejection = `{"release_id":"rel-1","stage":"compile","reason":"compile_failed","repo":"o/r","commit_sha":"sha",
	"per_node":[{"node_id":"core","status":"failed","dbt_log_uri":"s3://c.log"},{"node_id":"fine","status":"ok"}]}`

// TestRoundRequestedBinding_DecodesTheTypedRetry holds that a retry entry reads through
// the typed decoder: its rejection classifies as the original did, the round is
// the retry's own, and the trigger it writes is stored under the entry's tenant.
func TestRoundRequestedBinding_DecodesTheTypedRetry(t *testing.T) {
	r := &retryRecorder{}
	h := newRoundRequestedHandler(retryDeps(r, `Compilation Error in model core: syntax error`), slog.New(slog.DiscardHandler))

	msg := retryMessage(t, "globex", pkgevents.RoundRequested{
		ReleaseID: "rel-1", Round: 3, Rejection: json.RawMessage(retryRejection),
	})
	require.NoError(t, h(context.Background(), msg))

	require.Len(t, r.decisions, 1, "only the failed node is classified")
	assert.Equal(t, "core", r.decisions[0].NodeID)
	assert.Equal(t, pkg_model.RemediationSourceCompile, r.decisions[0].Source)
	assert.Equal(t, 3, r.decisions[0].Round, "the round is the retry's")

	require.Len(t, r.entries, 1)
	assert.Equal(t, streams.HealTriggeredV1, r.entries[0].StreamName)
	tenantID, payload, err := outbox.UnwrapTenant(r.entries[0].Payload)
	require.NoError(t, err)
	assert.Equal(t, "globex", tenantID, "the retry's tenant is its envelope's")
	var trigger pkgevents.HealTriggered
	require.NoError(t, json.Unmarshal(payload, &trigger))
	assert.Equal(t, "rel-1", trigger.ReleaseID)
	assert.Equal(t, 3, trigger.Round)
	assert.True(t, r.committed)
}

// TestRoundRequestedBinding_TheRoundIsTheTypedRoundNotTheRejections holds that a
// round written inside the stored rejection never reaches the
// evidence: the retry names its round beside the rejection.
func TestRoundRequestedBinding_TheRoundIsTheTypedRoundNotTheRejections(t *testing.T) {
	r := &retryRecorder{}
	h := newRoundRequestedHandler(retryDeps(r, `Compilation Error in model core: syntax error`), slog.New(slog.DiscardHandler))
	stale := `{"release_id":"rel-1","stage":"compile","reason":"compile_failed","round":9,
		"per_node":[{"node_id":"core","status":"failed","dbt_log_uri":"s3://c.log"}]}`

	msg := retryMessage(t, "acme", pkgevents.RoundRequested{ReleaseID: "rel-1", Round: 2, Rejection: json.RawMessage(stale)})
	require.NoError(t, h(context.Background(), msg))

	require.Len(t, r.decisions, 1)
	assert.Equal(t, 2, r.decisions[0].Round)
}

// TestEvidenceFromRejected_CarriesNoRound holds that the evidence of a
// rejection names no round, whatever the rejection carries: 0 means round 1,
// which ClassifyRejection normalises, and a retry stamps its own round.
func TestEvidenceFromRejected_CarriesNoRound(t *testing.T) {
	for name, field := range map[string]string{"no round field": "", "a stored round": `"round":4,`} {
		t.Run(name, func(t *testing.T) {
			raw := []byte(`{"release_id":"rel-1","stage":"compile","reason":"compile_failed",` + field + `
			  "per_node":[{"node_id":"core","status":"failed","dbt_log_uri":"s3://c.log"}]}`)
			evs, err := evidenceFromRejected(raw)
			require.NoError(t, err)
			require.Len(t, evs, 1)
			assert.Equal(t, pkg_model.RemediationSourceCompile, evs[0].Source)
			assert.Equal(t, 0, evs[0].Round)
		})
	}
}

// TestRoundRequestedBinding_NothingToClassifyIsAcknowledged holds that a retry whose
// rejection yields no failed node to classify (a reason that is not healable, a
// shadow rejection) opens no transaction and is acknowledged.
func TestRoundRequestedBinding_NothingToClassifyIsAcknowledged(t *testing.T) {
	r := &retryRecorder{}
	h := newRoundRequestedHandler(retryDeps(r, ""), slog.New(slog.DiscardHandler))
	notHealable := `{"release_id":"rel-1","stage":"validation","reason":"upstream_clone_failed",
		"per_node":[{"node_id":"analytics.b","status":"failed","validation_op":"clone_from_prod"}]}`

	msg := retryMessage(t, "acme", pkgevents.RoundRequested{ReleaseID: "rel-1", Round: 2, Rejection: json.RawMessage(notHealable)})

	require.NoError(t, h(context.Background(), msg))
	assert.False(t, r.begun)
	assert.Empty(t, r.entries)
}

// TestRoundRequestedBinding_AnEntryNoRedeliveryCanReadIsPermanent holds that a retry
// whose envelope or payload cannot be decoded, one that names no tenant, and one
// whose rejection is not a rejection, is permanent: the consumer dead-letters it
// and writes nothing.
func TestRoundRequestedBinding_AnEntryNoRedeliveryCanReadIsPermanent(t *testing.T) {
	good := pkgevents.RoundRequested{ReleaseID: "rel-1", Round: 2, Rejection: json.RawMessage(retryRejection)}
	withField := func(name string, value any) goredis.XMessage {
		m := retryMessage(t, "acme", good)
		m.Values[name] = value
		return m
	}
	missing := func(name string) goredis.XMessage {
		m := retryMessage(t, "acme", good)
		delete(m.Values, name)
		return m
	}
	cases := map[string]goredis.XMessage{
		"no fields at all":                        {ID: "1-0", Values: map[string]any{}},
		"no payload":                              missing("payload"),
		"a payload that is not JSON":              withField("payload", "{not json"),
		"another schema version":                  withField("schema_version", "9"),
		"an unreadable occurred_at":               withField("occurred_at", "yesterday"),
		"no tenant":                               withField("tenant_id", ""),
		"a round below 2":                         retryMessage(t, "acme", pkgevents.RoundRequested{ReleaseID: "rel-1", Round: 1, Rejection: json.RawMessage(retryRejection)}),
		"no release":                              retryMessage(t, "acme", pkgevents.RoundRequested{Round: 2, Rejection: json.RawMessage(retryRejection)}),
		"a rejection that is not an object":       retryMessage(t, "acme", pkgevents.RoundRequested{ReleaseID: "rel-1", Round: 2, Rejection: json.RawMessage(`"x"`)}),
		"a rejection that does not decode as one": retryMessage(t, "acme", pkgevents.RoundRequested{ReleaseID: "rel-1", Round: 2, Rejection: json.RawMessage(`{"per_node":"x"}`)}),
	}
	for name, msg := range cases {
		t.Run(name, func(t *testing.T) {
			r := &retryRecorder{}
			h := newRoundRequestedHandler(retryDeps(r, "boom"), slog.New(slog.DiscardHandler))

			err := h(context.Background(), msg)

			require.Error(t, err)
			assert.ErrorIs(t, err, pkgevents.ErrPermanent)
			assert.False(t, r.begun)
		})
	}
}

// TestRejectionBinding_StoresTheTriggerUnderTheMessagesTenant holds that a
// release.rejected:v1 message, which carries a tenant_id field when its producer
// stamps one, hands that tenant to the classifier and falls back to the default
// tenant when it names none.
func TestRejectionBinding_StoresTheTriggerUnderTheMessagesTenant(t *testing.T) {
	for name, tc := range map[string]struct {
		values map[string]any
		want   string
	}{
		"a tenant_id field":  {map[string]any{"tenant_id": "globex"}, "globex"},
		"no tenant_id field": {map[string]any{}, pkgevents.DefaultTenantID},
		"an empty tenant_id": {map[string]any{"tenant_id": ""}, pkgevents.DefaultTenantID},
	} {
		t.Run(name, func(t *testing.T) {
			r := &retryRecorder{}
			h := newRejectionHandler(streams.ReleaseRejectedV1, retryDeps(r, `Compilation Error in model core: syntax error`), slog.New(slog.DiscardHandler))
			tc.values["payload"] = retryRejection

			require.NoError(t, h(context.Background(), goredis.XMessage{ID: "1-0", Values: tc.values}))

			require.Len(t, r.entries, 1)
			tenantID, _, err := outbox.UnwrapTenant(r.entries[0].Payload)
			require.NoError(t, err)
			assert.Equal(t, tc.want, tenantID)
			assert.Equal(t, 1, r.decisions[0].Round, "a rejection is round 1")
		})
	}
}
