package release

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/carolsimone/continuo/cli/internal/client"
	"github.com/carolsimone/continuo/cli/internal/config"
	"github.com/carolsimone/continuo/cli/internal/output"
	releasev1 "github.com/carolsimone/continuo/cli/proto/release/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeReleaseClient struct {
	resp *releasev1.GetReleaseNoticesResponse
	err  error

	gotReleaseID string
}

func (f *fakeReleaseClient) GetReleaseNotices(_ context.Context, releaseID string) (*releasev1.GetReleaseNoticesResponse, error) {
	f.gotReleaseID = releaseID
	return f.resp, f.err
}

func (f *fakeReleaseClient) Close() error { return nil }

func designNotice() *releasev1.ReleaseNotice {
	return &releasev1.ReleaseNotice{
		Kind: "seed_reload", NodeId: "finance.analytics.fx_rates", Service: "finance", EngineFamily: "schema_bound",
		Change:  "columns",
		Columns: &releasev1.ColumnChanges{Added: []string{"source"}},
		Load:    &releasev1.SeedLoad{Operation: "run", DropsTable: true, Reason: "a column was added"},
		Rebuilds: []*releasev1.Rebuild{
			{NodeId: "finance.analytics.fx_view", Materialized: "view", Operation: "run", Schedule: "daily"},
		},
		RefreshOnSchedule: []*releasev1.ScheduledRefresh{
			{NodeId: "finance.analytics.fx_mv", Materialized: "materialized_view", Schedule: "hourly"},
		},
		Warehouse: &releasev1.WarehouseFindings{
			Checked: true, Completeness: "complete",
			OutsideContinuo: []*releasev1.OutsideObject{{Object: "bi.fx_dashboard", Kind: "view", Owner: "bi"}},
		},
		Prediction: "stops", StopReason: "unowned_dependents", Unvalidated: true,
	}
}

func runNotices(t *testing.T, fake client.ReleaseClient, args ...string) ([]byte, int) {
	t.Helper()
	var out, errb bytes.Buffer
	cmd := NewNoticesCommand(func(context.Context, string) (client.ReleaseClient, error) { return fake, nil },
		&config.Config{Timeout: 2 * time.Second}, &out, &errb)
	cmd.SetArgs(args)
	cmd.SetOut(&out)
	cmd.SetErr(&errb)
	err := cmd.Execute()
	exit := 0
	var cliErr output.CLIError
	if errors.As(err, &cliErr) {
		exit = cliErr.ExitCode()
	} else if err != nil {
		exit = 1
	}
	return out.Bytes(), exit
}

func TestNotices_PrintsTheReleasesNotices(t *testing.T) {
	fake := &fakeReleaseClient{resp: &releasev1.GetReleaseNoticesResponse{ReleaseId: "rel-a1b2c3d-201", Status: "seed_building",
		Notices: []*releasev1.ReleaseNotice{designNotice()}}}
	out, code := runNotices(t, fake, "rel-a1b2c3d-201")
	require.Equal(t, 0, code)
	assert.Equal(t, "rel-a1b2c3d-201", fake.gotReleaseID)
	var got struct {
		ReleaseID string           `json:"release_id"`
		Status    string           `json:"status"`
		Notices   []map[string]any `json:"notices"`
	}
	require.NoError(t, json.Unmarshal(out, &got))
	assert.Equal(t, "rel-a1b2c3d-201", got.ReleaseID)
	assert.Equal(t, "seed_building", got.Status)
	assert.Equal(t, "stops", got.Notices[0]["prediction"])
	assert.Equal(t, []any{"source"}, got.Notices[0]["columns"].(map[string]any)["added"])
	assert.Equal(t, true, got.Notices[0]["load"].(map[string]any)["drops_table"])
	assert.Equal(t, "bi.fx_dashboard", got.Notices[0]["warehouse"].(map[string]any)["outside_continuo"].([]any)[0].(map[string]any)["object"])
	assert.Equal(t, "hourly", got.Notices[0]["refresh_on_schedule"].([]any)[0].(map[string]any)["schedule"])
	assert.Equal(t, "daily", got.Notices[0]["rebuilds"].([]any)[0].(map[string]any)["schedule"])
	assert.Equal(t, true, got.Notices[0]["unvalidated"])
}

func TestNotices_NoNoticesEncodesAnEmptyList(t *testing.T) {
	fake := &fakeReleaseClient{resp: &releasev1.GetReleaseNoticesResponse{ReleaseId: "rel-a", Status: "validating"}}
	out, code := runNotices(t, fake, "rel-a")
	require.Equal(t, 0, code)
	assert.Contains(t, string(out), `"notices":[]`)
}

func TestNotices_ANoticeWithNoNestedFindingsEncodesEmptyLists(t *testing.T) {
	fake := &fakeReleaseClient{resp: &releasev1.GetReleaseNoticesResponse{ReleaseId: "rel-a", Status: "validating",
		Notices: []*releasev1.ReleaseNotice{{NodeId: "s.t"}}}}
	out, code := runNotices(t, fake, "rel-a")
	require.Equal(t, 0, code)
	for _, want := range []string{`"rebuilds":[]`, `"refresh_on_schedule":[]`, `"added":[]`, `"removed":[]`, `"retyped":[]`, `"outside_continuo":[]`} {
		assert.Contains(t, string(out), want)
	}
}

func TestNotices_MapsTheServersRefusals(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		args     []string
		wantExit int
		wantCode string
	}{
		{"unknown release", status.Error(codes.NotFound, "no candidate release"), []string{"rel-x"}, 3, "not_found"},
		{"unreachable", status.Error(codes.Unavailable, "down"), []string{"rel-x"}, 5, "unavailable"},
		{"server fault", status.Error(codes.Internal, "boom"), []string{"rel-x"}, 6, "internal"},
		{"no argument", nil, nil, 2, "usage"},
		{"blank argument", nil, []string{" "}, 2, "usage"},
		{"two arguments", nil, []string{"a", "b"}, 2, "usage"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, code := runNotices(t, &fakeReleaseClient{err: tc.err}, tc.args...)
			assert.Equal(t, tc.wantExit, code)
			assert.Contains(t, string(out), `"`+tc.wantCode+`"`)
		})
	}
}

func TestNotices_OutputSchemaMatchesEmittedKeys(t *testing.T) {
	fake := &fakeReleaseClient{resp: &releasev1.GetReleaseNoticesResponse{ReleaseId: "rel-a", Status: "validating",
		Notices: []*releasev1.ReleaseNotice{designNotice()}}}
	out, _ := runNotices(t, fake, "rel-a")
	var emitted map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out, &emitted))
	cmd := NewNoticesCommand(nil, &config.Config{}, io.Discard, io.Discard)
	var schema map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(cmd.Annotations["output_schema"]), &schema))
	assert.ElementsMatch(t, mapKeys(emitted), mapKeys(schema))
	assert.NotContains(t, cmd.Annotations, "mutating", "release notices only reads")
}

func mapKeys(m map[string]json.RawMessage) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
