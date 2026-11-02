package streamsinit

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	pkgredis "github.com/carolsimone/continuo/pkg/redis"
	"github.com/carolsimone/continuo/pkg/streams"
)

// fakeAdmin records what Run asks of Redis and remembers what exists, so a
// second run reports nothing created.
type fakeAdmin struct {
	groups   map[string]map[string]bool // key -> group -> exists
	streams  map[string]bool            // key -> exists with no declared group
	calls    int
	failOn   string // "key group" that errors
	failWith error
}

func newFakeAdmin() *fakeAdmin {
	return &fakeAdmin{groups: map[string]map[string]bool{}, streams: map[string]bool{}}
}

func (f *fakeAdmin) EnsureGroup(_ context.Context, key, group string) (bool, error) {
	f.calls++
	if f.failOn == key+" "+group {
		return false, f.failWith
	}
	if f.groups[key] == nil {
		f.groups[key] = map[string]bool{}
	}
	if f.groups[key][group] {
		return false, nil
	}
	f.groups[key][group] = true
	return true, nil
}

func (f *fakeAdmin) EnsureStream(_ context.Context, key string) (bool, error) {
	f.calls++
	if f.streams[key] {
		return false, nil
	}
	f.streams[key] = true
	return true, nil
}

func discardLogger() *slog.Logger { return slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)) }

func contractPlan() Plan { return Plan{Groups: streams.Groups, CreateStreams: true} }

func TestRunCreatesEveryGroupOfTheContract(t *testing.T) {
	admin := newFakeAdmin()
	if err := Run(context.Background(), admin, contractPlan(), discardLogger()); err != nil {
		t.Fatal(err)
	}
	for stream, groups := range streams.Groups {
		for _, g := range groups {
			if !admin.groups[pkgredis.Key(stream)][g] {
				t.Errorf("group %s of %s was not created under %s", g, stream, pkgredis.Key(stream))
			}
		}
	}
	for key := range admin.groups {
		if !strings.HasPrefix(key, streams.KeyPrefix) {
			t.Errorf("created the unprefixed key %q", key)
		}
	}
	for key := range admin.streams {
		if !strings.HasPrefix(key, streams.KeyPrefix) {
			t.Errorf("created the unprefixed key %q", key)
		}
	}
	for _, retired := range streams.Retired {
		if admin.groups[pkgredis.Key(retired)] != nil || admin.streams[pkgredis.Key(retired)] {
			t.Errorf("created the retired stream %s", retired)
		}
	}
}

func TestRunCreatesAStreamThatHasNoGroup(t *testing.T) {
	admin := newFakeAdmin()
	plan := Plan{Groups: map[string][]string{streams.RemediationProposedV1: {}}, CreateStreams: true}
	if err := Run(context.Background(), admin, plan, discardLogger()); err != nil {
		t.Fatal(err)
	}
	if !admin.streams[pkgredis.Key(streams.RemediationProposedV1)] {
		t.Fatalf("the group-less stream was not created: %v", admin.streams)
	}
	if len(admin.groups) != 0 {
		t.Fatalf("a group was created for a stream with none: %v", admin.groups)
	}
}

func TestRunEveryContractStreamExistsAfterARun(t *testing.T) {
	admin := newFakeAdmin()
	if err := Run(context.Background(), admin, contractPlan(), discardLogger()); err != nil {
		t.Fatal(err)
	}
	for _, stream := range []string{streams.ConsumerDeadLetterV1, streams.OutboxDeadLetterV1, streams.RemediationProposedV1} {
		key := pkgredis.Key(stream)
		if len(admin.groups[key]) == 0 && !admin.streams[key] {
			t.Errorf("stream %s does not exist after the run", stream)
		}
	}
}

func TestRunIsIdempotent(t *testing.T) {
	admin := newFakeAdmin()
	if err := Run(context.Background(), admin, contractPlan(), discardLogger()); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	if err := Run(context.Background(), admin, contractPlan(), logger); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), `"created":1`) {
		t.Fatalf("the second run created something:\n%s", out.String())
	}
}

func TestRunLogsOneLinePerStreamWithTheGroupsCreated(t *testing.T) {
	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	plan := Plan{Groups: map[string][]string{
		streams.ReleasePromotedV2: {streams.CatalogReleaseTopology, streams.RunControllerReleasePromoted},
	}, CreateStreams: true}
	if err := Run(context.Background(), newFakeAdmin(), plan, logger); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), `"stream":"`+streams.ReleasePromotedV2+`"`) || !strings.Contains(out.String(), `"created":2`) {
		t.Fatalf("no line naming the stream and 2 created groups:\n%s", out.String())
	}
}

func TestRunCreatesNothingWhenTurnedOff(t *testing.T) {
	admin := newFakeAdmin()
	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	plan := Plan{Groups: streams.Groups, CreateStreams: false}
	if err := Run(context.Background(), admin, plan, logger); err != nil {
		t.Fatal(err)
	}
	if admin.calls != 0 {
		t.Fatalf("admin was called %d times with creation turned off", admin.calls)
	}
	if !strings.Contains(out.String(), "REDIS_INIT_CREATE_STREAMS") {
		t.Fatalf("the log does not say creation was turned off:\n%s", out.String())
	}
}

func TestRunReturnsTheFirstErrorNamingStreamAndGroup(t *testing.T) {
	admin := newFakeAdmin()
	boom := errors.New("boom")
	admin.failOn = pkgredis.Key(streams.TaskDispatchV1) + " " + streams.ExecutionControllerTaskDispatch
	admin.failWith = boom
	err := Run(context.Background(), admin, contractPlan(), discardLogger())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap boom", err)
	}
	if !strings.Contains(err.Error(), streams.TaskDispatchV1) || !strings.Contains(err.Error(), streams.ExecutionControllerTaskDispatch) {
		t.Fatalf("err = %v, want stream and group named", err)
	}
}
