package streamsinit

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/pkg/streams"
)

// fakeAdmin records what Run asks of Redis and remembers what exists, so a
// second run reports nothing created. It is keyed by the stream names Run
// passes, which are contract names.
type fakeAdmin struct {
	groups   map[string]map[string]bool // stream -> group -> exists
	streams  map[string]bool            // stream -> exists with no declared group
	calls    int
	created  int    // calls that reported created == true
	failOn   string // "<stream> <group>" whose EnsureGroup errors
	failWith error
	// events is every call in order: "login <user> <password>",
	// "group <stream> <group>" or "stream <stream>".
	events []string
	// loginErrs is the error CheckLogin returns for a username.
	loginErrs map[string]error
}

func newFakeAdmin() *fakeAdmin {
	return &fakeAdmin{groups: map[string]map[string]bool{}, streams: map[string]bool{}}
}

func (f *fakeAdmin) CheckLogin(_ context.Context, username, password string) error {
	f.events = append(f.events, "login "+username+" "+password)
	return f.loginErrs[username]
}

func (f *fakeAdmin) EnsureGroup(_ context.Context, stream, group string) (bool, error) {
	f.calls++
	f.events = append(f.events, "group "+stream+" "+group)
	if f.failOn == stream+" "+group {
		return false, f.failWith
	}
	if f.groups[stream] == nil {
		f.groups[stream] = map[string]bool{}
	}
	if f.groups[stream][group] {
		return false, nil
	}
	f.groups[stream][group] = true
	f.created++
	return true, nil
}

func (f *fakeAdmin) EnsureStream(_ context.Context, stream string) (bool, error) {
	f.calls++
	f.events = append(f.events, "stream "+stream)
	if f.streams[stream] {
		return false, nil
	}
	f.streams[stream] = true
	f.created++
	return true, nil
}

// seen is every stream name the admin was asked about.
func (f *fakeAdmin) seen() map[string]bool {
	out := map[string]bool{}
	for stream := range f.groups {
		out[stream] = true
	}
	for stream := range f.streams {
		out[stream] = true
	}
	return out
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
			if !admin.groups[stream][g] {
				t.Errorf("group %s of %s was not created", g, stream)
			}
		}
	}
	// Run names contract streams only: the key prefix is the admin's concern.
	for name := range admin.seen() {
		if strings.HasPrefix(name, streams.KeyPrefix) {
			t.Errorf("asked for the prefixed key %q, want the contract name", name)
		}
		if _, ok := streams.Groups[name]; !ok {
			t.Errorf("asked for %q, which is not a contract stream", name)
		}
	}
	for _, retired := range streams.Retired {
		if admin.groups[retired] != nil || admin.streams[retired] {
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
	if !admin.streams[streams.RemediationProposedV1] {
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
	want := map[string]bool{}
	for stream := range streams.Groups {
		want[stream] = true
		if len(admin.groups[stream]) == 0 && !admin.streams[stream] {
			t.Errorf("stream %s does not exist after the run", stream)
		}
	}
	got := admin.seen()
	for stream := range got {
		if !want[stream] {
			t.Errorf("stream %s exists after the run but is not in the contract", stream)
		}
	}
	if len(got) != len(want) {
		t.Errorf("%d streams exist after the run, want exactly the %d of the contract", len(got), len(want))
	}
}

func TestRunIsIdempotent(t *testing.T) {
	admin := newFakeAdmin()
	if err := Run(context.Background(), admin, contractPlan(), discardLogger()); err != nil {
		t.Fatal(err)
	}
	callsBefore, createdBefore := admin.calls, admin.created
	var out bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&out, nil))
	if err := Run(context.Background(), admin, contractPlan(), logger); err != nil {
		t.Fatal(err)
	}
	if admin.calls == callsBefore {
		t.Fatal("the second run asked the admin for nothing")
	}
	if admin.created != createdBefore {
		t.Fatalf("the second run created %d streams or groups, want 0", admin.created-createdBefore)
	}
	if strings.Contains(out.String(), `"created":1`) {
		t.Fatalf("the second run logged a creation:\n%s", out.String())
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
	admin.failOn = streams.TaskDispatchV1 + " " + streams.ExecutionControllerTaskDispatch
	admin.failWith = boom
	err := Run(context.Background(), admin, contractPlan(), discardLogger())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want it to wrap boom", err)
	}
	if !strings.Contains(err.Error(), streams.TaskDispatchV1) || !strings.Contains(err.Error(), streams.ExecutionControllerTaskDispatch) {
		t.Fatalf("err = %v, want stream and group named", err)
	}
}

// writeLogin writes a login file named username holding content.
func writeLogin(t *testing.T, dir, username, content string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, username), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// loginEvents is the login checks among events.
func loginEvents(events []string) []string {
	var out []string
	for _, e := range events {
		if strings.HasPrefix(e, "login ") {
			out = append(out, e)
		}
	}
	return out
}

func TestRunChecksEveryLoginBeforeCreating(t *testing.T) {
	dir := t.TempDir()
	writeLogin(t, dir, "continuo_ui", "ui-password\n")
	writeLogin(t, dir, "continuo_catalog", "catalog-password\r\n")
	admin := newFakeAdmin()
	plan := contractPlan()
	plan.LoginsDir = dir
	if err := Run(context.Background(), admin, plan, discardLogger()); err != nil {
		t.Fatal(err)
	}
	want := []string{"login continuo_catalog catalog-password", "login continuo_ui ui-password"}
	if len(admin.events) < len(want) || !slices.Equal(admin.events[:len(want)], want) {
		t.Fatalf("events start %v, want the login checks %v before anything is created", admin.events, want)
	}
	if got := loginEvents(admin.events); !slices.Equal(got, want) {
		t.Fatalf("login checks = %v, want %v", got, want)
	}
	if admin.created == 0 {
		t.Fatal("nothing was created after the logins signed in")
	}
}

func TestRunListsEveryFailedLogin(t *testing.T) {
	dir := t.TempDir()
	writeLogin(t, dir, "continuo_ui", "ui-password\n")
	writeLogin(t, dir, "continuo_catalog", "wrong\n")
	writeLogin(t, dir, "continuo_remediation", "\n")
	writeLogin(t, dir, "continuo_run_controller", "rc-password")
	admin := newFakeAdmin()
	admin.loginErrs = map[string]error{
		"continuo_catalog":        errors.New("WRONGPASS invalid username-password pair or user is disabled."),
		"continuo_run_controller": errors.New("WRONGPASS invalid username-password pair or user is disabled."),
	}
	plan := contractPlan()
	plan.LoginsDir = dir
	err := Run(context.Background(), admin, plan, discardLogger())
	if err == nil {
		t.Fatal("Run succeeded with failing logins")
	}
	for _, want := range []string{
		"redis login continuo_catalog: WRONGPASS invalid username-password pair or user is disabled.; create it as described in deploy/CREDENTIALS.md",
		"redis login continuo_run_controller: WRONGPASS invalid username-password pair or user is disabled.; create it as described in deploy/CREDENTIALS.md",
		"redis login continuo_remediation: ",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v\nwant it to contain %q", err, want)
		}
	}
	if strings.Contains(err.Error(), "continuo_ui") {
		t.Errorf("err = %v names the login that signed in", err)
	}
	if admin.calls != 0 {
		t.Fatalf("%d streams or groups were asked for after a login failed", admin.calls)
	}
}

// A Kubernetes Secret mounted as a directory holds each key as a symbolic link
// into a hidden ..data directory: the links are the logins, the hidden entries
// are not.
func TestRunReadsAMountedSecretDirectory(t *testing.T) {
	dir := t.TempDir()
	data := filepath.Join(dir, "..2026_11_04_10_00_00.000000001")
	if err := os.Mkdir(data, 0o700); err != nil {
		t.Fatal(err)
	}
	writeLogin(t, data, "continuo_ui", "ui-password\n")
	if err := os.Symlink(filepath.Base(data), filepath.Join(dir, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..data", "continuo_ui"), filepath.Join(dir, "continuo_ui")); err != nil {
		t.Fatal(err)
	}
	admin := newFakeAdmin()
	plan := Plan{Groups: map[string][]string{}, CreateStreams: true, LoginsDir: dir}
	if err := Run(context.Background(), admin, plan, discardLogger()); err != nil {
		t.Fatal(err)
	}
	if got, want := loginEvents(admin.events), []string{"login continuo_ui ui-password"}; !slices.Equal(got, want) {
		t.Fatalf("login checks = %v, want %v", got, want)
	}
}

// Turning creation off leaves the streams to the operator; the logins are
// still checked.
func TestRunChecksLoginsWhenCreationIsTurnedOff(t *testing.T) {
	dir := t.TempDir()
	writeLogin(t, dir, "continuo_ui", "ui-password\n")
	admin := newFakeAdmin()
	plan := Plan{Groups: streams.Groups, CreateStreams: false, LoginsDir: dir}
	if err := Run(context.Background(), admin, plan, discardLogger()); err != nil {
		t.Fatal(err)
	}
	if got, want := loginEvents(admin.events), []string{"login continuo_ui ui-password"}; !slices.Equal(got, want) {
		t.Fatalf("login checks = %v, want %v", got, want)
	}
	if admin.calls != 0 {
		t.Fatalf("admin created %d streams or groups with creation turned off", admin.calls)
	}
}

func TestRunWithoutALoginsDirChecksNoLogin(t *testing.T) {
	admin := newFakeAdmin()
	if err := Run(context.Background(), admin, contractPlan(), discardLogger()); err != nil {
		t.Fatal(err)
	}
	if got := loginEvents(admin.events); len(got) != 0 {
		t.Fatalf("login checks = %v, want none", got)
	}
}

func TestRunFailsOnAnUnreadableLoginsDir(t *testing.T) {
	admin := newFakeAdmin()
	plan := contractPlan()
	plan.LoginsDir = filepath.Join(t.TempDir(), "missing")
	err := Run(context.Background(), admin, plan, discardLogger())
	if err == nil || !strings.Contains(err.Error(), "REDIS_LOGINS_DIR") {
		t.Fatalf("err = %v, want it to name REDIS_LOGINS_DIR", err)
	}
	if admin.calls != 0 {
		t.Fatalf("%d streams or groups were asked for", admin.calls)
	}
}
