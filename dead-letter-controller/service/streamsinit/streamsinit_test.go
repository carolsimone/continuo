package streamsinit

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/carolsimone/continuo/dead-letter-controller/domain/trim"
	"github.com/carolsimone/continuo/dead-letter-controller/domain/unprefixed"
	"github.com/carolsimone/continuo/dead-letter-controller/service/ports"
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
	// "group <stream> <group>", "stream <stream>", "read <stream>" or
	// "delete <stream>".
	events []string
	// loginErrs is the error CheckLogin returns for a username.
	loginErrs map[string]error
	// unprefixed holds the unprefixed key of each stream that has one.
	unprefixed map[string]unprefixed.Key
	// readErrs and deleteErrs are the errors ReadUnprefixed and
	// DeleteUnprefixed return for a stream.
	readErrs, deleteErrs map[string]error
	// deleted is every unprefixed key DeleteUnprefixed deleted, in order.
	deleted []string
	// otherTypes is the Redis type of each stream's unprefixed key that is not
	// a Redis stream.
	otherTypes map[string]string
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

func (f *fakeAdmin) StreamExists(_ context.Context, stream string) (bool, error) {
	f.events = append(f.events, "exists "+stream)
	return len(f.groups[stream]) > 0 || f.streams[stream], nil
}

func (f *fakeAdmin) ReadUnprefixed(ctx context.Context, stream string) (unprefixed.Key, bool, error) {
	f.calls++
	f.events = append(f.events, "read "+stream)
	if err := ctx.Err(); err != nil {
		return unprefixed.Key{}, false, err
	}
	if err := f.readErrs[stream]; err != nil {
		return unprefixed.Key{}, false, err
	}
	k, ok := f.unprefixed[stream]
	return k, ok, nil
}

func (f *fakeAdmin) DeleteUnprefixed(_ context.Context, stream string) (bool, string, error) {
	f.calls++
	f.events = append(f.events, "delete "+stream)
	if err := f.deleteErrs[stream]; err != nil {
		return false, "", err
	}
	if otherType, ok := f.otherTypes[stream]; ok {
		return false, otherType, nil
	}
	if _, ok := f.unprefixed[stream]; !ok {
		return false, "", nil
	}
	delete(f.unprefixed, stream)
	f.deleted = append(f.deleted, stream)
	return true, "", nil
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

func contractPlan() Plan {
	return Plan{Groups: streams.Groups, CreateStreams: true, Retired: streams.Retired, DeleteUnprefixedKeys: true}
}

// drained is the unprefixed key of stream once every contract group of it has
// read and acknowledged its last entry.
func drained(stream string) unprefixed.Key {
	last := trim.StreamID{Ms: 9}
	k := unprefixed.Key{Last: &last}
	for _, g := range streams.Groups[stream] {
		k.Groups = append(k.Groups, unprefixed.Group{Name: g, LastDelivered: last})
	}
	return k
}

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
	admin.unprefixed = map[string]unprefixed.Key{streams.TaskDispatchV1: drained(streams.TaskDispatchV1)}
	plan := contractPlan()
	plan.CreateStreams = false
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

// fakeLogins is a fixed list of Redis logins, or a listing error.
type fakeLogins struct {
	logins []ports.RedisLogin
	err    error
}

func (f fakeLogins) Logins(context.Context) ([]ports.RedisLogin, error) { return f.logins, f.err }

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
	admin := newFakeAdmin()
	plan := contractPlan()
	plan.Logins = fakeLogins{logins: []ports.RedisLogin{
		{Username: "continuo_catalog", Password: "catalog-password"},
		{Username: "continuo_ui", Password: "ui-password"},
	}}
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
	admin := newFakeAdmin()
	admin.loginErrs = map[string]error{
		"continuo_catalog":        errors.New("WRONGPASS invalid username-password pair or user is disabled."),
		"continuo_run_controller": errors.New("WRONGPASS invalid username-password pair or user is disabled."),
	}
	plan := contractPlan()
	plan.Logins = fakeLogins{logins: []ports.RedisLogin{
		{Username: "continuo_catalog", Password: "wrong"},
		{Username: "continuo_remediation", ReadErr: errors.New("REDIS_PASSWORD file is empty")},
		{Username: "continuo_run_controller", Password: "rc-password"},
		{Username: "continuo_ui", Password: "ui-password"},
	}}
	err := Run(context.Background(), admin, plan, discardLogger())
	if err == nil {
		t.Fatal("Run succeeded with failing logins")
	}
	for _, want := range []string{
		"redis login continuo_catalog: WRONGPASS invalid username-password pair or user is disabled.; create it as described in deploy/CREDENTIALS.md",
		"redis login continuo_run_controller: WRONGPASS invalid username-password pair or user is disabled.; create it as described in deploy/CREDENTIALS.md",
		"redis login continuo_remediation: REDIS_PASSWORD file is empty",
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

// Turning creation off leaves the streams to the operator; the logins are
// still checked.
func TestRunChecksLoginsWhenCreationIsTurnedOff(t *testing.T) {
	admin := newFakeAdmin()
	plan := Plan{Groups: streams.Groups, CreateStreams: false,
		Logins: fakeLogins{logins: []ports.RedisLogin{{Username: "continuo_ui", Password: "ui-password"}}}}
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

func TestRunWithoutLoginsChecksNoLogin(t *testing.T) {
	admin := newFakeAdmin()
	if err := Run(context.Background(), admin, contractPlan(), discardLogger()); err != nil {
		t.Fatal(err)
	}
	if got := loginEvents(admin.events); len(got) != 0 {
		t.Fatalf("login checks = %v, want none", got)
	}
}

func TestRunFailsWhenTheLoginsCannotBeListed(t *testing.T) {
	admin := newFakeAdmin()
	plan := contractPlan()
	plan.Logins = fakeLogins{err: errors.New("read the Redis logins of REDIS_LOGINS_DIR: no such directory")}
	err := Run(context.Background(), admin, plan, discardLogger())
	if err == nil || !strings.Contains(err.Error(), "REDIS_LOGINS_DIR") {
		t.Fatalf("err = %v, want it to name REDIS_LOGINS_DIR", err)
	}
	if admin.calls != 0 {
		t.Fatalf("%d streams or groups were asked for", admin.calls)
	}
}

// eventIndex is the position of the first event with prefix, or -1.
func eventIndex(events []string, prefix string) int {
	return slices.IndexFunc(events, func(e string) bool { return strings.HasPrefix(e, prefix) })
}

// lastEventIndex is the position of the last event with prefix, or -1.
func lastEventIndex(events []string, prefix string) int {
	for i := len(events) - 1; i >= 0; i-- {
		if strings.HasPrefix(events[i], prefix) {
			return i
		}
	}
	return -1
}

func TestRunDeletesADrainedUnprefixedKeyAfterCreating(t *testing.T) {
	admin := newFakeAdmin()
	existing(admin)
	admin.unprefixed = map[string]unprefixed.Key{streams.TaskDispatchV1: drained(streams.TaskDispatchV1)}
	var out bytes.Buffer
	if err := Run(context.Background(), admin, contractPlan(), slog.New(slog.NewJSONHandler(&out, nil))); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(admin.deleted, []string{streams.TaskDispatchV1}) {
		t.Fatalf("deleted = %v, want only %s", admin.deleted, streams.TaskDispatchV1)
	}
	for stream := range streams.Groups {
		if !slices.Contains(admin.events, "read "+stream) {
			t.Errorf("the unprefixed key of %s was not read", stream)
		}
	}
	if first, last := eventIndex(admin.events, "read "), max(lastEventIndex(admin.events, "group "), lastEventIndex(admin.events, "stream ")); first < last {
		t.Fatalf("an unprefixed key was read (event %d) before every stream and group was created (event %d)", first, last)
	}
	if !strings.Contains(out.String(), `"unprefixed key deleted"`) || !strings.Contains(out.String(), `"stream":"`+streams.TaskDispatchV1+`"`) {
		t.Fatalf("no line names the deleted key:\n%s", out.String())
	}
}

// existing marks every stream of the contract as created under the prefix, as
// a run of the release that added the prefix leaves Redis.
func existing(admin *fakeAdmin) {
	for stream, groups := range streams.Groups {
		if len(groups) == 0 {
			admin.streams[stream] = true
			continue
		}
		admin.groups[stream] = map[string]bool{}
		for _, g := range groups {
			admin.groups[stream][g] = true
		}
	}
}

// No consumer of this release reads an unprefixed key, so one that still holds
// entries a contract group has not finished stops the run, naming the key and
// the group. The run then deletes nothing: in a pre-upgrade hook the previous
// release still runs and reads the unprefixed keys, and a rejected upgrade
// leaves Redis as it found it.
func TestRunFailsWhileAnUnprefixedKeyHoldsPendingEntries(t *testing.T) {
	admin := newFakeAdmin()
	existing(admin)
	k := drained(streams.TaskDispatchV1)
	k.Groups[0].Pending = 2
	admin.unprefixed = map[string]unprefixed.Key{
		streams.TaskDispatchV1:    k,
		streams.ReleasePromotedV2: drained(streams.ReleasePromotedV2),
		streams.Retired[0]:        {},
	}
	var out bytes.Buffer
	err := Run(context.Background(), admin, contractPlan(), slog.New(slog.NewJSONHandler(&out, nil)))
	if err == nil {
		t.Fatal("Run succeeded while an unprefixed key still holds pending entries")
	}
	for _, want := range []string{streams.TaskDispatchV1, streams.ExecutionControllerTaskDispatch + ": 2 pending", "DEL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v\nwant it to contain %q", err, want)
		}
	}
	if i := eventIndex(admin.events, "delete "); i >= 0 {
		t.Fatalf("event %q: a run that fails on a held key deletes nothing", admin.events[i])
	}
	for _, stream := range []string{streams.TaskDispatchV1, streams.ReleasePromotedV2} {
		if !slices.Contains(admin.events, "read "+stream) {
			t.Errorf("the unprefixed key of %s was not read: every key is checked before the run fails", stream)
		}
	}
	if !strings.Contains(out.String(), `"unprefixed key kept"`) {
		t.Fatalf("no line names the kept key:\n%s", out.String())
	}
}

func TestRunFailsWhileAnUnprefixedKeyHoldsUndeliveredEntries(t *testing.T) {
	admin := newFakeAdmin()
	existing(admin)
	k := drained(streams.TaskDispatchV1)
	k.Groups[0].LastDelivered = trim.StreamID{Ms: 5}
	admin.unprefixed = map[string]unprefixed.Key{streams.TaskDispatchV1: k}
	err := Run(context.Background(), admin, contractPlan(), discardLogger())
	if err == nil || !strings.Contains(err.Error(), "delivered up to 5-0") {
		t.Fatalf("Run = %v, want an error naming what the group has not been delivered", err)
	}
	if len(admin.deleted) != 0 {
		t.Fatalf("deleted %v: a group has not been delivered the last entry", admin.deleted)
	}
}

// A Redis a release before the prefix last used holds the streams' unprefixed
// keys, with their groups, and none of their prefixed keys. This release never
// reads those keys, so the run stops before it creates anything.
func TestRunRefusesARedisLastUsedBeforeThePrefix(t *testing.T) {
	admin := newFakeAdmin()
	admin.unprefixed = map[string]unprefixed.Key{
		streams.TaskDispatchV1:    drained(streams.TaskDispatchV1),
		streams.ReleasePromotedV2: drained(streams.ReleasePromotedV2),
	}
	err := Run(context.Background(), admin, contractPlan(), discardLogger())
	if err == nil {
		t.Fatal("Run succeeded on a Redis that holds only the unprefixed keys")
	}
	for _, want := range []string{streams.TaskDispatchV1, streams.ReleasePromotedV2, "upgrade through the release that added the continuo: key prefix"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("err = %v\nwant it to contain %q", err, want)
		}
	}
	if admin.created != 0 || len(admin.deleted) != 0 || eventIndex(admin.events, "delete ") >= 0 {
		t.Fatalf("created %d, deleted %v: the run must change nothing", admin.created, admin.deleted)
	}
}

// The check needs a contract group on the unprefixed key: a stream new to this
// release has no key at all, and an unprefixed key that holds only a group the
// contract does not declare holds nothing this release would have read.
func TestRunCreatesAStreamWhoseUnprefixedKeyHoldsNoContractGroup(t *testing.T) {
	admin := newFakeAdmin()
	admin.unprefixed = map[string]unprefixed.Key{
		streams.TaskDispatchV1: {Groups: []unprefixed.Group{{Name: "renamed-group"}}},
	}
	if err := Run(context.Background(), admin, contractPlan(), discardLogger()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(admin.deleted, []string{streams.TaskDispatchV1}) {
		t.Fatalf("deleted = %v", admin.deleted)
	}
}

// A login whose rule predates the drain answers NOPERM on an unprefixed key; the
// check cannot tell what it holds and lets the run go on.
func TestRunGoesOnWhenItCannotReadAnUnprefixedKeyBeforeCreating(t *testing.T) {
	admin := newFakeAdmin()
	admin.readErrs = map[string]error{streams.TaskDispatchV1: errors.New("NOPERM No permissions to access a key")}
	if err := Run(context.Background(), admin, contractPlan(), discardLogger()); err != nil {
		t.Fatalf("Run = %v, want nil", err)
	}
	if admin.created == 0 {
		t.Fatal("nothing was created")
	}
}

func TestRunReturnsTheContextErrorWhenCancelled(t *testing.T) {
	admin := newFakeAdmin()
	existing(admin)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Run(ctx, admin, contractPlan(), discardLogger()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
}

func TestRunDeletesTheUnprefixedKeyOfEveryRetiredStream(t *testing.T) {
	admin := newFakeAdmin()
	admin.unprefixed = map[string]unprefixed.Key{streams.Retired[0]: {}}
	if err := Run(context.Background(), admin, contractPlan(), discardLogger()); err != nil {
		t.Fatal(err)
	}
	for _, retired := range streams.Retired {
		if !slices.Contains(admin.events, "delete "+retired) {
			t.Errorf("the unprefixed key of the retired stream %s was not deleted", retired)
		}
		if slices.Contains(admin.events, "read "+retired) {
			t.Errorf("the retired stream %s was read: nothing consumes it, so no group holds its key back", retired)
		}
	}
	if !slices.Equal(admin.deleted, []string{streams.Retired[0]}) {
		t.Fatalf("deleted = %v", admin.deleted)
	}
}

// A key that is not a Redis stream is not this release's to delete: the run
// logs it with its type, leaves it and goes on to the next key.
func TestRunLeavesAnUnprefixedKeyThatIsNotAStream(t *testing.T) {
	admin := newFakeAdmin()
	admin.otherTypes = map[string]string{streams.Retired[0]: "string"}
	admin.unprefixed = map[string]unprefixed.Key{streams.Retired[1]: {}}
	var out bytes.Buffer
	if err := Run(context.Background(), admin, contractPlan(), slog.New(slog.NewJSONHandler(&out, nil))); err != nil {
		t.Fatalf("Run = %v, want nil: a key that is not a stream does not fail the step", err)
	}
	if !slices.Equal(admin.deleted, []string{streams.Retired[1]}) {
		t.Fatalf("deleted = %v, want only %s", admin.deleted, streams.Retired[1])
	}
	for _, want := range []string{`"unprefixed key left, not a stream"`, `"key":"` + streams.Retired[0] + `"`, `"type":"string"`} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the log lacks %s:\n%s", want, out.String())
		}
	}
}

// Deleting the unprefixed keys only removes what no consumer reads, so a key
// that cannot be read or deleted is logged and left, and the run still succeeds.
func TestRunKeepsGoingWhenAnUnprefixedKeyCannotBeDeleted(t *testing.T) {
	admin := newFakeAdmin()
	existing(admin)
	noperm := errors.New("NOPERM User continuo_redis_migrate has no permissions to access the 'task.dispatch:v1' key")
	admin.readErrs = map[string]error{streams.TaskDispatchV1: noperm}
	admin.deleteErrs = map[string]error{streams.Retired[0]: noperm}
	admin.unprefixed = map[string]unprefixed.Key{
		streams.ReleasePromotedV2: drained(streams.ReleasePromotedV2),
		streams.Retired[1]:        {},
	}
	var out bytes.Buffer
	if err := Run(context.Background(), admin, contractPlan(), slog.New(slog.NewJSONHandler(&out, nil))); err != nil {
		t.Fatalf("Run = %v, want nil: an unprefixed key it cannot delete does not fail the step", err)
	}
	if !slices.Contains(admin.deleted, streams.ReleasePromotedV2) || !slices.Contains(admin.deleted, streams.Retired[1]) {
		t.Fatalf("deleted = %v, want the keys after the failing ones deleted too", admin.deleted)
	}
	for _, want := range []string{streams.TaskDispatchV1, streams.Retired[0], "NOPERM", "deploy/redis-acl-rules.acl"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the log lacks %s:\n%s", want, out.String())
		}
	}
}

func TestRunLeavesTheUnprefixedKeysWhenTold(t *testing.T) {
	admin := newFakeAdmin()
	existing(admin)
	admin.unprefixed = map[string]unprefixed.Key{streams.TaskDispatchV1: drained(streams.TaskDispatchV1)}
	plan := contractPlan()
	plan.DeleteUnprefixedKeys = false
	if err := Run(context.Background(), admin, plan, discardLogger()); err != nil {
		t.Fatal(err)
	}
	if i := eventIndex(admin.events, "read "); i >= 0 {
		t.Fatalf("event %q: the unprefixed keys are read with DeleteUnprefixedKeys off", admin.events[i])
	}
	if i := eventIndex(admin.events, "delete "); i >= 0 {
		t.Fatalf("event %q: an unprefixed key is deleted with DeleteUnprefixedKeys off", admin.events[i])
	}
}
