package schedules_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/state/domain/schedule"
	"github.com/carolsimone/continuo/state/service/schedules"
)

func upsert(name, cronText, timezone string) schedules.UpsertInput {
	return schedules.UpsertInput{Name: name, Cron: cronText, Timezone: timezone}
}

// A name the live release has gets its first row at version 1, with its next
// fire counted from now and the two fires after it.
func TestSchedulesUpsert_CreatesTheFirstRowOfANameTheLiveReleaseHas(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"daily": 41})
	in := upsert("daily", " 0 1 * * * ", "Europe/Paris")
	in.Description = "Runs daily at 1am CET"

	got, next, err := h.svc.Upsert(context.Background(), tenantA, in, "alice@example.com")

	require.NoError(t, err)
	assert.Equal(t, tenantA, got.TenantID())
	assert.Equal(t, "daily", got.Name())
	assert.Equal(t, "0 1 * * *", got.Cron(), "the cron is stored as the parser reads it, without the spaces around it")
	assert.Equal(t, "Europe/Paris", got.Timezone())
	assert.Equal(t, "Runs daily at 1am CET", got.Description())
	assert.False(t, got.Paused())
	assert.Equal(t, int64(1), got.Version())
	assert.Equal(t, "alice@example.com", got.UpdatedBy())
	assert.True(t, now.Equal(got.UpdatedAt()))
	want := []time.Time{
		time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC),
	}
	assert.Equal(t, want, next, "01:00 in Paris is 00:00 UTC after the clocks went back")
	nextFire, ok := got.NextFireAt()
	require.True(t, ok)
	assert.True(t, want[0].Equal(nextFire), "the stored next fire is the first fire returned")

	row, ok := h.rows.row(tenantA, "daily")
	require.True(t, ok)
	assert.Equal(t, int64(1), row.version)
	assert.Equal(t, "0 1 * * *", row.cron)
	require.NotNil(t, row.nextFireAt)
	assert.True(t, want[0].Equal(*row.nextFireAt))
}

// The first row of a name no model of the tenant's live release carries is
// refused, as the schedule policy rules, and nothing is written.
func TestSchedulesUpsert_RefusesTheFirstRowOfANameNoModelHas(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"daily": 3})
	h.liveModels(tenantB, map[string]int{"weekly": 2})

	for _, name := range []string{"weekly", "dialy"} {
		_, _, err := h.svc.Upsert(context.Background(), tenantA, upsert(name, "0 1 * * *", "UTC"), "alice@example.com")

		require.ErrorIs(t, err, schedule.ErrUnknownName, name)
		assert.Contains(t, err.Error(), name)
		_, ok := h.rows.row(tenantA, name)
		assert.False(t, ok, "%s: nothing is written", name)
	}
	assert.Zero(t, h.rows.saves)
}

// A name that has a row can always be edited, paused, resumed and saved again,
// whatever its models in the live release: only a first row is checked.
func TestSchedulesUpsert_EditsARowWhoseNameHasNoModelLeft(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"weekly": 2})
	ctx := context.Background()
	_, _, err := h.svc.Upsert(ctx, tenantA, upsert("weekly", "0 3 * * 1", "Europe/Paris"), "alice@example.com")
	require.NoError(t, err)
	h.liveModels(tenantA, map[string]int{})

	pause := upsert("weekly", "0 3 * * 1", "Europe/Paris")
	pause.Paused, pause.ExpectedVersion = true, 1
	paused, next, err := h.svc.Upsert(ctx, tenantA, pause, "bob@example.com")
	require.NoError(t, err)
	assert.True(t, paused.Paused())
	assert.Equal(t, int64(2), paused.Version())
	assert.Equal(t, "bob@example.com", paused.UpdatedBy())
	assert.Empty(t, next, "a paused schedule fires nothing")
	_, ok := paused.NextFireAt()
	assert.False(t, ok)

	resume := upsert("weekly", "0 4 * * 1", "Europe/Paris")
	resume.ExpectedVersion = 2
	resumed, next, err := h.svc.Upsert(ctx, tenantA, resume, "bob@example.com")
	require.NoError(t, err)
	assert.False(t, resumed.Paused())
	assert.Equal(t, int64(3), resumed.Version())
	assert.Equal(t, h.fires(t, "0 4 * * 1", "Europe/Paris", now, 3), next, "a resume counts from now")
}

// An expected version the row does not have is refused before anything is
// written, so an edit made since the operator read the schedule is kept.
func TestSchedulesUpsert_AStaleExpectedVersionIsRefused(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"daily": 1})
	ctx := context.Background()
	_, _, err := h.svc.Upsert(ctx, tenantA, upsert("daily", "0 1 * * *", "UTC"), "alice@example.com")
	require.NoError(t, err)
	edit := upsert("daily", "0 2 * * *", "UTC")
	edit.ExpectedVersion = 1
	_, _, err = h.svc.Upsert(ctx, tenantA, edit, "bob@example.com")
	require.NoError(t, err)
	saves := h.rows.saves

	for _, stale := range []int64{1, 3} {
		edit := upsert("daily", "0 5 * * *", "UTC")
		edit.ExpectedVersion = stale
		_, _, err := h.svc.Upsert(ctx, tenantA, edit, "carol@example.com")
		require.ErrorIs(t, err, schedule.ErrStaleVersion, "expected version %d", stale)
	}

	assert.Equal(t, saves, h.rows.saves, "nothing is written")
	row, _ := h.rows.row(tenantA, "daily")
	assert.Equal(t, "0 2 * * *", row.cron)
	assert.Equal(t, int64(2), row.version)
	assert.Equal(t, "bob@example.com", row.updatedBy)
}

// The expected version is compared with the row the edit read, not only with
// the row the save meets: a row that moved on to the expected version after it
// was read would otherwise take an edit built on the older row, at a lower
// version than it has.
func TestSchedulesUpsert_ComparesTheExpectedVersionWithTheRowItRead(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"daily": 1})
	h.rows.put(tenantA, "daily", storedRow{cron: "0 1 * * *", timezone: "UTC", version: 1, updatedBy: "alice@example.com", updatedAt: now})
	h.rows.afterGet = func() {
		h.rows.put(tenantA, "daily", storedRow{cron: "0 9 * * *", timezone: "UTC", version: 4, updatedBy: "dave@example.com", updatedAt: now})
	}
	edit := upsert("daily", "0 2 * * *", "UTC")
	edit.ExpectedVersion = 4

	_, _, err := h.svc.Upsert(context.Background(), tenantA, edit, "bob@example.com")

	require.ErrorIs(t, err, schedule.ErrStaleVersion)
	assert.Zero(t, h.rows.saves)
	row, _ := h.rows.row(tenantA, "daily")
	assert.Equal(t, int64(4), row.version)
}

// An expected version of a row that is gone is stale: the schedule was removed
// since the operator read it, so the edit does not bring it back.
func TestSchedulesUpsert_AnExpectedVersionOfARowThatIsGoneIsStale(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"daily": 1})
	edit := upsert("daily", "0 2 * * *", "UTC")
	edit.ExpectedVersion = 3

	_, _, err := h.svc.Upsert(context.Background(), tenantA, edit, "bob@example.com")

	require.ErrorIs(t, err, schedule.ErrStaleVersion)
	_, ok := h.rows.row(tenantA, "daily")
	assert.False(t, ok)
}

// An edit with no expected version whose save finds that another edit saved
// after its read reads the row again and saves over it: the last write wins,
// at the version after the other edit's, never at the same one.
func TestSchedulesUpsert_WithNoExpectedVersionSavesAgainOverAnEditThatSavedFirst(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"daily": 1})
	h.rows.put(tenantA, "daily", storedRow{cron: "0 1 * * *", timezone: "UTC", version: 1, updatedBy: "alice@example.com", updatedAt: now})
	overtaken := false
	h.rows.afterGet = func() {
		if !overtaken {
			overtaken = true
			h.rows.put(tenantA, "daily", storedRow{cron: "0 9 * * *", timezone: "UTC", version: 2, updatedBy: "dave@example.com", updatedAt: now})
		}
	}

	got, next, err := h.svc.Upsert(context.Background(), tenantA, upsert("daily", "0 2 * * *", "UTC"), "bob@example.com")

	require.NoError(t, err)
	assert.Equal(t, int64(3), got.Version())
	assert.Len(t, next, 3)
	row, _ := h.rows.row(tenantA, "daily")
	assert.Equal(t, int64(3), row.version)
	assert.Equal(t, "0 2 * * *", row.cron)
	assert.Equal(t, "bob@example.com", row.updatedBy)
	assert.Equal(t, 2, h.rows.gets, "the edit read the row again")
	assert.Equal(t, 2, h.rows.saves)
}

// A first cron whose save finds that another edit created the row since its
// read is saved over that row at version 2, not written again at version 1.
func TestSchedulesUpsert_ACreateThatAnotherCreateOvertookSavesOverIt(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"daily": 1})
	overtaken := false
	h.rows.afterGet = func() {
		if !overtaken {
			overtaken = true
			h.rows.put(tenantA, "daily", storedRow{cron: "0 9 * * *", timezone: "UTC", version: 1, updatedBy: "dave@example.com", updatedAt: now})
		}
	}

	got, _, err := h.svc.Upsert(context.Background(), tenantA, upsert("daily", "0 2 * * *", "UTC"), "bob@example.com")

	require.NoError(t, err)
	assert.Equal(t, int64(2), got.Version())
	row, _ := h.rows.row(tenantA, "daily")
	assert.Equal(t, int64(2), row.version)
	assert.Equal(t, "0 2 * * *", row.cron)
	assert.Equal(t, "bob@example.com", row.updatedBy)
}

// While other edits keep saving between its read and its save, an edit with no
// expected version reads and saves three times, then is refused as stale, and
// the row keeps the other edits' last save.
func TestSchedulesUpsert_WithNoExpectedVersionGivesUpAfterThreeTries(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"daily": 1})
	h.rows.put(tenantA, "daily", storedRow{cron: "0 1 * * *", timezone: "UTC", version: 1, updatedBy: "alice@example.com", updatedAt: now})
	h.rows.afterGet = func() {
		row, _ := h.rows.row(tenantA, "daily")
		row.version++
		row.updatedBy = "dave@example.com"
		h.rows.put(tenantA, "daily", row)
	}

	_, _, err := h.svc.Upsert(context.Background(), tenantA, upsert("daily", "0 2 * * *", "UTC"), "bob@example.com")

	require.ErrorIs(t, err, schedule.ErrStaleVersion)
	assert.Equal(t, 3, h.rows.gets)
	assert.Equal(t, 3, h.rows.saves)
	row, _ := h.rows.row(tenantA, "daily")
	assert.Equal(t, int64(4), row.version)
	assert.Equal(t, "0 1 * * *", row.cron)
	assert.Equal(t, "dave@example.com", row.updatedBy)
}

// An edit that names its expected version is not tried again: a save that
// finds the row moved on since the read is refused as stale at once.
func TestSchedulesUpsert_WithAnExpectedVersionAStaleSaveIsNotTriedAgain(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"daily": 1})
	h.rows.put(tenantA, "daily", storedRow{cron: "0 1 * * *", timezone: "UTC", version: 1, updatedBy: "alice@example.com", updatedAt: now})
	h.rows.afterGet = func() {
		h.rows.put(tenantA, "daily", storedRow{cron: "0 9 * * *", timezone: "UTC", version: 2, updatedBy: "dave@example.com", updatedAt: now})
	}
	edit := upsert("daily", "0 2 * * *", "UTC")
	edit.ExpectedVersion = 1

	_, _, err := h.svc.Upsert(context.Background(), tenantA, edit, "bob@example.com")

	require.ErrorIs(t, err, schedule.ErrStaleVersion)
	assert.Equal(t, 1, h.rows.gets)
	assert.Equal(t, 1, h.rows.saves)
	row, _ := h.rows.row(tenantA, "daily")
	assert.Equal(t, int64(2), row.version)
	assert.Equal(t, "dave@example.com", row.updatedBy)
}

// A row whose stored cron no longer parses is a row: an edit saves over it,
// with no expected version or with the version it is at, whatever its name's
// models, and its version still counts up from the stored one.
func TestSchedulesUpsert_RepairsARowWhoseStoredCronNoLongerParses(t *testing.T) {
	ctx := context.Background()
	for name, expected := range map[string]int64{"without a version": 0, "with its version": 5} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t)
			h.rows.put(tenantA, "daily", storedRow{cron: "0 1 * * *", timezone: "Mars/Olympus", version: 5, updatedBy: "alice@example.com", updatedAt: now})
			edit := upsert("daily", "0 1 * * *", "Europe/Paris")
			edit.ExpectedVersion = expected

			got, next, err := h.svc.Upsert(ctx, tenantA, edit, "bob@example.com")

			require.NoError(t, err)
			assert.Equal(t, int64(6), got.Version())
			assert.Len(t, next, 3)
			row, _ := h.rows.row(tenantA, "daily")
			assert.Equal(t, "Europe/Paris", row.timezone)
			assert.Equal(t, int64(6), row.version)
			repaired, ok, err := h.rows.Get(ctx, tenantA, "daily")
			require.NoError(t, err, "the repaired row parses")
			require.True(t, ok)
			assert.Equal(t, "bob@example.com", repaired.UpdatedBy())
		})
	}

	h := newHarness(t)
	h.rows.put(tenantA, "daily", storedRow{cron: "0 1 * * *", timezone: "Mars/Olympus", version: 5, updatedBy: "alice@example.com", updatedAt: now})
	edit := upsert("daily", "0 1 * * *", "Europe/Paris")
	edit.ExpectedVersion = 4
	_, _, err := h.svc.Upsert(ctx, tenantA, edit, "bob@example.com")
	require.ErrorIs(t, err, schedule.ErrStaleVersion, "a stale version over such a row is still stale")
	assert.Zero(t, h.rows.saves)
}

// An input the service cannot honour is ErrInvalid, its message the reason fit
// to show the operator, and nothing is read or written. A refusal of the
// parser carries the parser's own message.
func TestSchedulesUpsert_RefusesAnInvalidInput(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"daily": 1})
	parserRefusals := map[string]schedules.UpsertInput{
		"empty cron":         upsert("daily", "", "UTC"),
		"CRON_TZ= prefix":    upsert("daily", "CRON_TZ=UTC 0 1 * * *", "UTC"),
		"TZ= prefix":         upsert("daily", "TZ=UTC 0 1 * * *", "UTC"),
		"six fields":         upsert("daily", "0 0 1 * * *", "UTC"),
		"unknown zone":       upsert("daily", "0 1 * * *", "Mars/Olympus"),
		"empty zone":         upsert("daily", "0 1 * * *", ""),
		"interval too short": upsert("daily", "@every 30s", "UTC"),
		"never fires":        upsert("daily", "0 0 30 2 *", "UTC"),
	}
	for name, in := range parserRefusals {
		_, _, err := h.svc.Upsert(context.Background(), tenantA, in, "alice@example.com")

		require.ErrorIs(t, err, schedules.ErrInvalid, name)
		assert.Equal(t, h.parseError(t, in.Cron, in.Timezone), err.Error(), "%s: the parser's message", name)
	}

	noName := upsert("", "0 1 * * *", "UTC")
	_, _, err := h.svc.Upsert(context.Background(), tenantA, noName, "alice@example.com")
	require.ErrorIs(t, err, schedules.ErrInvalid)
	assert.Equal(t, "schedule_name is required", err.Error())

	negative := upsert("daily", "0 1 * * *", "UTC")
	negative.ExpectedVersion = -1
	_, _, err = h.svc.Upsert(context.Background(), tenantA, negative, "alice@example.com")
	require.ErrorIs(t, err, schedules.ErrInvalid)
	assert.Equal(t, "expected_version must not be negative", err.Error())

	assert.Zero(t, h.rows.saves)
	assert.Empty(t, h.rows.rows)
}

// A schedule name may be up to 50 characters long, the longest a run of it can
// store, counted in characters rather than bytes: a longer name is ErrInvalid,
// with a message that names the limit, and nothing is read or written.
func TestSchedulesUpsert_RefusesANameLongerThanFiftyCharacters(t *testing.T) {
	fifty, fiftyOne := strings.Repeat("a", 50), strings.Repeat("a", 51)
	accented, accentedTooLong := strings.Repeat("é", 50), strings.Repeat("é", 51)
	require.Len(t, accented, 100, "each é is two bytes")

	for _, name := range []string{fifty, accented} {
		h := newHarness(t)
		h.liveModels(tenantA, map[string]int{name: 1})
		got, _, err := h.svc.Upsert(context.Background(), tenantA, upsert(name, "0 1 * * *", "UTC"), "alice@example.com")
		require.NoError(t, err, "a name of 50 characters (%d bytes) is saved", len(name))
		assert.Equal(t, name, got.Name())
	}

	for _, name := range []string{fiftyOne, accentedTooLong} {
		h := newHarness(t)
		h.liveModels(tenantA, map[string]int{name: 1})
		_, _, err := h.svc.Upsert(context.Background(), tenantA, upsert(name, "0 1 * * *", "UTC"), "alice@example.com")
		require.ErrorIs(t, err, schedules.ErrInvalid, "a name of 51 characters (%d bytes) is refused", len(name))
		assert.Equal(t, "schedule_name must be at most 50 characters long, not 51", err.Error())
		assert.Zero(t, h.rows.gets, "nothing is read")
		assert.Zero(t, h.rows.saves, "nothing is written")
	}
}

// A failing read of the row or of the live release fails the edit, as neither
// stale nor unknown, and writes nothing.
func TestSchedulesUpsert_AFailingReadFailsTheEdit(t *testing.T) {
	h := newHarness(t)
	h.models.Err = errBoom
	_, _, err := h.svc.Upsert(context.Background(), tenantA, upsert("daily", "0 1 * * *", "UTC"), "alice@example.com")
	require.ErrorIs(t, err, errBoom)
	assert.NotErrorIs(t, err, schedule.ErrUnknownName)

	h = newHarness(t)
	h.rows.failWith = errBoom
	_, _, err = h.svc.Upsert(context.Background(), tenantA, upsert("daily", "0 1 * * *", "UTC"), "alice@example.com")
	require.ErrorIs(t, err, errBoom)
	assert.NotErrorIs(t, err, schedule.ErrStaleVersion)
}

// A tenant edits only its own schedules: another tenant's models do not let
// it create a name, and another tenant's row of the same name is untouched.
func TestSchedulesUpsert_IsOfTheTenant(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantB, map[string]int{"daily": 1})
	ctx := context.Background()

	_, _, err := h.svc.Upsert(ctx, tenantA, upsert("daily", "0 1 * * *", "UTC"), "alice@example.com")
	require.ErrorIs(t, err, schedule.ErrUnknownName)

	_, _, err = h.svc.Upsert(ctx, tenantB, upsert("daily", "0 1 * * *", "UTC"), "bob@example.com")
	require.NoError(t, err)
	h.liveModels(tenantA, map[string]int{"daily": 1})
	_, _, err = h.svc.Upsert(ctx, tenantA, upsert("daily", "0 7 * * *", "UTC"), "alice@example.com")
	require.NoError(t, err)

	theirs, _ := h.rows.row(tenantB, "daily")
	assert.Equal(t, "0 1 * * *", theirs.cron)
	assert.Equal(t, int64(1), theirs.version)
	mine, _ := h.rows.row(tenantA, "daily")
	assert.Equal(t, "0 7 * * *", mine.cron)
}

// Delete removes the row, whatever its stored cron, and succeeds when there is
// none: the schedule then runs only when triggered.
func TestSchedulesDelete_RemovesTheRowAndSucceedsWhenThereIsNone(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.rows.put(tenantA, "daily", storedRow{cron: "0 1 * * *", timezone: "UTC", version: 2})
	h.rows.put(tenantA, "broken", storedRow{cron: "0 1 * * *", timezone: "Mars/Olympus", version: 1})
	h.rows.put(tenantB, "daily", storedRow{cron: "0 1 * * *", timezone: "UTC", version: 1})

	require.NoError(t, h.svc.Delete(ctx, tenantA, "daily"))
	require.NoError(t, h.svc.Delete(ctx, tenantA, "broken"))
	require.NoError(t, h.svc.Delete(ctx, tenantA, "daily"), "deleting a schedule that has no cron succeeds")
	require.NoError(t, h.svc.Delete(ctx, tenantA, "never-had-one"))

	_, ok := h.rows.row(tenantA, "daily")
	assert.False(t, ok)
	_, ok = h.rows.row(tenantA, "broken")
	assert.False(t, ok)
	_, ok = h.rows.row(tenantB, "daily")
	assert.True(t, ok, "another tenant's row of the same name stays")
}

func TestSchedulesDelete_RefusesAnEmptyNameAndFailsWithItsRead(t *testing.T) {
	h := newHarness(t)
	err := h.svc.Delete(context.Background(), tenantA, "")
	require.ErrorIs(t, err, schedules.ErrInvalid)
	assert.Equal(t, "schedule_name is required", err.Error())

	h.rows.failWith = errBoom
	require.ErrorIs(t, h.svc.Delete(context.Background(), tenantA, "daily"), errBoom)
}

// Preview reads a cron as Upsert does and writes nothing: the cron in words
// and its next three fires from now.
func TestSchedulesPreview_ReturnsTheWordsAndTheNextThreeFires(t *testing.T) {
	h := newHarness(t)
	wantWords, err := h.parser.Describe("0 1 * * *")
	require.NoError(t, err)

	words, next, err := h.svc.Preview(context.Background(), "0 1 * * *", "Europe/Paris")

	require.NoError(t, err)
	assert.Equal(t, wantWords, words)
	assert.NotEmpty(t, words)
	assert.Equal(t, []time.Time{
		time.Date(2026, 10, 29, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 30, 0, 0, 0, 0, time.UTC),
		time.Date(2026, 10, 31, 0, 0, 0, 0, time.UTC),
	}, next)
	assert.Zero(t, h.rows.saves)
}

// A cron the parser fires but the words library cannot read previews with no
// words rather than failing.
func TestSchedulesPreview_ACronWithNoWordsStillPreviews(t *testing.T) {
	h := newHarness(t)
	_, err := h.parser.Describe("0 9 * * */9")
	require.Error(t, err, "the words library cannot read a day-of-week step of */9")

	words, next, err := h.svc.Preview(context.Background(), "0 9 * * */9", "UTC")

	require.NoError(t, err)
	assert.Empty(t, words)
	assert.Equal(t, h.fires(t, "0 9 * * */9", "UTC", now, 3), next)
}

func TestSchedulesPreview_RefusesWhatTheParserRefusesWithItsMessage(t *testing.T) {
	h := newHarness(t)
	for _, c := range []struct{ cron, timezone string }{
		{"CRON_TZ=Europe/Paris 0 1 * * *", "Europe/Paris"},
		{"61 * * * *", "UTC"},
		{"0 1 * * *", "Mars/Olympus"},
		{"@every 10s", "UTC"},
	} {
		_, _, err := h.svc.Preview(context.Background(), c.cron, c.timezone)

		require.ErrorIs(t, err, schedules.ErrInvalid, c.cron)
		assert.Equal(t, h.parseError(t, c.cron, c.timezone), err.Error())
	}
}

// List is one entry per name the live release has or that has a row, by name:
// design §9.3's four rows. Each carries its cron, in words, its models and its
// last run.
func TestSchedulesList_IsTheUnionOfTheLiveReleaseAndTheRows(t *testing.T) {
	h := newHarness(t)
	ctx := context.Background()
	h.liveModels(tenantA, map[string]int{"daily": 41, "hourly": 6, "e2e-schedule": 5, "weekly": 1})
	daily := upsert("daily", "0 1 * * *", "Europe/Paris")
	_, _, err := h.svc.Upsert(ctx, tenantA, daily, "alice@example.com")
	require.NoError(t, err)
	hourly := upsert("hourly", "0 * * * *", "Europe/Paris")
	hourly.Paused = true
	_, _, err = h.svc.Upsert(ctx, tenantA, hourly, "bob@example.com")
	require.NoError(t, err)
	_, _, err = h.svc.Upsert(ctx, tenantA, upsert("weekly", "0 3 * * 1", "Europe/Paris"), "alice@example.com")
	require.NoError(t, err)
	h.liveModels(tenantA, map[string]int{"daily": 41, "hourly": 6, "e2e-schedule": 5})
	last := activeRun("daily")
	h.lastRuns.put(tenantA, last)
	h.lastRuns.put(tenantA, activeRun("single-node-run"))

	got, err := h.svc.List(ctx, tenantA)

	require.NoError(t, err)
	require.Len(t, got, 4, "a last run of a name with neither models nor a row is not a schedule")
	assert.Equal(t, []string{"daily", "e2e-schedule", "hourly", "weekly"},
		[]string{got[0].Name, got[1].Name, got[2].Name, got[3].Name})

	scheduled := got[0]
	require.NotNil(t, scheduled.Schedule)
	assert.Equal(t, "0 1 * * *", scheduled.Schedule.Cron())
	assert.Equal(t, 41, scheduled.ModelCount)
	wantWords, err := h.parser.Describe("0 1 * * *")
	require.NoError(t, err)
	assert.Equal(t, wantWords, scheduled.Words)
	require.NotNil(t, scheduled.LastRun)
	assert.Equal(t, last.RunID, scheduled.LastRun.RunID)
	assert.True(t, scheduled.HasActiveRun())

	manual := got[1]
	assert.Nil(t, manual.Schedule, "a name with no row runs only when triggered")
	assert.Empty(t, manual.Words)
	assert.Equal(t, 5, manual.ModelCount)
	assert.Nil(t, manual.LastRun)
	assert.False(t, manual.HasActiveRun())

	paused := got[2]
	require.NotNil(t, paused.Schedule)
	assert.True(t, paused.Schedule.Paused())
	assert.Equal(t, "bob@example.com", paused.Schedule.UpdatedBy())
	assert.Equal(t, 6, paused.ModelCount)

	flagged := got[3]
	require.NotNil(t, flagged.Schedule)
	assert.Equal(t, "0 3 * * 1", flagged.Schedule.Cron())
	assert.Zero(t, flagged.ModelCount, "a scheduled name no model of the live release has is listed with no models")
}

// A row whose stored cron no longer parses, of a name the live release has,
// is listed once, with no cron and the name's models; Upsert can still repair
// it.
func TestSchedulesList_ARowWhoseStoredCronNoLongerParsesOfALiveNameIsListedOnce(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"daily": 2, "weekly": 1})
	h.rows.put(tenantA, "daily", storedRow{cron: "0 1 * * *", timezone: "Mars/Olympus", version: 3})
	h.rows.put(tenantA, "weekly", storedRow{cron: "0 3 * * 1", timezone: "UTC", version: 1})

	got, err := h.svc.List(context.Background(), tenantA)

	require.NoError(t, err)
	require.Len(t, got, 2, "the bad row's name is listed once, not once for the live release and once for the row")
	assert.Equal(t, "daily", got[0].Name)
	assert.Nil(t, got[0].Schedule)
	assert.Empty(t, got[0].Words)
	assert.Equal(t, 2, got[0].ModelCount)
	assert.Equal(t, "weekly", got[1].Name)
	require.NotNil(t, got[1].Schedule)
}

// A row whose stored cron no longer parses, of a name no model of the live
// release has, is listed with no cron, no models and its last run, so the
// operator can see it and fix or remove it.
func TestSchedulesList_ARowWhoseStoredCronNoLongerParsesOfANameWithNoModelsShowsNoCron(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"daily": 2})
	h.rows.put(tenantA, "gone", storedRow{cron: "not a cron", timezone: "UTC", version: 2})
	h.rows.put(tenantB, "theirs", storedRow{cron: "not a cron", timezone: "UTC", version: 1})
	last := activeRun("gone")
	h.lastRuns.put(tenantA, last)

	got, err := h.svc.List(context.Background(), tenantA)

	require.NoError(t, err)
	require.Len(t, got, 2, "another tenant's bad row is not listed")
	assert.Equal(t, "daily", got[0].Name)
	gone := got[1]
	assert.Equal(t, "gone", gone.Name)
	assert.Nil(t, gone.Schedule)
	assert.Empty(t, gone.Words)
	assert.Zero(t, gone.ModelCount)
	require.NotNil(t, gone.LastRun)
	assert.Equal(t, last.RunID, gone.LastRun.RunID)
	assert.True(t, gone.HasActiveRun())
}

// A cron the words library cannot read is listed with its cron and no words.
func TestSchedulesList_ACronWithNoWordsIsListedWithoutWords(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantA, map[string]int{"daily": 1})
	_, _, err := h.svc.Upsert(context.Background(), tenantA, upsert("daily", "0 9 * * */9", "UTC"), "alice@example.com")
	require.NoError(t, err)

	got, err := h.svc.List(context.Background(), tenantA)

	require.NoError(t, err)
	require.Len(t, got, 1)
	require.NotNil(t, got[0].Schedule)
	assert.Equal(t, "0 9 * * */9", got[0].Schedule.Cron())
	assert.Empty(t, got[0].Words)
}

// A tenant lists only its own live release, rows and runs.
func TestSchedulesList_IsOfTheTenant(t *testing.T) {
	h := newHarness(t)
	h.liveModels(tenantB, map[string]int{"theirs": 1})
	_, _, err := h.svc.Upsert(context.Background(), tenantB, upsert("theirs", "0 1 * * *", "UTC"), "bob@example.com")
	require.NoError(t, err)
	h.lastRuns.put(tenantB, activeRun("theirs"))

	mine, err := h.svc.List(context.Background(), tenantA)
	require.NoError(t, err)
	assert.Empty(t, mine)

	theirs, err := h.svc.List(context.Background(), tenantB)
	require.NoError(t, err)
	require.Len(t, theirs, 1)
	assert.True(t, theirs[0].HasActiveRun())
}

func TestSchedulesList_AFailingReadFailsTheList(t *testing.T) {
	for name, fail := range map[string]func(h *harness){
		"live release": func(h *harness) { h.models.Err = errBoom },
		"rows":         func(h *harness) { h.rows.failWith = errBoom },
		"bad rows":     func(h *harness) { h.rows.failInvalidWith = errBoom },
		"last runs":    func(h *harness) { h.lastRuns.failWith = errBoom },
	} {
		h := newHarness(t)
		fail(h)
		_, err := h.svc.List(context.Background(), tenantA)
		require.ErrorIs(t, err, errBoom, name)
	}
}
