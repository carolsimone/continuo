package deploycheck

import (
	"regexp"
	"testing"
)

// state refuses a rebase whose caller previewed a release that is no longer
// live with a fixed message, and the ui recognises that refusal by the message
// alone to answer 409 release_moved and preview again. The ui cannot import
// state, so it carries its own copy; this pins the copy to state's.
func TestRebaseReleaseMovedMessageMatchesBetweenStateAndUI(t *testing.T) {
	stateMsg := extractLiteral(t,
		readRepoFile(t, "state", "service", "runstart", "rebase.go"),
		regexp.MustCompile(`ErrLiveReleaseMoved\s*=\s*errors\.New\(\s*"([^"\\]*)"\s*\)`),
		"state/service/runstart/rebase.go", "ErrLiveReleaseMoved = errors.New(\"...\")")
	uiMsg := extractLiteral(t,
		readRepoFile(t, "ui", "src", "server", "routes", "runs.ts"),
		regexp.MustCompile(`export const RELEASE_MOVED_MESSAGE\s*=\s*'([^'\\]*)'`),
		"ui/src/server/routes/runs.ts", "export const RELEASE_MOVED_MESSAGE = '...'")
	if stateMsg != uiMsg {
		t.Errorf("ui's RELEASE_MOVED_MESSAGE %q differs from state's ErrLiveReleaseMoved %q", uiMsg, stateMsg)
	}
}

// extractLiteral returns the single string literal re captures in src, and
// fails when src does not declare it exactly once.
func extractLiteral(t *testing.T, src string, re *regexp.Regexp, file, want string) string {
	t.Helper()
	matches := re.FindAllStringSubmatch(src, -1)
	if len(matches) != 1 {
		t.Fatalf("%s declares %s %d times, want once", file, want, len(matches))
	}
	if matches[0][1] == "" {
		t.Fatalf("%s declares an empty %s", file, want)
	}
	return matches[0][1]
}
