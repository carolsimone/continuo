package deploycheck

import (
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The upgrade note tells an operator the reason run-controller records on each
// run it cancels at its first start; the reason it records is runsettle.Reason.
// pkg cannot import run-controller, so both texts are read from their sources:
// the quoted reason of NOTES.txt, wrapped over lines as it is, and the string
// constant.
func TestNotesQuoteTheReasonRunSettleRecords(t *testing.T) {
	notes := strings.Join(strings.Fields(readRepoFile(t, "deploy", "continuo", "templates", "NOTES.txt")), " ")
	quoted := regexp.MustCompile(`cancelled by "system", reason "([^"]+)"`).FindStringSubmatch(notes)
	require.NotNil(t, quoted, `NOTES.txt no longer quotes a cancellation reason as: cancelled by "system", reason "..."`)

	settle := readRepoFile(t, "run-controller", "service", "runsettle", "settle.go")
	literal := regexp.MustCompile(`(?m)^const Reason = (".*")$`).FindStringSubmatch(settle)
	require.NotNil(t, literal, "runsettle/settle.go no longer declares `const Reason = \"...\"` on one line")
	reason, err := strconv.Unquote(literal[1])
	require.NoError(t, err)

	assert.Equal(t, reason, quoted[1], "the reason NOTES.txt quotes is not the one runsettle.Reason records")
}
