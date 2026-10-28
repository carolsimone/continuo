package events_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/events"
)

type releaseRequestedFixture struct {
	Input   events.ReleaseRequested `json:"input"`
	Payload json.RawMessage         `json:"payload"`
}

var releaseRequestedFixtures = []string{
	"testdata/release_requested_v1.json",
	"testdata/release_requested_v1_empty.json",
}

// TestReleaseRequestedPayload_MatchesGoldenFixtures: the bytes release-controller
// writes into a release.requested:v1 entry equal the fixture's payload, which
// topology-controller's tests decode from an identical copy of the same file.
func TestReleaseRequestedPayload_MatchesGoldenFixtures(t *testing.T) {
	for _, path := range releaseRequestedFixtures {
		t.Run(path, func(t *testing.T) {
			raw, err := os.ReadFile(path) //nolint:gosec // G304: path is one of the fixed fixture paths above
			require.NoError(t, err)
			var fx releaseRequestedFixture
			require.NoError(t, json.Unmarshal(raw, &fx))

			got, err := events.ReleaseRequestedPayload(fx.Input)
			require.NoError(t, err)
			require.JSONEq(t, string(fx.Payload), string(got))
		})
	}
}
