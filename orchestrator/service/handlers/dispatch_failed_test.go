// White-box (in-package) test: the dispatchFailedReason mapper is
// unexported and exercised directly from package handlers.
package handlers

import (
	"errors"
	"fmt"
	"testing"

	"github.com/carolsimone/continuo/orchestrator/domain/snapshot"
	pkgEvents "github.com/carolsimone/continuo/pkg/events"
)

func TestDispatchFailedReason(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantReason pkgEvents.DispatchFailedReason
		wantOK     bool
	}{
		{
			name:       "ErrTargetNotFound maps to target_not_found",
			err:        snapshot.ErrTargetNotFound,
			wantReason: pkgEvents.DispatchFailedReasonTargetNotFound,
			wantOK:     true,
		},
		{
			name:       "ErrEmptyProjection maps to empty_projection",
			err:        snapshot.ErrEmptyProjection,
			wantReason: pkgEvents.DispatchFailedReasonEmptyProjection,
			wantOK:     true,
		},
		{
			name:       "wrapped ErrTargetNotFound still matches",
			err:        fmt.Errorf("snapshot failed: %w", snapshot.ErrTargetNotFound),
			wantReason: pkgEvents.DispatchFailedReasonTargetNotFound,
			wantOK:     true,
		},
		{
			name:       "wrapped ErrEmptyProjection still matches",
			err:        fmt.Errorf("snapshot failed: %w", snapshot.ErrEmptyProjection),
			wantReason: pkgEvents.DispatchFailedReasonEmptyProjection,
			wantOK:     true,
		},
		{
			name:       "ErrNoTests maps to no_tests",
			err:        snapshot.ErrNoTests,
			wantReason: pkgEvents.DispatchFailedReasonNoTests,
			wantOK:     true,
		},
		{
			name:       "wrapped ErrNoTests still matches",
			err:        fmt.Errorf("snapshot failed: %w", snapshot.ErrNoTests),
			wantReason: pkgEvents.DispatchFailedReasonNoTests,
			wantOK:     true,
		},
		{
			name:       "ErrRerunOfTestUnsupported maps to rerun_of_test_unsupported",
			err:        snapshot.ErrRerunOfTestUnsupported,
			wantReason: pkgEvents.DispatchFailedReasonRerunOfTestUnsupported,
			wantOK:     true,
		},
		{
			name:       "wrapped ErrRerunOfTestUnsupported still matches",
			err:        fmt.Errorf("snapshot failed: %w", snapshot.ErrRerunOfTestUnsupported),
			wantReason: pkgEvents.DispatchFailedReasonRerunOfTestUnsupported,
			wantOK:     true,
		},
		{
			name:       "unknown error returns false",
			err:        errors.New("boom"),
			wantReason: "",
			wantOK:     false,
		},
		{
			name:       "ErrPermanent-wrapped error returns false (consumer handles ACK+drop)",
			err:        fmt.Errorf("validation: %w", pkgEvents.ErrPermanent),
			wantReason: "",
			wantOK:     false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotReason, gotOK := dispatchFailedReason(tc.err)
			if gotReason != tc.wantReason || gotOK != tc.wantOK {
				t.Fatalf("dispatchFailedReason(%v) = (%q, %v), want (%q, %v)",
					tc.err, gotReason, gotOK, tc.wantReason, tc.wantOK)
			}
		})
	}
}
