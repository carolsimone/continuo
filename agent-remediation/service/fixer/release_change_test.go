package fixer

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/agent-remediation/domain/proposal"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

func TestOwnChangeDiff_RendersPromotedToCandidate(t *testing.T) {
	svc := Services{Versions: &fakeVersions{v: ports.CurrentVersion{RawCode: "select id, amount from raw"}, ok: true}, Sanitizer: fakeSanitizer{}, Logger: testLogger()}

	got := ownChangeDiff(context.Background(), svc, "s.u", "select id, amount_eur from raw")

	assert.Contains(t, got, "-select id, amount from raw")
	assert.Contains(t, got, "+select id, amount_eur from raw")
}

func TestOwnChangeDiff_EmptyWithoutAPromotedVersionOrOnError(t *testing.T) {
	svc := Services{Versions: &fakeVersions{ok: false}, Sanitizer: fakeSanitizer{}, Logger: testLogger()}
	assert.Empty(t, ownChangeDiff(context.Background(), svc, "s.new", "select 1"))

	svc.Versions = &fakeVersions{err: errors.New("boom")}
	assert.Empty(t, ownChangeDiff(context.Background(), svc, "s.u", "select 1"))
}

func TestOwnChangeDiff_ComparesTwoCleanedTexts(t *testing.T) {
	stored := "select '[redacted:email]' as owner\nselect 1"
	svc := Services{Versions: &fakeVersions{v: ports.CurrentVersion{RawCode: stored}, ok: true}, Sanitizer: fakeSanitizer{}, Logger: testLogger()}

	got := ownChangeDiff(context.Background(), svc, "s.u", "select 'ops@acme.com' as owner\nselect 2")

	assert.Equal(t, proposal.ComputeUnifiedDiff(stored, "select '[redacted:email]' as owner\nselect 2", "s.u"), got, "catalog's redaction of the stored version never shows as a change")
	assert.NotContains(t, got, "ops@acme.com")
	assert.Empty(t, ownChangeDiff(context.Background(), svc, "s.u", "select 'ops@acme.com' as owner\nselect 1"), "a source that differs only where catalog redacted has no change")
}
