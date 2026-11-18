package handlers

import (
	"encoding/json"
	"testing"

	"github.com/carolsimone/continuo/pkg/codebundle"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	redactionAWSKeyID = "AKIAIOSFODNN7EXAMPLE" //nolint:gosec // G101: the example key id AWS documents, not a credential.
	redactionPassword = "s3cr3t-pa55"
	redactionEmail    = "ops@acme.com"
)

// A code version is stored redacted: its raw code (or a python node's
// projection), its compiled code and the shared code it reads carry no
// credential, key or email address into the graph. The hashes the producer
// computed over the original text pass through, so a version's identity does
// not move.
func TestVersionsPlan_RedactsSecretsInStoredCode(t *testing.T) {
	b := bundleFixture()
	n := b.Nodes["analytics.revenue"]
	n.RawCode = "select 1 -- password=" + redactionPassword + "\n-- owner " + redactionEmail
	n.CompiledCode = "select * from dblink('postgres://app:" + redactionPassword + "@db.internal/x')"
	b.Nodes["analytics.revenue"] = n

	b.Nodes["svc.model"] = codebundle.Node{
		Runtime:     "python",
		RawCode:     "unused because a projection exists " + redactionAWSKeyID,
		Projection:  `{"api_key": "` + redactionPassword + `"}`,
		ContentHash: "sha256:def",
		CodeUnitIDs: []string{"svc:m1"},
	}
	m1 := b.SharedCode["svc:m1"]
	m1.Source = "macro one -- key " + redactionAWSKeyID
	b.SharedCode["svc:m1"] = m1

	in := planVersionWrite(inputFixture(true, false), b, nodesFixture(), versionsTestLogger())

	byID := map[string]int{}
	for i, v := range in.Nodes {
		byID[v.UniqueID] = i
	}
	revenue := in.Nodes[byID["analytics.revenue"]]
	assert.Equal(t, "select 1 -- password=[redacted:secret]\n-- owner [redacted:email]", revenue.RawCode)
	assert.Equal(t, "select * from dblink('postgres://app:[redacted:secret]@db.internal/x')", revenue.CompiledCode)
	assert.Equal(t, "sha256:abc", revenue.ContentHash, "the producer's hashes are kept")

	python := in.Nodes[byID["svc.model"]]
	assert.Equal(t, `{"api_key": "[redacted:secret]"}`, python.RawCode, "the projection is what is stored, and it is redacted")

	require.NotEmpty(t, in.Units)
	for _, u := range in.Units {
		if u.UnitID == "svc:m1" {
			assert.Equal(t, "macro one -- key [redacted:secret]", u.Source)
		}
	}

	// Nothing a rule names survives anywhere in what is written.
	stored, err := json.Marshal(in)
	require.NoError(t, err)
	for _, secret := range []string{redactionPassword, redactionAWSKeyID, redactionEmail} {
		assert.NotContains(t, string(stored), secret)
	}
}

// Code with nothing to redact is stored as it is.
func TestVersionsPlan_StoresCodeWithNoSecretUntouched(t *testing.T) {
	in := planVersionWrite(inputFixture(true, false), bundleFixture(), nodesFixture(), versionsTestLogger())

	require.Len(t, in.Nodes, 1)
	assert.Equal(t, "select 1", in.Nodes[0].RawCode)
	assert.Equal(t, "select 1 compiled", in.Nodes[0].CompiledCode)
}
