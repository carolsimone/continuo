package contractbundle_test

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/pkg/contractbundle"
)

// bundleFixture is testdata/contracts_v1.json: what topology-controller's
// builder takes for one release (Input) and the exact JSON text it writes
// before gzip (Canonical). topology-controller's tests read a byte-identical
// copy, so the two writers are pinned to one document.
type bundleFixture struct {
	Input struct {
		TenantID       string `json:"tenant_id"`
		ReleaseID      string `json:"release_id"`
		LibraryVersion string `json:"library_version"`
		Contracts      []struct {
			UniqueID     string          `json:"unique_id"`
			Service      string          `json:"service"`
			ContractHash string          `json:"contract_hash"`
			ContentHash  string          `json:"content_hash"`
			SourcePath   string          `json:"source_path"`
			SourceSHA256 string          `json:"source_sha256"`
			Document     json.RawMessage `json:"document"`
		} `json:"contracts"`
		Provenance map[string]contractbundle.ServiceProvenance `json:"provenance"`
		Timings    json.RawMessage                             `json:"timings"`
	} `json:"input"`
	Canonical string `json:"canonical"`
}

func loadFixture(t *testing.T) bundleFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/contracts_v1.json")
	require.NoError(t, err)
	var fx bundleFixture
	require.NoError(t, json.Unmarshal(raw, &fx))
	require.NotEmpty(t, fx.Canonical)
	return fx
}

func sha(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// gz gzips raw with a zero modification time, the way the writer stores it.
func gz(t *testing.T, raw []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(raw)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func TestDecode_ReadsTheFixture(t *testing.T) {
	fx := loadFixture(t)
	obj := gz(t, []byte(fx.Canonical))

	doc, err := contractbundle.Decode(obj, sha(obj))
	require.NoError(t, err)

	assert.Equal(t, contractbundle.SchemaVersion, doc.SchemaVersion)
	assert.Equal(t, fx.Input.TenantID, doc.TenantID)
	assert.Equal(t, fx.Input.ReleaseID, doc.ReleaseID)
	assert.Equal(t, "edb41f33ec46e84780e99872ab2bd67f074959d2bf3e9c9fc54e61f8982b0d93", doc.ODCSSchemaSHA256)
	assert.Equal(t, fx.Input.LibraryVersion, doc.ContinuoContracts)
	assert.Equal(t, "null", string(doc.Ontology))
	assert.Equal(t, "[]", string(doc.Failures))
	assert.JSONEq(t, string(fx.Input.Timings), string(doc.Timings))

	assert.Equal(t, fx.Input.Provenance, doc.Provenance)
	assert.Equal(t, "0.13.0", doc.Provenance["service-py"].ContinuoPythonRuntime)
	assert.Equal(t, map[string]string{"pyarrow": "26.0.0"}, doc.Provenance["service-py"].ImagePackages)

	assert.Equal(t, contractbundle.Summary{Contracts: 1, ByFamily: map[string]int{"python": 1}}, doc.Summary)

	require.Len(t, fx.Input.Contracts, 1)
	want := fx.Input.Contracts[0]
	require.Len(t, doc.Contracts, 1)
	got, ok := doc.Contracts["analytics.py_daily_kpis"]
	require.True(t, ok, "the contracts are keyed by unique id")
	assert.Equal(t, "analytics.py_daily_kpis", want.UniqueID)
	assert.Equal(t, "python", got.Family)
	assert.Equal(t, want.Service, got.Service)
	assert.Equal(t, want.ContractHash, got.ContractHash)
	assert.Equal(t, want.ContentHash, got.ContentHash)
	assert.Equal(t, contractbundle.Source{Path: "contracts/py_daily_kpis.odcs.yaml", SHA256: want.SourceSHA256}, got.Source)
	assert.JSONEq(t, string(want.Document), string(got.Document))
}

func TestDecode_AcceptsAnUppercaseChecksum(t *testing.T) {
	obj := gz(t, []byte(loadFixture(t).Canonical))
	upper := bytes.ToUpper([]byte(sha(obj)))
	_, err := contractbundle.Decode(obj, string(upper))
	assert.NoError(t, err)
}

func TestDecode_RefusesAWrongChecksumBeforeUnzipping(t *testing.T) {
	valid := gz(t, []byte(loadFixture(t).Canonical))
	notGzip := []byte("plain text, not gzip")
	oversized := make([]byte, contractbundle.MaxObjectBytes+1)

	for name, obj := range map[string][]byte{
		"a valid object":      valid,
		"an object not gzip":  notGzip,
		"an oversized object": oversized,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := contractbundle.Decode(obj, sha([]byte("another object")))
			assert.ErrorIs(t, err, contractbundle.ErrChecksumMismatch)
			assert.NotErrorIs(t, err, contractbundle.ErrMalformed)
			assert.NotErrorIs(t, err, contractbundle.ErrTooLarge)
		})
	}
}

func TestDecode_RefusesAnotherSchemaVersion(t *testing.T) {
	for name, doc := range map[string]string{
		"version 2":     `{"schema_version":2,"tenant_id":"default","release_id":"r","contracts":{}}`,
		"version 0":     `{"schema_version":0,"tenant_id":"default","release_id":"r","contracts":{}}`,
		"no version":    `{"tenant_id":"default","release_id":"r","contracts":{}}`,
		"another shape": `{"schema_version":2,"release_id":"r","contracts":[{"id":"a.b"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			obj := gz(t, []byte(doc))
			_, err := contractbundle.Decode(obj, sha(obj))
			assert.ErrorIs(t, err, contractbundle.ErrUnsupportedVersion)
			assert.NotErrorIs(t, err, contractbundle.ErrMalformed)
		})
	}
}

func TestDecode_RefusesMalformedObjects(t *testing.T) {
	for name, obj := range map[string][]byte{
		"not gzip":         []byte("plain text, not gzip"),
		"not json":         gz(t, []byte("not json")),
		"not an object":    gz(t, []byte("[]")),
		"a string version": gz(t, []byte(`{"schema_version":"1","tenant_id":"default","release_id":"r","contracts":{}}`)),
		"empty release_id": gz(t, []byte(`{"schema_version":1,"tenant_id":"default","release_id":"","contracts":{}}`)),
		"contracts a list": gz(t, []byte(`{"schema_version":1,"tenant_id":"default","release_id":"r","contracts":[]}`)),
		"empty unique id": gz(t, []byte(
			`{"schema_version":1,"tenant_id":"default","release_id":"r","contracts":{"":{"family":"python"}}}`)),
	} {
		t.Run(name, func(t *testing.T) {
			_, err := contractbundle.Decode(obj, sha(obj))
			assert.ErrorIs(t, err, contractbundle.ErrMalformed)
		})
	}
}

func TestDecode_IgnoresKeysItDoesNotKnow(t *testing.T) {
	obj := gz(t, []byte(`{"schema_version":1,"tenant_id":"default","release_id":"r","later":{"x":1},"contracts":{"a.b":{"family":"python","later":true}}}`))
	doc, err := contractbundle.Decode(obj, sha(obj))
	require.NoError(t, err)
	assert.Equal(t, "python", doc.Contracts["a.b"].Family)
}

func TestKey(t *testing.T) {
	assert.Equal(t, "tenants/default/topologies/rel-4e5f6a7-212/contracts.json.gz", contractbundle.Key("default", "rel-4e5f6a7-212"))
	assert.Equal(t, "tenants/acme/topologies/r1/contracts.json.gz", contractbundle.Key("acme", "r1"))
}
