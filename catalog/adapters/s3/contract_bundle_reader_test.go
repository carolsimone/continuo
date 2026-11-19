package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/pkg/contractbundle"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
	pkgredis "github.com/carolsimone/continuo/pkg/redis"
)

// contractBundleRef is where the tests store the release's contract bundle.
var contractBundleRef = objectstore.Ref{Bucket: defaultBucket, Key: contractbundle.Key("default", "rel-1")}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// putContractBundle stores the gzipped raw document at contractBundleRef and
// returns the SHA-256 of the stored bytes.
func putContractBundle(t *testing.T, mem *objectstoretest.Memory, raw []byte) string {
	t.Helper()
	obj := gzipped(t, raw)
	require.NoError(t, mem.Put(context.Background(), contractBundleRef, obj, "application/gzip"))
	return sha256Hex(obj)
}

// contractBundleOf builds a schema-1 bundle document of release rel-1 holding
// the given entries, each a (key, entry JSON) pair.
func contractBundleOf(entries ...[2]string) []byte {
	var contracts bytes.Buffer
	for i, e := range entries {
		if i > 0 {
			contracts.WriteByte(',')
		}
		fmt.Fprintf(&contracts, "%q:%s", e[0], e[1])
	}
	return fmt.Appendf(nil,
		`{"schema_version":1,"tenant_id":"default","release_id":"rel-1","continuo_contracts":"0.2.0","contracts":{%s}}`,
		contracts.String())
}

// contractEntry is one contract of a bundle: a document with the given id and
// a contract hash.
func contractEntry(id, contractHash string) string {
	return fmt.Sprintf(
		`{"family":"python","service":"service-py","contract_hash":%q,"content_hash":"sha256:c-%s","source":{"path":"contracts/%s.odcs.yaml","sha256":"sha256:s"},"document":{"id":%q,"kind":"DataContract","schema":[{"name":"t","properties":[{"name":"n","maxLength":32}]}]}}`,
		contractHash, id, id, id)
}

func fetchContractBundle(t *testing.T, mem *objectstoretest.Memory, sha string) (ports.ContractBundle, error) {
	t.Helper()
	return NewContractBundleReader(mem).Fetch(context.Background(), contractBundleRef.String(), sha)
}

func TestContractBundleReader_ReadsTheFixture(t *testing.T) {
	raw, err := os.ReadFile("../../../pkg/contractbundle/testdata/contracts_v1.json")
	require.NoError(t, err)
	var fx struct {
		Canonical string `json:"canonical"`
		Input     struct {
			ReleaseID      string `json:"release_id"`
			LibraryVersion string `json:"library_version"`
			Contracts      []struct {
				UniqueID     string          `json:"unique_id"`
				Family       string          `json:"family"`
				Service      string          `json:"service"`
				ContractHash string          `json:"contract_hash"`
				ContentHash  string          `json:"content_hash"`
				SourcePath   string          `json:"source_path"`
				Document     json.RawMessage `json:"document"`
			} `json:"contracts"`
		} `json:"input"`
	}
	require.NoError(t, json.Unmarshal(raw, &fx))
	require.Len(t, fx.Input.Contracts, 2)

	mem := objectstoretest.NewMemory(defaultBucket)
	sha := putContractBundle(t, mem, []byte(fx.Canonical))

	bundle, err := fetchContractBundle(t, mem, sha)
	require.NoError(t, err)

	assert.Equal(t, fx.Input.ReleaseID, bundle.ReleaseID)
	assert.Equal(t, "default", bundle.TenantID, "the tenant the bundle was written for")
	assert.Equal(t, fx.Input.LibraryVersion, bundle.ContinuoContracts)
	require.Len(t, bundle.Contracts, 2)
	byID := map[string]contractversion.Incoming{}
	for _, c := range bundle.Contracts {
		byID[c.UniqueID] = c
	}
	for _, want := range fx.Input.Contracts {
		got, ok := byID[want.UniqueID]
		require.True(t, ok, "the bundle holds %s", want.UniqueID)
		assert.Equal(t, want.ContractHash, got.ContractHash, want.UniqueID)
		assert.Equal(t, want.ContentHash, got.ContentHash, want.UniqueID)
		assert.Equal(t, want.Family, got.Family, want.UniqueID)
		assert.Equal(t, want.Service, got.Service, want.UniqueID)
		assert.Equal(t, want.SourcePath, got.SourcePath, want.UniqueID)
		// A number keeps the exact text the contract hash was taken over.
		var wantDocument map[string]any
		dec := json.NewDecoder(bytes.NewReader(want.Document))
		dec.UseNumber()
		require.NoError(t, dec.Decode(&wantDocument))
		assert.Equal(t, wantDocument, got.Document, "%s: the document is the stored form, decoded", want.UniqueID)
		assert.Equal(t, want.UniqueID, got.Document["id"])
	}
	assert.Equal(t, "python", byID["analytics.py_daily_kpis"].Family)
	assert.Equal(t, "contracts/py_daily_kpis.odcs.yaml", byID["analytics.py_daily_kpis"].SourcePath)
	assert.Equal(t, "dbt", byID["analytics.governed"].Family)
	assert.Equal(t, "models/tagged/governed.sql", byID["analytics.governed"].SourcePath)
	assert.Equal(t, []objectstoretest.Read{{Ref: contractBundleRef, MaxBytes: contractbundle.MaxObjectBytes}}, mem.Reads(),
		"the bundle is read once, capped at the contract-bundle ceiling")
}

func TestContractBundleReader_ReadsTheBucketOfTheURI(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	ref := objectstore.Ref{Bucket: "bundles", Key: contractbundle.Key("default", "rel-1")}
	obj := gzipped(t, contractBundleOf([2]string{"a.t", contractEntry("a.t", "sha256:h1")}))
	require.NoError(t, mem.Put(context.Background(), ref, obj, "application/gzip"))

	bundle, err := NewContractBundleReader(mem).Fetch(context.Background(), ref.String(), sha256Hex(obj))
	require.NoError(t, err)
	assert.Len(t, bundle.Contracts, 1)
	assert.Equal(t, ref, mem.Reads()[0].Ref)
}

func TestContractBundleReader_ABundleOfNoContractsIsEmpty(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	sha := putContractBundle(t, mem, contractBundleOf())

	bundle, err := fetchContractBundle(t, mem, sha)
	require.NoError(t, err)
	assert.Equal(t, "rel-1", bundle.ReleaseID)
	assert.Empty(t, bundle.Contracts)
}

// The contracts come back in unique-id order whatever order the object lists
// them in, so what catalog writes for a promotion never depends on it.
func TestContractBundleReader_ContractsComeBackSortedByUniqueID(t *testing.T) {
	ids := []string{"analytics.m", "analytics.b", "billing.z", "analytics.a", "analytics.q", "core.c", "analytics.k"}
	entries := make([][2]string, len(ids))
	for i, id := range ids {
		entries[i] = [2]string{id, contractEntry(id, "sha256:h-"+id)}
	}
	want := []string{"analytics.a", "analytics.b", "analytics.k", "analytics.m", "analytics.q", "billing.z", "core.c"}

	for range 10 {
		mem := objectstoretest.NewMemory(defaultBucket)
		sha := putContractBundle(t, mem, contractBundleOf(entries...))
		bundle, err := fetchContractBundle(t, mem, sha)
		require.NoError(t, err)
		got := make([]string, len(bundle.Contracts))
		for i, c := range bundle.Contracts {
			got[i] = c.UniqueID
		}
		require.Equal(t, want, got)
	}
}

func TestContractBundleReader_AWrongChecksumIsInvalid(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	putContractBundle(t, mem, contractBundleOf([2]string{"a.t", contractEntry("a.t", "sha256:h1")}))

	_, err := fetchContractBundle(t, mem, sha256Hex([]byte("another object")))
	require.ErrorIs(t, err, ports.ErrContractBundleInvalid)
	assert.ErrorIs(t, err, contractbundle.ErrChecksumMismatch)
	assert.NotErrorIs(t, err, ports.ErrContractBundleNotFound)
	assert.Contains(t, err.Error(), contractBundleRef.String())
}

func TestContractBundleReader_AMissingObjectIsNotFound(t *testing.T) {
	_, err := fetchContractBundle(t, objectstoretest.NewMemory(defaultBucket), sha256Hex(nil))
	require.ErrorIs(t, err, ports.ErrContractBundleNotFound)
	assert.NotErrorIs(t, err, ports.ErrContractBundleInvalid)
	assert.Contains(t, err.Error(), contractBundleRef.String())
}

// A URI that names no object is a producer defect no retry repairs, and the
// reader reads nothing for it.
func TestContractBundleReader_AURIThatNamesNoObjectIsInvalidWithoutARead(t *testing.T) {
	for _, tc := range []struct{ name, uri string }{
		{"blank", "  "},
		{"bucket without a key", "s3://continuo"},
		{"bucket with an empty key", "s3://continuo/"},
		{"another scheme", "gs://continuo/tenants/default/topologies/rel-1/contracts.json.gz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mem := objectstoretest.NewMemory(defaultBucket)

			_, err := NewContractBundleReader(mem).Fetch(context.Background(), tc.uri, sha256Hex(nil))
			require.ErrorIs(t, err, ports.ErrContractBundleInvalid)
			assert.ErrorIs(t, err, objectstore.ErrInvalidRef)
			assert.NotErrorIs(t, err, ports.ErrContractBundleNotFound)
			assert.Empty(t, mem.Reads())
		})
	}
}

func TestContractBundleReader_AnObjectOverTheCeilingIsInvalid(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	mem.FailGet(contractBundleRef, fmt.Errorf("%w: %s is over the limit", objectstore.ErrTooLarge, contractBundleRef))

	_, err := fetchContractBundle(t, mem, sha256Hex(nil))
	require.ErrorIs(t, err, ports.ErrContractBundleInvalid)
	assert.ErrorIs(t, err, objectstore.ErrTooLarge)
	assert.NotErrorIs(t, err, ports.ErrContractBundleNotFound)
}

// A bundle that names no tenant cannot be matched to the promotion that
// carries it, so it is refused as invalid, without being read further.
func TestContractBundleReader_ABundleWithNoTenantIsInvalid(t *testing.T) {
	for name, tenant := range map[string]string{"empty": `""`, "blank": `"  "`} {
		t.Run(name, func(t *testing.T) {
			mem := objectstoretest.NewMemory(defaultBucket)
			sha := putContractBundle(t, mem, fmt.Appendf(nil,
				`{"schema_version":1,"tenant_id":%s,"release_id":"rel-1","continuo_contracts":"0.2.0","contracts":{}}`, tenant))

			_, err := fetchContractBundle(t, mem, sha)
			require.ErrorIs(t, err, ports.ErrContractBundleInvalid)
			assert.Contains(t, err.Error(), "tenant_id")
		})
	}
	t.Run("absent", func(t *testing.T) {
		mem := objectstoretest.NewMemory(defaultBucket)
		sha := putContractBundle(t, mem, []byte(`{"schema_version":1,"release_id":"rel-1","contracts":{}}`))

		_, err := fetchContractBundle(t, mem, sha)
		require.ErrorIs(t, err, ports.ErrContractBundleInvalid)
	})
}

func TestContractBundleReader_ADocumentOfAnotherVersionIsInvalid(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	sha := putContractBundle(t, mem, []byte(`{"schema_version":2,"release_id":"rel-1","contracts":{}}`))

	_, err := fetchContractBundle(t, mem, sha)
	require.ErrorIs(t, err, ports.ErrContractBundleInvalid)
	assert.ErrorIs(t, err, contractbundle.ErrUnsupportedVersion)
}

func TestContractBundleReader_AnUnreadableObjectIsInvalid(t *testing.T) {
	for name, obj := range map[string][]byte{
		"not gzip": []byte("plain text, not gzip"),
		"not json": gzipped(t, []byte("not json")),
		"a contract under an empty unique id": gzipped(t, contractBundleOf(
			[2]string{"", contractEntry("", "sha256:h1")})),
	} {
		t.Run(name, func(t *testing.T) {
			mem := objectstoretest.NewMemory(defaultBucket)
			require.NoError(t, mem.Put(context.Background(), contractBundleRef, obj, "application/gzip"))

			_, err := fetchContractBundle(t, mem, sha256Hex(obj))
			require.ErrorIs(t, err, ports.ErrContractBundleInvalid)
			assert.ErrorIs(t, err, contractbundle.ErrMalformed)
		})
	}
}

// An entry catalog could not version safely is refused as a whole: the bundle
// is keyed by unique id, and each contract's hash and document id must agree
// with its entry.
func TestContractBundleReader_ABadEntryIsInvalid(t *testing.T) {
	for name, entry := range map[string][2]string{
		"an empty contract hash":   {"a.t", contractEntry("a.t", "")},
		"a document of another id": {"a.t", contractEntry("a.other", "sha256:h1")},
		"a document with no id": {"a.t",
			`{"family":"python","service":"s","contract_hash":"sha256:h1","document":{"kind":"DataContract"}}`},
		"a document that is not an object": {"a.t",
			`{"family":"python","service":"s","contract_hash":"sha256:h1","document":[1,2]}`},
		"no document": {"a.t",
			`{"family":"python","service":"s","contract_hash":"sha256:h1"}`},
		"an id that is not a string": {"a.t",
			`{"family":"python","service":"s","contract_hash":"sha256:h1","document":{"id":7}}`},
	} {
		t.Run(name, func(t *testing.T) {
			mem := objectstoretest.NewMemory(defaultBucket)
			sha := putContractBundle(t, mem, contractBundleOf(
				[2]string{"a.good", contractEntry("a.good", "sha256:h0")}, entry))

			_, err := fetchContractBundle(t, mem, sha)
			require.ErrorIs(t, err, ports.ErrContractBundleInvalid)
			assert.Contains(t, err.Error(), "a.t", "the refusal names the entry")
			assert.NotErrorIs(t, err, ports.ErrContractBundleNotFound)
		})
	}
}

// A failure that is neither a missing object, an oversized one nor a bad
// document keeps its cause and wraps none of the port's sentinels, so an S3
// outage reaches the consumer's classifier as infrastructure.
func TestContractBundleReader_OtherFailuresKeepTheirCause(t *testing.T) {
	mem := objectstoretest.NewMemory(defaultBucket)
	mem.FailGet(contractBundleRef, statusError{code: http.StatusServiceUnavailable})

	_, err := fetchContractBundle(t, mem, sha256Hex(nil))
	require.Error(t, err)
	assert.NotErrorIs(t, err, ports.ErrContractBundleNotFound)
	assert.NotErrorIs(t, err, ports.ErrContractBundleInvalid)
	assert.Equal(t, pkgredis.ClassInfrastructure, pkgredis.Classify(err))
}

// A number beyond float64's exact range (2^53 + 1) reaches the domain as the
// text the bundle holds, so the stored version keeps the value its
// contract_hash was taken over.
func TestContractBundleReader_KeepsANumberAsItsText(t *testing.T) {
	entry := `{"family":"python","service":"service-py","contract_hash":"sha256:h","content_hash":"sha256:c","source":{"path":"contracts/t.odcs.yaml","sha256":"sha256:s"},"document":{"id":"analytics.t","limit":9007199254740993,"ratio":0.1}}`
	mem := objectstoretest.NewMemory(defaultBucket)
	sha := putContractBundle(t, mem, contractBundleOf([2]string{"analytics.t", entry}))

	bundle, err := fetchContractBundle(t, mem, sha)
	require.NoError(t, err)

	require.Len(t, bundle.Contracts, 1)
	assert.Equal(t, json.Number("9007199254740993"), bundle.Contracts[0].Document["limit"])
	assert.Equal(t, json.Number("0.1"), bundle.Contracts[0].Document["ratio"])
}
