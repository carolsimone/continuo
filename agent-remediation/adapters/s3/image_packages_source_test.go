package s3

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
	"github.com/carolsimone/continuo/pkg/objectstore/objectstoretest"
)

// fakeBundles answers where a release's contract bundle is.
type fakeBundles struct {
	uri, sha string
	err      error
	asked    []string
}

func (f *fakeBundles) ContractBundle(_ context.Context, releaseID string) (string, string, error) {
	f.asked = append(f.asked, releaseID)
	return f.uri, f.sha, f.err
}

var _ ports.ContractBundleReader = (*fakeBundles)(nil)

// contractBundleRef is where the tests' release keeps its contract bundle.
var contractBundleRef = objectstore.Ref{Bucket: testBucket, Key: "tenants/default/topologies/rel-1/contracts.json.gz"}

// producerBundleRef is where the service's CI uploaded its bundle for rel-1.
var producerBundleRef = objectstore.Ref{Bucket: testBucket, Key: "svc-py/rel-1/odcs-bundle.json"}

const contractBundleDoc = `{"schema_version":1,"tenant_id":"default","release_id":"rel-1",
 "provenance":{"svc-py":{"continuo_python_runtime":"0.13.0","image_packages":{"pyarrow":"26.0.0","duckdb":"1.4.0"}}},
 "contracts":{}}`

const producerBundleDoc = `{"bundle_version":1,"service":"svc-py",
 "producer":{"continuo_python_runtime":"0.13.0","continuo_contracts":"0.2.0"},
 "image_packages":{"pyarrow":"25.0.0"},"nodes":[]}`

// gzipped returns doc gzipped with its SHA-256.
func gzipped(t *testing.T, doc string) ([]byte, string) {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write([]byte(doc))
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	sum := sha256.Sum256(buf.Bytes())
	return buf.Bytes(), hex.EncodeToString(sum[:])
}

// storeWithContractBundle returns a store holding doc as the release's
// contract bundle, and the release reader that names it.
func storeWithContractBundle(t *testing.T, doc string) (*objectstoretest.Memory, *fakeBundles) {
	t.Helper()
	mem := objectstoretest.NewMemory(testBucket)
	gz, sha := gzipped(t, doc)
	require.NoError(t, mem.Put(context.Background(), contractBundleRef, gz, "application/gzip"))
	return mem, &fakeBundles{uri: contractBundleRef.String(), sha: sha}
}

func TestImagePackages_FromTheRejectedReleasesContractBundle(t *testing.T) {
	mem, rel := storeWithContractBundle(t, contractBundleDoc)
	require.NoError(t, mem.Put(context.Background(), producerBundleRef, []byte(producerBundleDoc), "application/json"))

	got, err := NewImagePackagesSource(mem, rel, testBucket).ImagePackages(context.Background(), "rel-1", "svc-py")

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"pyarrow": "26.0.0", "duckdb": "1.4.0"}, got)
	assert.Equal(t, []string{"rel-1"}, rel.asked)
	assert.Equal(t, []objectstoretest.Read{{Ref: contractBundleRef, MaxBytes: maxContractBundleBytes}}, mem.Reads(),
		"the contract bundle answers, so the producer bundle is not read")
}

func TestImagePackages_AReleaseRejectedAtParseUsesItsProducerBundle(t *testing.T) {
	mem := objectstoretest.NewMemory(testBucket)
	require.NoError(t, mem.Put(context.Background(), producerBundleRef, []byte(producerBundleDoc), "application/json"))
	rel := &fakeBundles{} // no contract bundle: the parse never ended ok

	got, err := NewImagePackagesSource(mem, rel, testBucket).ImagePackages(context.Background(), "rel-1", "svc-py")

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"pyarrow": "25.0.0"}, got)
	assert.Equal(t, []objectstoretest.Read{{Ref: producerBundleRef, MaxBytes: maxProducerBundleBytes}}, mem.Reads())
}

func TestImagePackages_AContractBundleWithoutTheServiceFallsBackToTheProducerBundle(t *testing.T) {
	mem, rel := storeWithContractBundle(t, `{"schema_version":1,"tenant_id":"default","release_id":"rel-1","provenance":{},"contracts":{}}`)
	require.NoError(t, mem.Put(context.Background(), producerBundleRef, []byte(producerBundleDoc), "application/json"))

	got, err := NewImagePackagesSource(mem, rel, testBucket).ImagePackages(context.Background(), "rel-1", "svc-py")

	require.NoError(t, err)
	assert.Equal(t, map[string]string{"pyarrow": "25.0.0"}, got)
}

func TestImagePackages_NeitherHoldsTheServiceIsErrNoImagePackages(t *testing.T) {
	cases := map[string]func(t *testing.T) (*objectstoretest.Memory, *fakeBundles){
		"no bundle at all": func(*testing.T) (*objectstoretest.Memory, *fakeBundles) {
			return objectstoretest.NewMemory(testBucket), &fakeBundles{}
		},
		"bundles of another service": func(t *testing.T) (*objectstoretest.Memory, *fakeBundles) {
			mem, rel := storeWithContractBundle(t,
				`{"schema_version":1,"tenant_id":"default","release_id":"rel-1","provenance":{"svc-other":{"image_packages":{"a":"1"}}},"contracts":{}}`)
			require.NoError(t, mem.Put(context.Background(), producerBundleRef,
				[]byte(`{"bundle_version":1,"service":"svc-other","image_packages":{"a":"1"},"nodes":[]}`), "application/json"))
			return mem, rel
		},
		"a producer bundle with no package list": func(t *testing.T) (*objectstoretest.Memory, *fakeBundles) {
			mem := objectstoretest.NewMemory(testBucket)
			require.NoError(t, mem.Put(context.Background(), producerBundleRef,
				[]byte(`{"bundle_version":1,"service":"svc-py","nodes":[]}`), "application/json"))
			return mem, &fakeBundles{}
		},
		"a contract bundle whose checksum is wrong and a producer bundle that is not JSON": func(t *testing.T) (*objectstoretest.Memory, *fakeBundles) {
			mem, rel := storeWithContractBundle(t, contractBundleDoc)
			rel.sha = "00"
			require.NoError(t, mem.Put(context.Background(), producerBundleRef, []byte("not json"), "application/json"))
			return mem, rel
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			mem, rel := setup(t)
			_, err := NewImagePackagesSource(mem, rel, testBucket).ImagePackages(context.Background(), "rel-1", "svc-py")
			require.ErrorIs(t, err, ports.ErrNoImagePackages)
			assert.Contains(t, err.Error(), "rel-1")
			assert.Contains(t, err.Error(), "svc-py")
		})
	}
}

func TestImagePackages_AReadThatMayClearIsReturned(t *testing.T) {
	boom := errors.New("connection reset")

	t.Run("the release read", func(t *testing.T) {
		_, err := NewImagePackagesSource(objectstoretest.NewMemory(testBucket), &fakeBundles{err: boom}, testBucket).
			ImagePackages(context.Background(), "rel-1", "svc-py")
		require.ErrorIs(t, err, boom)
		assert.NotErrorIs(t, err, ports.ErrNoImagePackages)
	})
	t.Run("the contract bundle read", func(t *testing.T) {
		mem, rel := storeWithContractBundle(t, contractBundleDoc)
		mem.FailGet(contractBundleRef, boom)
		_, err := NewImagePackagesSource(mem, rel, testBucket).ImagePackages(context.Background(), "rel-1", "svc-py")
		require.ErrorIs(t, err, boom)
		assert.NotErrorIs(t, err, ports.ErrNoImagePackages)
	})
	t.Run("the producer bundle read", func(t *testing.T) {
		mem := objectstoretest.NewMemory(testBucket)
		mem.FailGet(producerBundleRef, boom)
		_, err := NewImagePackagesSource(mem, &fakeBundles{}, testBucket).ImagePackages(context.Background(), "rel-1", "svc-py")
		require.ErrorIs(t, err, boom)
		assert.NotErrorIs(t, err, ports.ErrNoImagePackages)
	})
}
