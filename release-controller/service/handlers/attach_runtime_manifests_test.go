package handlers

import (
	"strings"
	"testing"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func completeRuntimeRef() pkgmodel.RuntimeManifestRef {
	return pkgmodel.RuntimeManifestRef{
		RuntimeManifestURI:                "s3://continuo/finance/rA/manifest.msgpack",
		RuntimeManifestSHA256:             "aa" + strings.Repeat("0", 62),
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: "bb" + strings.Repeat("0", 62),
	}
}

func otherRuntimeRef() pkgmodel.RuntimeManifestRef {
	return pkgmodel.RuntimeManifestRef{
		RuntimeManifestURI:                "s3://continuo/sales/rA/manifest.msgpack",
		RuntimeManifestSHA256:             "cc" + strings.Repeat("0", 62),
		RuntimeManifestDBTVersion:         "1.12.0b1",
		RuntimeManifestParseContextSHA256: "dd" + strings.Repeat("0", 62),
	}
}

func TestAttachRuntimeManifestsKeepsGraphAndDBTIDsSeparate(t *testing.T) {
	topo := release.Topology{{UniqueID: "public.orders", DBTUniqueID: "model.finance.orders", ServiceName: "finance"}}
	refs := map[string]pkgmodel.RuntimeManifestRef{"finance": completeRuntimeRef()}

	got, err := attachRuntimeManifests(topo, refs)

	require.NoError(t, err)
	assert.Equal(t, "public.orders", got[0].UniqueID)
	assert.Equal(t, "model.finance.orders", got[0].DBTUniqueID)
	assert.Equal(t, completeRuntimeRef(), got[0].RuntimeManifestRef)
}

func TestAttachRuntimeManifestsCopiesOnlyToMatchingService(t *testing.T) {
	// Each service's nodes must receive that service's own artifact. Bleeding one
	// service's reference onto another's nodes would point a stable service at a
	// manifest that never described it.
	topo := release.Topology{
		{UniqueID: "public.orders", ServiceName: "finance"},
		{UniqueID: "public.leads", ServiceName: "sales"},
	}
	refs := map[string]pkgmodel.RuntimeManifestRef{
		"finance": completeRuntimeRef(),
		"sales":   otherRuntimeRef(),
	}

	got, err := attachRuntimeManifests(topo, refs)

	require.NoError(t, err)
	assert.Equal(t, completeRuntimeRef(), got[0].RuntimeManifestRef)
	assert.Equal(t, otherRuntimeRef(), got[1].RuntimeManifestRef)
}

func TestAttachRuntimeManifestsDoesNotMutateInput(t *testing.T) {
	topo := release.Topology{{UniqueID: "public.orders", ServiceName: "finance"}}
	refs := map[string]pkgmodel.RuntimeManifestRef{"finance": completeRuntimeRef()}

	_, err := attachRuntimeManifests(topo, refs)

	require.NoError(t, err)
	assert.Equal(t, pkgmodel.RuntimeManifestRef{}, topo[0].RuntimeManifestRef,
		"the input topology must not be mutated in place")
}

func TestAttachRuntimeManifestsAllowsServicesWithoutARef(t *testing.T) {
	// A release produced before runtime manifests existed carries no refs at all.
	// Those nodes must pass through untouched so their Jobs keep parsing the
	// project themselves.
	topo := release.Topology{
		{UniqueID: "public.orders", ServiceName: "finance"},
		{UniqueID: "public.leads", ServiceName: "sales"},
	}
	refs := map[string]pkgmodel.RuntimeManifestRef{"finance": completeRuntimeRef()}

	got, err := attachRuntimeManifests(topo, refs)

	require.NoError(t, err)
	assert.Equal(t, completeRuntimeRef(), got[0].RuntimeManifestRef)
	assert.Equal(t, pkgmodel.RuntimeManifestRef{}, got[1].RuntimeManifestRef)
}

func TestAttachRuntimeManifestsAcceptsNoRefsAtAll(t *testing.T) {
	topo := release.Topology{{UniqueID: "public.orders", ServiceName: "finance"}}

	got, err := attachRuntimeManifests(topo, nil)

	require.NoError(t, err)
	assert.Equal(t, pkgmodel.RuntimeManifestRef{}, got[0].RuntimeManifestRef)
}

func TestAttachRuntimeManifestsRejectsPartialRef(t *testing.T) {
	// A half-filled reference means the producer disagreed with itself; attaching
	// it would hand an executor an artifact it cannot verify.
	topo := release.Topology{{UniqueID: "public.orders", ServiceName: "finance"}}
	refs := map[string]pkgmodel.RuntimeManifestRef{
		"finance": {RuntimeManifestURI: "s3://continuo/finance/rA/manifest.msgpack"},
	}

	_, err := attachRuntimeManifests(topo, refs)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "finance")
}

func TestAttachRuntimeManifestsReportsDeterministicallyWithTwoMalformedRefs(t *testing.T) {
	// Two malformed refs in the same map: iteration order over a Go map is
	// randomized, so without sorting the reported service could differ from run
	// to run even though the inputs are identical. That instability leaks into
	// the persisted release.rejected:v1 error_detail. The lexically-first
	// service name ("finance" < "sales") must always be the one named.
	topo := release.Topology{
		{UniqueID: "public.orders", ServiceName: "finance"},
		{UniqueID: "public.leads", ServiceName: "sales"},
	}
	refs := map[string]pkgmodel.RuntimeManifestRef{
		"sales":   {RuntimeManifestURI: "s3://continuo/sales/rA/manifest.msgpack"},   // partial
		"finance": {RuntimeManifestURI: "s3://continuo/finance/rA/manifest.msgpack"}, // partial
	}

	for i := 0; i < 20; i++ {
		_, err := attachRuntimeManifests(topo, refs)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"finance"`, "must always name the lexically-first offending service")
		assert.NotContains(t, err.Error(), `"sales"`)
	}
}

func TestRuntimeManifestForServiceReturnsTheSharedRef(t *testing.T) {
	topo := release.Topology{
		{UniqueID: "public.orders", ServiceName: "finance", RuntimeManifestRef: completeRuntimeRef()},
		{UniqueID: "public.invoices", ServiceName: "finance", RuntimeManifestRef: completeRuntimeRef()},
		{UniqueID: "public.leads", ServiceName: "sales", RuntimeManifestRef: otherRuntimeRef()},
	}

	got, err := runtimeManifestForService(topo, "finance")

	require.NoError(t, err)
	assert.Equal(t, completeRuntimeRef(), got)
}

func TestRuntimeManifestForServiceRejectsDisagreeingNodes(t *testing.T) {
	// One service is parsed from one project into one artifact. Two of its nodes
	// naming different artifacts means the topology was stitched from two parses,
	// and there is no single artifact to pin.
	topo := release.Topology{
		{UniqueID: "public.orders", ServiceName: "finance", RuntimeManifestRef: completeRuntimeRef()},
		{UniqueID: "public.invoices", ServiceName: "finance", RuntimeManifestRef: otherRuntimeRef()},
	}

	_, err := runtimeManifestForService(topo, "finance")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "public.invoices")
}

func TestRuntimeManifestForServiceIsZeroWhenNothingPinned(t *testing.T) {
	topo := release.Topology{{UniqueID: "public.orders", ServiceName: "finance"}}

	got, err := runtimeManifestForService(topo, "finance")

	require.NoError(t, err)
	assert.Equal(t, pkgmodel.RuntimeManifestRef{}, got)
}

func TestRuntimeManifestForServiceIsZeroForAbsentService(t *testing.T) {
	topo := release.Topology{{UniqueID: "public.orders", ServiceName: "finance", RuntimeManifestRef: completeRuntimeRef()}}

	got, err := runtimeManifestForService(topo, "sales")

	require.NoError(t, err)
	assert.Equal(t, pkgmodel.RuntimeManifestRef{}, got)
}

func TestAttachRuntimeManifestsRejectsUnreferencedServiceKey(t *testing.T) {
	// A ref keyed by a service with no node in the topology means the parse
	// result and the topology disagree about what this release contains.
	topo := release.Topology{{UniqueID: "public.orders", ServiceName: "finance"}}
	refs := map[string]pkgmodel.RuntimeManifestRef{
		"finance": completeRuntimeRef(),
		"ghost":   otherRuntimeRef(),
	}

	_, err := attachRuntimeManifests(topo, refs)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "ghost")
}
