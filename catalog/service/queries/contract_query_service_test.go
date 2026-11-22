package queries_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/carolsimone/continuo/catalog/adapters/serialization"
	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/service/queries"
)

const kpisID = "analytics.py_daily_kpis"

// kpisContract is design 3.9's py_daily_kpis in its stored form, as the graph
// returns it: numbers are float64.
func kpisContract(amountType string) map[string]any {
	return map[string]any{
		"apiVersion": "v3.2.0",
		"kind":       "DataContract",
		"id":         kpisID,
		"name":       "py_daily_kpis",
		"version":    "0",
		"status":     "active",
		"schema": []any{map[string]any{
			"name":         "py_daily_kpis",
			"physicalType": "table",
			"properties": []any{
				map[string]any{"name": "day", "physicalType": "DATE", "logicalType": "date", "required": true},
				map[string]any{"name": "tx_count", "physicalType": "INTEGER", "logicalType": "integer", "required": true},
				map[string]any{"name": "total_amount", "physicalType": amountType, "logicalType": "number", "required": true},
			},
		}},
	}
}

// kpisHistory is the node's three promotions: version 1 and 2 change the
// amount's type, version 3 reverts to version 1's contract and is current.
func kpisHistory() []contractversion.VersionView {
	promoted := time.Date(2026, 11, 27, 16, 40, 12, 0, time.UTC)
	return []contractversion.VersionView{
		{
			UniqueID: kpisID, Version: 3, ContractHash: "sha256:aaa", ReleaseID: "rel-3", PromotionSeq: 52,
			PromotedAt: promoted, ContentHash: "sha256:c3", ContinuoContracts: "0.2.0",
			SameAsVersion: 1, SourcePath: "contracts/py_daily_kpis.odcs.yaml", IsCurrent: true,
			Document: kpisContract("NUMERIC(12,2)"),
		},
		{
			UniqueID: kpisID, Version: 2, ContractHash: "sha256:bbb", ReleaseID: "rel-2", PromotionSeq: 41,
			PromotedAt: promoted.Add(-24 * time.Hour), ContentHash: "sha256:c2", ContinuoContracts: "0.2.0",
			SourcePath: "contracts/py_daily_kpis.odcs.yaml", Document: kpisContract("NUMERIC(14,2)"),
		},
		{
			UniqueID: kpisID, Version: 1, ContractHash: "sha256:aaa", ReleaseID: "rel-1", PromotionSeq: 30,
			PromotedAt: promoted.Add(-48 * time.Hour), ContentHash: "sha256:c1", ContinuoContracts: "0.2.0",
			SourcePath: "contracts/py_daily_kpis.odcs.yaml", Document: kpisContract("NUMERIC(12,2)"),
		},
	}
}

// fakeContractReader satisfies queries.ContractReader over a fixed history,
// newest first, and records what it was asked.
type fakeContractReader struct {
	history []contractversion.VersionView
	err     error

	tenants  []string
	gotLimit int
}

func (f *fakeContractReader) Current(_ context.Context, tenantID, uniqueID string) (contractversion.VersionView, error) {
	f.tenants = append(f.tenants, tenantID)
	if f.err != nil {
		return contractversion.VersionView{}, f.err
	}
	for _, v := range f.history {
		if v.UniqueID == uniqueID && v.IsCurrent {
			return v, nil
		}
	}
	return contractversion.VersionView{}, contractversion.ErrNotFound
}

func (f *fakeContractReader) History(_ context.Context, tenantID, uniqueID string, limit int) ([]contractversion.VersionView, error) {
	f.tenants = append(f.tenants, tenantID)
	f.gotLimit = limit
	if f.err != nil {
		return nil, f.err
	}
	var out []contractversion.VersionView
	for _, v := range f.history {
		if v.UniqueID == uniqueID {
			v.Document = nil
			out = append(out, v)
		}
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (f *fakeContractReader) ByVersion(_ context.Context, tenantID, uniqueID string, version int64) (contractversion.VersionView, error) {
	f.tenants = append(f.tenants, tenantID)
	if f.err != nil {
		return contractversion.VersionView{}, f.err
	}
	for _, v := range f.history {
		if v.UniqueID == uniqueID && v.Version == version {
			return v, nil
		}
	}
	return contractversion.VersionView{}, contractversion.ErrNotFound
}

func (f *fakeContractReader) Live(context.Context, string, int64) ([]contractversion.LiveContract, error) {
	return nil, nil
}

func (f *fakeContractReader) LatestPromotionSeq(context.Context, string) (int64, error) {
	return 0, nil
}

var _ queries.ContractReader = (*fakeContractReader)(nil)

func newContractService(reader queries.ContractReader) *queries.ContractQueryService {
	return queries.NewContractQueryService(reader, serialization.ODCSYAML{}, serialization.CanonicalJSON, "")
}

// wantKPIsYAML is the published form of the current version: the stored
// contract with version "3" and the provenance property, and no servers list.
const wantKPIsYAML = `apiVersion: v3.2.0
kind: DataContract
id: analytics.py_daily_kpis
name: py_daily_kpis
version: "3"
status: active
customProperties:
  - vendor: continuo
    property: provenance
    value:
      contractHash: "sha256:aaa"
      contentHash: "sha256:c3"
      releaseId: rel-3
      promotionSeq: 52
      promotedAt: "2026-11-27T16:40:12Z"
      continuoContracts: 0.2.0
      sourcePath: contracts/py_daily_kpis.odcs.yaml
schema:
  - name: py_daily_kpis
    physicalType: table
    properties:
      - {name: day, logicalType: date, physicalType: DATE, required: true}
      - {name: tx_count, logicalType: integer, physicalType: INTEGER, required: true}
      - {name: total_amount, logicalType: number, physicalType: "NUMERIC(12,2)", required: true}
`

func TestGetNodeContract_ReturnsTheCurrentVersion(t *testing.T) {
	reader := &fakeContractReader{history: kpisHistory()}

	got, err := newContractService(reader).GetNodeContract(context.Background(), testTenant, kpisID, true)

	require.NoError(t, err)
	assert.Equal(t, int64(3), got.Version.Version)
	assert.Equal(t, int64(1), got.Version.SameAsVersion)
	assert.True(t, got.Version.IsCurrent)
	assert.Equal(t, "sha256:aaa", got.Version.ContractHash)
	assert.Equal(t, "rel-3", got.Version.ReleaseID)
	assert.Equal(t, wantKPIsYAML, got.DocumentYAML)
	assert.Equal(t, []string{testTenant}, reader.tenants, "one read, for the request's tenant")
}

func TestGetNodeContract_WithoutTheDocumentRendersNone(t *testing.T) {
	got, err := newContractService(&fakeContractReader{history: kpisHistory()}).
		GetNodeContract(context.Background(), testTenant, kpisID, false)

	require.NoError(t, err)
	assert.Equal(t, int64(3), got.Version.Version)
	assert.Empty(t, got.DocumentYAML)
}

func TestGetNodeContract_NotFoundWithoutAContract(t *testing.T) {
	_, err := newContractService(&fakeContractReader{history: kpisHistory()}).
		GetNodeContract(context.Background(), testTenant, "analytics.no_contract", true)

	assert.ErrorIs(t, err, contractversion.ErrNotFound)
}

func TestGetNodeContract_AReadFailureIsNotANotFound(t *testing.T) {
	boom := errors.New("neo4j is down")

	_, err := newContractService(&fakeContractReader{err: boom}).
		GetNodeContract(context.Background(), testTenant, kpisID, true)

	assert.ErrorIs(t, err, boom)
	assert.NotErrorIs(t, err, contractversion.ErrNotFound)
}

func TestListNodeContractVersions_NewestFirstWithIsCurrent(t *testing.T) {
	reader := &fakeContractReader{history: kpisHistory()}

	got, err := newContractService(reader).ListNodeContractVersions(context.Background(), testTenant, kpisID, 0)

	require.NoError(t, err)
	require.Len(t, got, 3)
	var versions []int64
	var current []int64
	for _, v := range got {
		versions = append(versions, v.Version)
		if v.IsCurrent {
			current = append(current, v.Version)
		}
		assert.Nil(t, v.Document, "a history row carries no document")
	}
	assert.Equal(t, []int64{3, 2, 1}, versions)
	assert.Equal(t, []int64{3}, current)
	assert.Equal(t, int64(1), got[0].SameAsVersion)
	assert.Equal(t, 0, reader.gotLimit, "limit 0 asks for every version")
	assert.Equal(t, []string{testTenant}, reader.tenants)
}

func TestListNodeContractVersions_PassesTheLimitDown(t *testing.T) {
	reader := &fakeContractReader{history: kpisHistory()}

	got, err := newContractService(reader).ListNodeContractVersions(context.Background(), testTenant, kpisID, 2)

	require.NoError(t, err)
	assert.Len(t, got, 2)
	assert.Equal(t, 2, reader.gotLimit)
}

func TestListNodeContractVersions_ANodeWithNoHistoryHasAnEmptyList(t *testing.T) {
	got, err := newContractService(&fakeContractReader{history: kpisHistory()}).
		ListNodeContractVersions(context.Background(), testTenant, "analytics.no_contract", 0)

	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestGetNodeContractDiff_TheDesignsExample(t *testing.T) {
	reader := &fakeContractReader{history: kpisHistory()}

	got, err := newContractService(reader).GetNodeContractDiff(context.Background(), testTenant, kpisID, 1, 2)

	require.NoError(t, err)
	assert.Equal(t, []contractversion.FieldChange{{
		Path:       "schema[0].properties[2].physicalType",
		BeforeJSON: `"NUMERIC(12,2)"`,
		AfterJSON:  `"NUMERIC(14,2)"`,
	}}, got)
	assert.Equal(t, []string{testTenant, testTenant}, reader.tenants, "both versions read for the request's tenant")
}

func TestGetNodeContractDiff_ARevertDiffersFromTheVersionBeforeItAndNotFromItsOrigin(t *testing.T) {
	service := newContractService(&fakeContractReader{history: kpisHistory()})

	toOrigin, err := service.GetNodeContractDiff(context.Background(), testTenant, kpisID, 1, 3)
	require.NoError(t, err)
	assert.Empty(t, toOrigin, "version 3 holds version 1's contract")

	fromPrevious, err := service.GetNodeContractDiff(context.Background(), testTenant, kpisID, 2, 3)
	require.NoError(t, err)
	assert.Equal(t, []contractversion.FieldChange{{
		Path:       "schema[0].properties[2].physicalType",
		BeforeJSON: `"NUMERIC(14,2)"`,
		AfterJSON:  `"NUMERIC(12,2)"`,
	}}, fromPrevious)
}

func TestGetNodeContractDiff_RefusesAVersionTheNodeDoesNotHave(t *testing.T) {
	service := newContractService(&fakeContractReader{history: kpisHistory()})

	_, errTo := service.GetNodeContractDiff(context.Background(), testTenant, kpisID, 1, 9)
	_, errFrom := service.GetNodeContractDiff(context.Background(), testTenant, kpisID, 9, 1)

	assert.ErrorIs(t, errTo, contractversion.ErrNotFound)
	assert.ErrorIs(t, errFrom, contractversion.ErrNotFound)
}

// boundHistory is kpisHistory with the current version resolved against an
// ontology release: a link on the contract, on its schema object and on a column.
func boundHistory() []contractversion.VersionView {
	history := kpisHistory()
	link := func(thing string) []any {
		return []any{map[string]any{"type": "ontology", "url": "continuo-ontology:" + thing}}
	}
	current := history[0]
	current.OntologyVersion = "rel-o-3@sha256:abc123"
	current.Document["authoritativeDefinitions"] = link("Kpi")
	object := current.Document["schema"].([]any)[0].(map[string]any)
	object["authoritativeDefinitions"] = link("Kpi")
	object["properties"].([]any)[0].(map[string]any)["authoritativeDefinitions"] = link("Kpi.day")
	history[0] = current
	return history
}

// The document GetNodeContract returns is the published form the live folder
// writes, ontology links absolute under the install's base URL included, and
// its provenance names the ontology version the contract was resolved against.
func TestGetNodeContract_ShowsAbsoluteLinks(t *testing.T) {
	service := queries.NewContractQueryService(&fakeContractReader{history: boundHistory()},
		serialization.ODCSYAML{}, serialization.CanonicalJSON, "https://ontology.example/things")

	got, err := service.GetNodeContract(context.Background(), testTenant, kpisID, true)

	require.NoError(t, err)
	assert.Contains(t, got.DocumentYAML, "authoritativeDefinitions:\n  - {type: ontology, url: \"https://ontology.example/things/Kpi\"}\n")
	assert.Contains(t, got.DocumentYAML, `{type: ontology, url: "https://ontology.example/things/Kpi.day"}`)
	assert.NotContains(t, got.DocumentYAML, "continuo-ontology:")
	assert.Contains(t, got.DocumentYAML, `ontologyVersion: "rel-o-3@sha256:abc123"`)
	assert.Equal(t, "rel-o-3@sha256:abc123", got.Version.OntologyVersion)
	assert.Equal(t, "sha256:aaa", got.Version.ContractHash,
		"the rewrite touches the published document, never the recorded hash")
}

// With no base URL the document keeps the links as stored.
func TestGetNodeContract_WithNoBaseURLKeepsTheLinks(t *testing.T) {
	got, err := newContractService(&fakeContractReader{history: boundHistory()}).
		GetNodeContract(context.Background(), testTenant, kpisID, true)

	require.NoError(t, err)
	assert.Contains(t, got.DocumentYAML, `{type: ontology, url: "continuo-ontology:Kpi"}`)
	assert.Contains(t, got.DocumentYAML, `{type: ontology, url: "continuo-ontology:Kpi.day"}`)
	assert.NotContains(t, got.DocumentYAML, "https://")
}
