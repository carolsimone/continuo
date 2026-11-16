package contractversion_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
)

// render writes a value the way a JSON adapter would for these tests'
// scalars: a string quoted, a nil as null, anything else as it prints.
func render(v any) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case string:
		return strconv.Quote(t)
	case map[string]any:
		return fmt.Sprintf("object(%d)", len(t))
	case []any:
		return fmt.Sprintf("array(%d)", len(t))
	default:
		return fmt.Sprint(t)
	}
}

func property(name, physicalType string) map[string]any {
	return map[string]any{"name": name, "physicalType": physicalType, "logicalType": "number", "required": true}
}

func kpisDocument(amountType string) map[string]any {
	return map[string]any{
		"id":      kpis,
		"version": "0",
		"schema": []any{map[string]any{
			"name": "py_daily_kpis",
			"properties": []any{
				property("day", "DATE"),
				property("tx_count", "INTEGER"),
				property("total_amount", amountType),
			},
		}},
	}
}

func TestDiff_TheDesignsExample(t *testing.T) {
	got := contractversion.Diff(kpisDocument("NUMERIC(12,2)"), kpisDocument("NUMERIC(14,2)"), render)

	assert.Equal(t, []contractversion.FieldChange{{
		Path:       "schema[0].properties[2].physicalType",
		BeforeJSON: `"NUMERIC(12,2)"`,
		AfterJSON:  `"NUMERIC(14,2)"`,
	}}, got)
}

func TestDiff_IdenticalDocumentsHaveNoChange(t *testing.T) {
	assert.Empty(t, contractversion.Diff(kpisDocument("NUMERIC(12,2)"), kpisDocument("NUMERIC(12,2)"), render))
	assert.Empty(t, contractversion.Diff(nil, nil, render))
	assert.Empty(t, contractversion.Diff(map[string]any{"a": map[string]any{}, "b": []any{}}, map[string]any{"a": map[string]any{}, "b": []any{}}, render))
}

func TestDiff_AddedAndRemovedPaths(t *testing.T) {
	before := map[string]any{
		"status": "active",
		"team":   map[string]any{"name": "service-py"},
		"schema": []any{map[string]any{"properties": []any{property("day", "DATE"), property("old", "TEXT")}}},
	}
	after := map[string]any{
		"team":        map[string]any{"name": "service-py", "description": "owners"},
		"description": map[string]any{"purpose": "rollup"},
		"schema":      []any{map[string]any{"properties": []any{property("day", "DATE")}}},
	}

	got := contractversion.Diff(before, after, render)

	assert.Equal(t, []contractversion.FieldChange{
		{Path: "description", BeforeJSON: "", AfterJSON: "object(1)"},
		{Path: "schema[0].properties[1]", BeforeJSON: "object(4)", AfterJSON: ""},
		{Path: "status", BeforeJSON: `"active"`, AfterJSON: ""},
		{Path: "team.description", BeforeJSON: "", AfterJSON: `"owners"`},
	}, got)
}

func TestDiff_AnAppendedArrayElementIsOneAddedPath(t *testing.T) {
	before := map[string]any{"tags": []any{"a"}}
	after := map[string]any{"tags": []any{"a", "b"}}

	assert.Equal(t, []contractversion.FieldChange{{Path: "tags[1]", AfterJSON: `"b"`}},
		contractversion.Diff(before, after, render))
}

func TestDiff_ANullValueIsNotAnAbsentPath(t *testing.T) {
	assert.Equal(t, []contractversion.FieldChange{{Path: "a", BeforeJSON: "null", AfterJSON: ""}},
		contractversion.Diff(map[string]any{"a": nil}, map[string]any{}, render))
	assert.Equal(t, []contractversion.FieldChange{{Path: "a", BeforeJSON: "", AfterJSON: "null"}},
		contractversion.Diff(map[string]any{}, map[string]any{"a": nil}, render))
	assert.Equal(t, []contractversion.FieldChange{{Path: "a", BeforeJSON: "null", AfterJSON: `"x"`}},
		contractversion.Diff(map[string]any{"a": nil}, map[string]any{"a": "x"}, render))
}

func TestDiff_AValueThatChangesKindIsOneRowAtItsPath(t *testing.T) {
	got := contractversion.Diff(
		map[string]any{"description": "plain text", "x": []any{"a"}},
		map[string]any{"description": map[string]any{"purpose": "rollup"}, "x": map[string]any{"0": "a"}},
		render)

	assert.Equal(t, []contractversion.FieldChange{
		{Path: "description", BeforeJSON: `"plain text"`, AfterJSON: "object(1)"},
		{Path: "x", BeforeJSON: "array(1)", AfterJSON: "object(1)"},
	}, got)
}

func TestDiff_AllBeforeIsEmptyMakesEveryTopLevelKeyAdded(t *testing.T) {
	got := contractversion.Diff(nil, map[string]any{"b": "2", "a": "1"}, render)
	assert.Equal(t, []contractversion.FieldChange{{Path: "a", AfterJSON: `"1"`}, {Path: "b", AfterJSON: `"2"`}}, got)
}

func TestDiff_ListsKeysSortedAndArrayIndexesNumerically(t *testing.T) {
	// Go randomises map iteration, so repeat the run to catch an order that
	// leaks from a range over a map.
	before := map[string]any{"z": "1", "m": "1", "a": "1", "list": []any{}}
	after := map[string]any{"z": "2", "m": "2", "a": "2", "list": []any{}}
	for i := 0; i < 12; i++ {
		before["list"] = append(before["list"].([]any), "x")
		after["list"] = append(after["list"].([]any), "x")
	}
	before["list"].([]any)[10] = "old"
	before["list"].([]any)[2] = "old"

	want := []string{"a", "list[2]", "list[10]", "m", "z"}
	for run := 0; run < 25; run++ {
		var paths []string
		for _, c := range contractversion.Diff(before, after, render) {
			paths = append(paths, c.Path)
		}
		assert.Equal(t, want, paths, "run %d", run)
	}
}

func TestDiff_AKeyThatWouldReadAsAPathIsQuoted(t *testing.T) {
	before := map[string]any{"readQueries": map[string]any{"a.b": "1", "c[0]": "1", "d e": "1", "": "1", `q"r`: "1", "plain-key_1": "1"}}
	after := map[string]any{"readQueries": map[string]any{"a.b": "2", "c[0]": "2", "d e": "2", "": "2", `q"r`: "2", "plain-key_1": "2"}}

	var paths []string
	for _, c := range contractversion.Diff(before, after, render) {
		paths = append(paths, c.Path)
	}

	assert.Equal(t, []string{
		`readQueries[""]`,
		`readQueries["a.b"]`,
		`readQueries["c[0]"]`,
		`readQueries.d e`,
		`readQueries.plain-key_1`,
		`readQueries["q\"r"]`,
	}, paths)
}

func TestDiff_NamesTheCustomPropertyAndItsNestedKey(t *testing.T) {
	prop := func(daily string) map[string]any {
		return map[string]any{"vendor": "continuo", "property": "readQueries", "value": map[string]any{"daily": daily}}
	}
	before := map[string]any{"customProperties": []any{map[string]any{"vendor": "continuo", "property": "service", "value": "service-py"}, prop("select 1")}}
	after := map[string]any{"customProperties": []any{map[string]any{"vendor": "continuo", "property": "service", "value": "service-py"}, prop("select 2")}}

	assert.Equal(t, []contractversion.FieldChange{{
		Path: "customProperties[1].value.daily", BeforeJSON: `"select 1"`, AfterJSON: `"select 2"`,
	}}, contractversion.Diff(before, after, render))
}

func TestDiff_LeavesBothDocumentsAlone(t *testing.T) {
	before, after := kpisDocument("NUMERIC(12,2)"), kpisDocument("NUMERIC(14,2)")
	contractversion.Diff(before, after, render)
	assert.Equal(t, kpisDocument("NUMERIC(12,2)"), before)
	assert.Equal(t, kpisDocument("NUMERIC(14,2)"), after)
}

// Two scalars are compared through the caller's renderer, so a number decoded
// as its text and the same number decoded as a float, which the renderer
// writes alike, are not a change, while two numbers it writes apart are.
func TestDiff_ComparesScalarsAsTheRendererWritesThem(t *testing.T) {
	type numberText string
	before := map[string]any{"limit": numberText("9007199254740993"), "same": numberText("12")}
	after := map[string]any{"limit": numberText("9007199254740992"), "same": float64(12)}

	assert.Equal(t, []contractversion.FieldChange{
		{Path: "limit", BeforeJSON: "9007199254740993", AfterJSON: "9007199254740992"},
	}, contractversion.Diff(before, after, render))
}
