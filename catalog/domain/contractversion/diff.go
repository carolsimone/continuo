package contractversion

import (
	"reflect"
	"slices"
	"strconv"
	"strings"
)

// FieldChange is one changed path of a contract document. BeforeJSON and
// AfterJSON hold the value on each side as the caller's renderer wrote it; an
// added path has an empty BeforeJSON and a removed one an empty AfterJSON.
type FieldChange struct {
	Path       string
	BeforeJSON string
	AfterJSON  string
}

// Diff lists the paths at which two contract documents differ, in a
// deterministic order: object keys sorted, array elements by index, depth
// first. Two objects, or two arrays, are compared member by member; a path
// present on one side only, or holding values of different kinds or unequal
// scalars, is one change. An added or removed object or array is one change at
// its own path, not one per field inside it. Paths read like
// `schema[0].properties[2].physicalType`: a key that is empty or holds a dot,
// a bracket or a quote is written `["key"]`, so two paths never read alike.
//
// render writes a value the way the caller stores it (an adapter passes its
// canonical JSON); it is called only for values that sit at a changed path.
func Diff(before, after map[string]any, render func(any) string) []FieldChange {
	d := differ{render: render}
	d.objects("", before, after)
	return d.changes
}

type differ struct {
	render  func(any) string
	changes []FieldChange
}

func (d *differ) objects(path string, before, after map[string]any) {
	keys := make([]string, 0, len(before)+len(after))
	for k := range before {
		keys = append(keys, k)
	}
	for k := range after {
		if _, inBefore := before[k]; !inBefore {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)

	for _, k := range keys {
		b, inBefore := before[k]
		a, inAfter := after[k]
		d.member(keyPath(path, k), b, inBefore, a, inAfter)
	}
}

func (d *differ) arrays(path string, before, after []any) {
	for i := range max(len(before), len(after)) {
		var b, a any
		if i < len(before) {
			b = before[i]
		}
		if i < len(after) {
			a = after[i]
		}
		d.member(path+"["+strconv.Itoa(i)+"]", b, i < len(before), a, i < len(after))
	}
}

// member compares one path that either side may lack.
func (d *differ) member(path string, before any, inBefore bool, after any, inAfter bool) {
	switch {
	case !inBefore:
		d.changes = append(d.changes, FieldChange{Path: path, AfterJSON: d.render(after)})
	case !inAfter:
		d.changes = append(d.changes, FieldChange{Path: path, BeforeJSON: d.render(before)})
	default:
		d.values(path, before, after)
	}
}

func (d *differ) values(path string, before, after any) {
	switch b := before.(type) {
	case map[string]any:
		if a, ok := after.(map[string]any); ok {
			d.objects(path, b, a)
			return
		}
	case []any:
		if a, ok := after.([]any); ok {
			d.arrays(path, b, a)
			return
		}
	}
	if !reflect.DeepEqual(before, after) {
		d.changes = append(d.changes, FieldChange{Path: path, BeforeJSON: d.render(before), AfterJSON: d.render(after)})
	}
}

// keyPath appends an object key to a path.
func keyPath(path, key string) string {
	if key == "" || strings.ContainsAny(key, `.[]"`) {
		return path + "[" + strconv.Quote(key) + "]"
	}
	if path == "" {
		return key
	}
	return path + "." + key
}
