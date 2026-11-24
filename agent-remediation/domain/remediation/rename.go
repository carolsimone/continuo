package remediation

import (
	"fmt"
	"regexp"
	"strconv"
)

// ModelRename is the plan for a dbt model whose relation another node already
// builds: the model moves, in the same schema, from OldName to NewName, which
// in dbt means its file moves from OldPath to NewPath (a model's name is its
// file's name). References holds every other file of its project that names
// the model, by repository-relative path, with its content; each must name
// NewName once the model moves. Relation, OtherService and OtherFilePath name
// the relation in dispute and the node that already builds it.
type ModelRename struct {
	Service       string
	Relation      string
	OtherService  string
	OtherFilePath string
	OldName       string
	NewName       string
	OldPath       string
	NewPath       string
	References    map[string]string
	Order         []string
}

// Explanation is the account of the rename a pull request opens with: which
// node already builds the relation, why the release was refused, what the
// model is renamed to, and that its owner may choose another name.
func (r ModelRename) Explanation() string {
	return fmt.Sprintf(
		"%s is already built by %s (%s). This release added a second model with that name in %s, so continuo refused it. "+
			"The new model is renamed to %s so the release can pass; rename it as you prefer before merging.",
		r.Relation, r.OtherService, r.OtherFilePath, r.Service, r.NewName)
}

// versionSuffix matches a model name that already ends in _v<n>.
var versionSuffix = regexp.MustCompile(`^(.+)_v([0-9]+)$`)

// NextModelName returns the first of name_v2, name_v3, … that taken does not
// report as used. A name that already ends in _v<n> goes on from n+1, so
// orders_v2 becomes orders_v3 rather than orders_v2_v2.
func NextModelName(name string, taken func(string) bool) string {
	base, next := name, 2
	if m := versionSuffix.FindStringSubmatch(name); m != nil {
		if n, err := strconv.Atoi(m[2]); err == nil {
			base, next = m[1], n+1
		}
	}
	for {
		candidate := fmt.Sprintf("%s_v%d", base, next)
		if !taken(candidate) {
			return candidate
		}
		next++
	}
}

// RefsModel reports whether sql refs the model name: ref('name'), ref("name"),
// or the two-argument ref('project', 'name').
func RefsModel(sql, name string) bool {
	return refPattern(name).MatchString(sql)
}

func refPattern(name string) *regexp.Regexp {
	q := regexp.QuoteMeta(name)
	return regexp.MustCompile(`\bref\(\s*(?:['"][^'"]+['"]\s*,\s*)?['"]` + q + `['"]\s*\)`)
}
