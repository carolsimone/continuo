package remediation

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// ModelRename is the plan for a dbt model whose relation another node already
// builds, in the same schema. Relation, OtherService and OtherFilePath name the
// relation in dispute and the node that already builds it. References holds,
// by repository-relative path, every other file of the model's project that
// names the model, with its content.
//
// The model's name and its table are usually one name, and then the model
// moves: OldName and NewName are model names, and the model's file moves from
// OldPath to NewPath (in dbt a model's name is its file's name). Each reference
// must name NewName once the model moves.
//
// When the model's alias, not its name, makes it write the relation in dispute
// (ChangesAlias), moving the file would leave the alias, and so the clash, in
// place. The alias changes instead: OldName and NewName are the alias's old and
// new table name, NewPath is OldPath, and no file moves.
type ModelRename struct {
	Service       string
	Relation      string
	OtherService  string
	OtherFilePath string
	ChangesAlias  bool
	OldName       string
	NewName       string
	OldPath       string
	NewPath       string
	References    map[string]string
	Order         []string
}

// Explanation is the account of the rename a pull request opens with: which
// node already builds the relation, why the release was refused, what the
// model, or its alias, is renamed to, and that its owner may choose another
// name.
func (r ModelRename) Explanation() string {
	what := "The new model is renamed to " + r.NewName
	if r.ChangesAlias {
		what = "The new model's alias is changed to " + r.NewName
	}
	return fmt.Sprintf(
		"%s is already built by %s (%s). This release added a second model that builds it in %s, so continuo refused it. "+
			"%s so the release can pass; rename it as you prefer before merging.",
		r.Relation, r.OtherService, r.OtherFilePath, r.Service, what)
}

// RelationTable is the table part of a relation id: what follows its last dot.
func RelationTable(relation string) string {
	return relation[strings.LastIndex(relation, ".")+1:]
}

// RelationSchema is the schema part of a relation id: what comes before its
// last dot, empty when it has none.
func RelationSchema(relation string) string {
	if i := strings.LastIndex(relation, "."); i >= 0 {
		return relation[:i]
	}
	return ""
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

// RefsModel reports whether text, a dbt SQL or Python model or a properties
// file, refs the model name of project: ref('name') or ref("name") (a versioned
// ref('name', v=2) too), dbt.ref('name') in a Python model, or the two-argument
// ref('project', 'name') naming the model's own project. A two-argument ref to
// another package names that package's model, never this one, and so does any
// two-argument ref when project is empty.
func RefsModel(text, project, name string) bool {
	q := regexp.QuoteMeta(name)
	local := regexp.MustCompile(`\bref\(\s*['"]` + q + `['"]\s*(?:,\s*(?:v|version)\s*=[^)]*)?\)`)
	if local.MatchString(text) {
		return true
	}
	if project == "" {
		return false
	}
	qualified := regexp.MustCompile(`\bref\(\s*['"]` + regexp.QuoteMeta(project) + `['"]\s*,\s*['"]` + q + `['"]\s*(?:,\s*(?:v|version)\s*=[^)]*)?\)`)
	return qualified.MatchString(text)
}
