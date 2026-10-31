package neo4jinfra

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// tenantGuardExempt names the files of this package whose Cypher acts on every
// tenant at once, so their statements are not held to the tenant rule: the
// schema DDL, the startup migration, the marker sweeper and the export loop's
// list of tenants. Every other non-test file is.
var tenantGuardExempt = []string{"schema.go", "migrate.go", "marker_sweeper.go", "tenants.go"}

// TestCypherCarriesTheTenant fails any Cypher statement of this package in
// which a label-qualified node pattern, such as (n:Node {…}) or
// (:NodeVersion), lacks tenant_id: $tenant_id in its property map. The rule
// applies to a node reached through a relationship too. A pattern that names
// no label, such as (n) or (rej)-[:RESOLVED_BY]->(res), is not
// label-qualified: it starts from a node an earlier pattern bound under the
// tenant, and no relationship joins two tenants.
func TestCypherCarriesTheTenant(t *testing.T) {
	pkg := os.DirFS(".")
	entries, err := fs.ReadDir(pkg, ".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	files := map[string][]byte{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := fs.ReadFile(pkg, name)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		files[name] = src
	}

	scan, err := scanCypher(files)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range scan.violations {
		t.Errorf("%s: %s has no tenant_id: $tenant_id", v.position, v.pattern)
	}
	t.Logf("checked %d label-qualified node patterns", scan.patterns)
	// The scan must have found the package's statements; a scanner that reads
	// nothing would pass every file.
	if scan.patterns < 50 {
		t.Errorf("the scan checked %d label-qualified node patterns, want the package's statements (at least 50)", scan.patterns)
	}
}

// TestCypherGuard_FailsAStatementWithoutTheTenant proves the guard is sound:
// each statement below lacks the tenant in one label-qualified node pattern,
// and the scan reports that pattern, whether the statement is a raw string, an
// interpreted string, a concatenation, a constant used in a concatenation or a
// format string with a label placeholder.
func TestCypherGuard_FailsAStatementWithoutTheTenant(t *testing.T) {
	cases := []struct {
		name    string
		expr    string
		pattern string
	}{
		{"no property map", "`MATCH (:NodeVersion) RETURN 1`", "(:NodeVersion)"},
		{"map without the tenant", "`MATCH (n:Node {unique_id: $uid}) RETURN n`", "(n:Node {unique_id: $uid})"},
		{"interpreted string", `"MERGE (m:Meta {key: 'x'}) RETURN m"`, "(m:Meta {key: 'x'})"},
		{"reached through a relationship",
			"`MATCH (n:Node {tenant_id: $tenant_id})-[:CURRENT]->(v:NodeVersion) RETURN v`", "(v:NodeVersion)"},
		{"in an EXISTS subquery",
			"`MATCH (r:Rejection {tenant_id: $tenant_id}) RETURN exists { (r)-[:RESOLVED_BY]->(:Proposal) } AS x`", "(:Proposal)"},
		{"tenant read from another node",
			"`MATCH (n:Node {tenant_id: $tenant_id}) MATCH (m:Meta {tenant_id: n.tenant_id}) RETURN m`", "(m:Meta {tenant_id: n.tenant_id})"},
		{"another parameter", "`MATCH (n:Node {tenant_id: $tenant}) RETURN n`", "(n:Node {tenant_id: $tenant})"},
		{"two labels", "`CREATE (n:Node:Table {unique_id: $uid})`", "(n:Node:Table {unique_id: $uid})"},
		{"label expression", "`MATCH (n:Node|Table) RETURN n`", "(n:Node|Table)"},
		{"backquoted label", "`MATCH (n:`+\"`Node`\"+` {unique_id: $uid}) RETURN n`", "(n:`Node` {unique_id: $uid})"},
		{"label placeholder", "fmt.Sprintf(`MATCH (n:%s {unique_id: $uid}) RETURN n`, label)", "(n:%s {unique_id: $uid})"},
		{"lower-case keyword", "`match (n:Node) return n`", "(n:Node)"},
		{"concatenation", "`MATCH (n:Node {tenant_id: $tenant_id}) ` + columns() + ` OPTIONAL MATCH (v:NodeVersion {unique_id: n.unique_id})`",
			"(v:NodeVersion {unique_id: n.unique_id})"},
		{"fragment of a concatenation", "`MATCH (n:Node {tenant_id: $tenant_id}), ` + `(v:NodeVersion) RETURN n, v`",
			"(v:NodeVersion)"},
		{"constant in a concatenation", "`MATCH (n:Node {tenant_id: $tenant_id}), ` + fragment", "(v:NodeVersion {unique_id: $uid})"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// fragment holds no keyword, so only a scan that reads the
			// constant inside the concatenation can see its pattern.
			src := "package neo4jinfra\n\n" +
				"const fragment = `(v:NodeVersion {unique_id: $uid}) RETURN n, v`\n\n" +
				"var statement = " + c.expr + "\n"
			scan, err := scanCypher(map[string][]byte{"synthetic.go": []byte(src)})
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, v := range scan.violations {
				got = append(got, v.pattern)
			}
			if !slices.Contains(got, c.pattern) {
				t.Errorf("the guard reported %q, want it to report %q", got, c.pattern)
			}
		})
	}
}

// TestCypherGuard_AcceptsTenantBoundStatements shows what the rule leaves
// alone: a label-qualified pattern that carries the tenant, an unlabelled
// pattern, a relationship type and a function call or map projection that
// only looks like a pattern.
func TestCypherGuard_AcceptsTenantBoundStatements(t *testing.T) {
	statements := []string{
		"`MATCH (n:Node {tenant_id: $tenant_id, unique_id: $uid})-[:CURRENT]->(v:NodeVersion {tenant_id: $tenant_id}) RETURN v`",
		"`MERGE (m:Meta {key: 'export', tenant_id: $tenant_id}) RETURN m`",
		"`MATCH (rej:Rejection {tenant_id: $tenant_id}) OPTIONAL MATCH (rej)-[:RESOLVED_BY]->(res) WHERE res:NodeVersion OR res:Proposal RETURN res`",
		"`MATCH (n:Node {tenant_id: $tenant_id}) RETURN coalesce(n.active, true), count(DISTINCT n), duration({days: $d}), collect(n { .unique_id, x: coalesce(n.a, n.b) })`",
		"`MATCH (n:Node {\n\t\ttenant_id: $tenant_id,\n\t\tunique_id: $uid\n\t}) RETURN n`",
		`"no statement here (x: y)"`,
	}
	for _, s := range statements {
		src := "package neo4jinfra\n\nvar statement = " + s + "\n"
		scan, err := scanCypher(map[string][]byte{"synthetic.go": []byte(src)})
		if err != nil {
			t.Fatal(err)
		}
		if len(scan.violations) != 0 {
			t.Errorf("the guard reported %v in %s", scan.violations, s)
		}
	}
}

// TestCypherGuard_ExemptsOnlyTheListedFiles shows that the same statement is
// a violation in every file but the exempt ones, the backup's export and
// import included.
func TestCypherGuard_ExemptsOnlyTheListedFiles(t *testing.T) {
	src := []byte("package neo4jinfra\n\nconst statement = `MATCH (n:Node) DETACH DELETE n`\n")
	for _, name := range tenantGuardExempt {
		scan, err := scanCypher(map[string][]byte{name: src})
		if err != nil {
			t.Fatal(err)
		}
		if len(scan.violations) != 0 {
			t.Errorf("%s is exempt, but the guard reported %v", name, scan.violations)
		}
	}
	for _, name := range []string{"export.go", "import.go", "node_query_repository.go"} {
		scan, err := scanCypher(map[string][]byte{name: src})
		if err != nil {
			t.Fatal(err)
		}
		if len(scan.violations) != 1 {
			t.Errorf("%s is not exempt, but the guard reported %v", name, scan.violations)
		}
	}
}

// cypherScan is what scanCypher found: how many label-qualified node patterns
// it checked, and those that lack the tenant.
type cypherScan struct {
	patterns   int
	violations []cypherViolation
}

type cypherViolation struct {
	position string
	pattern  string
}

// cypherKeyword marks a string that holds a Cypher statement.
var cypherKeyword = regexp.MustCompile(`(?i)\b(MATCH|MERGE|CREATE)\b`)

// tenantInMap is the property that binds a node pattern to the statement's
// tenant.
var tenantInMap = regexp.MustCompile(`(^|[{,\s])tenant_id\s*:\s*\$tenant_id\b`)

// scanCypher parses each Go source of files, keyed by file name, and checks
// every Cypher statement of the files the guard does not exempt. A statement
// is a string literal, or a concatenation of string literals and the
// package's string constants, that contains MATCH, MERGE or CREATE; a part of
// a concatenation that is not a string, such as a call, stands for nothing.
func scanCypher(files map[string][]byte) (cypherScan, error) {
	fset := token.NewFileSet()
	parsed := map[string]*ast.File{}
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		f, err := parser.ParseFile(fset, name, files[name], 0)
		if err != nil {
			return cypherScan{}, fmt.Errorf("parse %s: %w", name, err)
		}
		parsed[name] = f
	}
	consts := stringConstants(parsed)

	var scan cypherScan
	for _, name := range names {
		if slices.Contains(tenantGuardExempt, name) {
			continue
		}
		for _, stmt := range statementsOf(parsed[name], consts) {
			if !cypherKeyword.MatchString(stmt.text) {
				continue
			}
			for _, p := range labelledNodePatterns(stmt.text) {
				scan.patterns++
				if !tenantInMap.MatchString(p.properties) {
					scan.violations = append(scan.violations, cypherViolation{
						position: fset.Position(stmt.pos).String(),
						pattern:  p.text,
					})
				}
			}
		}
	}
	return scan, nil
}

// stringConstants returns the expression of every constant the files
// declare, by name, so a concatenation that names one reads its value.
func stringConstants(files map[string]*ast.File) map[string]ast.Expr {
	consts := map[string]ast.Expr{}
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			decl, ok := n.(*ast.GenDecl)
			if !ok || decl.Tok != token.CONST {
				return true
			}
			for _, spec := range decl.Specs {
				vs, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, name := range vs.Names {
					if i < len(vs.Values) {
						consts[name.Name] = vs.Values[i]
					}
				}
			}
			return true
		})
	}
	return consts
}

type statement struct {
	pos  token.Pos
	text string
}

// statementsOf returns every string expression of f: each string literal
// that is not part of a concatenation, and each whole concatenation.
func statementsOf(f *ast.File, consts map[string]ast.Expr) []statement {
	var out []statement
	var visit func(n ast.Node) bool
	visit = func(n ast.Node) bool {
		switch e := n.(type) {
		case *ast.BinaryExpr:
			if e.Op != token.ADD {
				return true
			}
			var parts []string
			var others []ast.Expr
			flattenConcat(e, consts, map[string]bool{}, &parts, &others)
			out = append(out, statement{pos: e.Pos(), text: strings.Join(parts, "")})
			for _, o := range others {
				ast.Inspect(o, visit)
			}
			return false
		case *ast.BasicLit:
			if e.Kind == token.STRING {
				if s, err := strconv.Unquote(e.Value); err == nil {
					out = append(out, statement{pos: e.Pos(), text: s})
				}
			}
		}
		return true
	}
	ast.Inspect(f, visit)
	return out
}

// flattenConcat appends the text of each part of the concatenation e to
// parts: a string literal's value, a string constant's value, and a space for
// any other expression, which it appends to others so its own strings are
// read too.
func flattenConcat(e ast.Expr, consts map[string]ast.Expr, resolving map[string]bool, parts *[]string, others *[]ast.Expr) {
	switch x := e.(type) {
	case *ast.ParenExpr:
		flattenConcat(x.X, consts, resolving, parts, others)
	case *ast.BinaryExpr:
		if x.Op == token.ADD {
			flattenConcat(x.X, consts, resolving, parts, others)
			flattenConcat(x.Y, consts, resolving, parts, others)
			return
		}
		*parts = append(*parts, " ")
		*others = append(*others, x)
	case *ast.BasicLit:
		if x.Kind == token.STRING {
			if s, err := strconv.Unquote(x.Value); err == nil {
				*parts = append(*parts, s)
				return
			}
		}
		*parts = append(*parts, " ")
	case *ast.Ident:
		if v, ok := consts[x.Name]; ok && !resolving[x.Name] {
			resolving[x.Name] = true
			flattenConcat(v, consts, resolving, parts, &[]ast.Expr{})
			delete(resolving, x.Name)
			return
		}
		*parts = append(*parts, " ")
	default:
		*parts = append(*parts, " ")
		*others = append(*others, x)
	}
}

// nodePattern is one label-qualified node pattern of a statement: its text
// and the text of its property map, empty when it has none.
type nodePattern struct {
	text       string
	properties string
}

// labelledNodePatterns returns every node pattern of cypher that names a
// label: an opening parenthesis, an optional variable, then a colon and a
// label (a name, a backquoted name or a format verb such as %s, followed by
// any further labels joined by :, & or |), then an optional property map.
func labelledNodePatterns(cypher string) []nodePattern {
	var out []nodePattern
	for start := 0; start < len(cypher); start++ {
		if cypher[start] != '(' {
			continue
		}
		i := skipSpace(cypher, start+1)
		i = skipName(cypher, i)
		i = skipSpace(cypher, i)
		if i >= len(cypher) || cypher[i] != ':' {
			continue
		}
		end, ok := skipLabels(cypher, i)
		if !ok {
			continue
		}
		i = skipSpace(cypher, end)
		props := ""
		if i < len(cypher) && cypher[i] == '{' {
			close := matchingBrace(cypher, i)
			props = cypher[i : close+1]
			i = close + 1
		}
		// The pattern runs to its closing parenthesis; one that never closes
		// ends with the string.
		closing := strings.IndexByte(cypher[i:], ')')
		stop := len(cypher)
		if closing >= 0 {
			stop = i + closing + 1
		}
		out = append(out, nodePattern{text: cypher[start:stop], properties: props})
	}
	return out
}

// skipLabels reads the labels that start at the colon at i, and returns the
// index after them; ok is false when no label follows the colon.
func skipLabels(s string, i int) (int, bool) {
	labels := 0
	for i < len(s) && (s[i] == ':' || s[i] == '&' || s[i] == '|') {
		j := skipSpace(s, i+1)
		if j < len(s) && s[j] == '!' {
			j = skipSpace(s, j+1)
		}
		k := skipLabel(s, j)
		if k == j {
			break
		}
		labels++
		i = skipSpace(s, k)
	}
	return i, labels > 0
}

// skipLabel returns the index after the label at i: a name, a backquoted
// name, or a format verb that a label is printed into.
func skipLabel(s string, i int) int {
	if i+1 < len(s) && s[i] == '%' && isNameByte(s[i+1]) {
		return i + 2
	}
	return skipName(s, i)
}

// skipName returns the index after the name at i (letters, digits and
// underscores, or a backquoted name), or i when none starts there.
func skipName(s string, i int) int {
	if i < len(s) && s[i] == '`' {
		if end := strings.IndexByte(s[i+1:], '`'); end >= 0 {
			return i + end + 2
		}
		return i
	}
	j := i
	for j < len(s) && isNameByte(s[j]) {
		j++
	}
	return j
}

func isNameByte(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

func skipSpace(s string, i int) int {
	for i < len(s) && (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') {
		i++
	}
	return i
}

// matchingBrace returns the index of the brace that closes the one at open,
// skipping quoted text, or the last index of s when it never closes.
func matchingBrace(s string, open int) int {
	depth := 0
	var quote byte
	for i := open; i < len(s); i++ {
		c := s[i]
		switch {
		case quote != 0:
			switch c {
			case '\\':
				i++
			case quote:
				quote = 0
			}
		case c == '\'' || c == '"' || c == '`':
			quote = c
		case c == '{':
			depth++
		case c == '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return len(s) - 1
}
