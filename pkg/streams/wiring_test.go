package streams_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// servicesWithMainGo lists service-root main.go paths (relative to repo root)
// that must reference only pkg/streams constants — never string literals — for
// stream names or consumer groups.
var servicesWithMainGo = []string{
	"run-controller/main.go",
	"catalog/main.go",
	"execution-controller/main.go",
	"release-controller/main.go",
	"remediation/main.go",
	"agent-remediation/main.go",
	"dead-letter-controller/main.go",
}

// streamLiteralRe matches versioned stream literals (e.g. "task.dispatch:v1",
// "consumer.dead_letter:v1"). Underscores appear in segment names such as
// dead_letter, so they are part of the character class.
var streamLiteralRe = regexp.MustCompile(`^[a-z][a-z._]+:v[0-9]+$`)

// groupLiteralRe matches kebab-case identifiers plausibly used as consumer-group names.
var groupLiteralRe = regexp.MustCompile(`^[a-z][a-z0-9]+(-[a-z0-9]+)+$`)

// repoRootFromTest walks up until it finds go.work.
func repoRootFromTest(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.work")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("repo root not found from %s", wd)
		}
		dir = parent
	}
}

// contractServicePrefixes returns "<service>-" for every service that
// contract.yaml declares as a consumer, so the detector below follows the
// contract and cannot drift from the services it names.
func contractServicePrefixes(t *testing.T) []string {
	t.Helper()
	raw, err := os.ReadFile("contract.yaml")
	if err != nil {
		t.Fatalf("read contract.yaml: %v", err)
	}
	var c struct {
		Streams []struct {
			Consumers []struct {
				Service string `yaml:"service"`
			} `yaml:"consumers"`
		} `yaml:"streams"`
	}
	if err := yaml.Unmarshal(raw, &c); err != nil {
		t.Fatalf("parse contract.yaml: %v", err)
	}
	seen := map[string]bool{}
	var prefixes []string
	for _, s := range c.Streams {
		for _, cons := range s.Consumers {
			if cons.Service != "" && !seen[cons.Service] {
				seen[cons.Service] = true
				prefixes = append(prefixes, cons.Service+"-")
			}
		}
	}
	if len(prefixes) == 0 {
		t.Fatal("contract.yaml declares no consuming service: the detector has no prefix to look for")
	}
	return prefixes
}

// inlinedNames returns one message per string literal of f that is a versioned
// stream name or a consumer-group name, positioned by fset. A literal is a
// group name when it is kebab-case and either mentions "group" or "consumer"
// or starts with one of servicePrefixes.
func inlinedNames(fset *token.FileSet, f *ast.File, servicePrefixes []string) []string {
	var found []string
	ast.Inspect(f, func(n ast.Node) bool {
		bl, ok := n.(*ast.BasicLit)
		if !ok || bl.Kind != token.STRING {
			return true
		}
		val := strings.Trim(bl.Value, `"`)
		pos := fset.Position(bl.Pos())
		if streamLiteralRe.MatchString(val) {
			found = append(found, fmt.Sprintf("%s:%d: stream-name literal %q — use pkg/streams constant", pos.Filename, pos.Line, val))
		}
		if groupLiteralRe.MatchString(val) && (strings.Contains(val, "group") ||
			strings.Contains(val, "consumer") ||
			hasAnyPrefix(val, servicePrefixes)) {
			found = append(found, fmt.Sprintf("%s:%d: group-name literal %q — use pkg/streams constant", pos.Filename, pos.Line, val))
		}
		return true
	})
	return found
}

func hasAnyPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// TestNoStreamOrGroupLiteralsInMains walks every service main.go and asserts
// that no string literal matches the stream/group regexes.
func TestNoStreamOrGroupLiteralsInMains(t *testing.T) {
	root := repoRootFromTest(t)
	prefixes := contractServicePrefixes(t)
	fset := token.NewFileSet()
	for _, rel := range servicesWithMainGo {
		path := filepath.Join(root, rel)
		f, err := parser.ParseFile(fset, path, nil, parser.AllErrors)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		for _, msg := range inlinedNames(fset, f, prefixes) {
			t.Error(msg)
		}
	}
}

// TestInlinedNamesDetector_FlagsAGroupOfEveryContractService proves the
// detector flags an inlined group literal for each service the contract
// declares as a consumer, and a versioned stream literal, in a main.go-shaped
// source; and that a constant reference and an unrelated kebab-case literal
// pass.
func TestInlinedNamesDetector_FlagsAGroupOfEveryContractService(t *testing.T) {
	prefixes := contractServicePrefixes(t)
	fset := token.NewFileSet()

	for _, prefix := range prefixes {
		group := prefix + "some-purpose"
		src := "package main\n\nfunc main() { _ = \"" + group + "\" }\n"
		f, err := parser.ParseFile(fset, "main.go", src, parser.AllErrors)
		if err != nil {
			t.Fatalf("parse fixture for %s: %v", group, err)
		}
		if got := inlinedNames(fset, f, prefixes); len(got) != 1 || !strings.Contains(got[0], group) {
			t.Errorf("the detector did not flag the inlined group %q: %v", group, got)
		}
	}

	src := "package main\n\nfunc main() {\n\t_ = \"task.dispatch:v1\"\n\t_ = streams.RunControllerTaskStatusUpdated\n\t_ = \"some-flag-name\"\n}\n"
	f, err := parser.ParseFile(fset, "main.go", src, parser.AllErrors)
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	got := inlinedNames(fset, f, prefixes)
	if len(got) != 1 || !strings.Contains(got[0], "task.dispatch:v1") {
		t.Fatalf("want the stream literal flagged and nothing else, got %v", got)
	}
}

// streamLiteralScanDirs lists the production directories that must reference
// pkg/streams constants for stream names rather than inline literals. These are
// the seams where a versioned-stream string is most likely to be hand-written:
// Redis stream bindings (the message_processing.stream_name dedup value), the
// message handlers (the dedup message_type argument), run-controller's outbox writer
// (each row's stream_name and event_type), and the shared consumer, event and
// outbox packages that name the dead-letter streams.
var streamLiteralScanDirs = []string{
	"run-controller/adapters/postgres",
	"run-controller/adapters/redis",
	"run-controller/service/commands",
	"run-controller/service/handlers",
	"run-controller/service/runreads",
	"run-controller/internal/grpc/runservice",
	"catalog/adapters/redis",
	"catalog/service/handlers",
	"execution-controller/adapters/redis",
	"execution-controller/service/handlers",
	"release-controller/adapters/redis",
	"release-controller/service/handlers",
	"remediation/adapters/redis",
	"agent-remediation/adapters/redis",
	"dead-letter-controller/adapters/redis",
	"dead-letter-controller/service/handlers",
	"pkg/redis",
	"pkg/events",
	"pkg/outbox",
}

// TestNoStreamLiteralsInProductionGoFiles walks every production (non-test) Go
// file under the binding/handler directories and asserts that no string literal
// matches the versioned-stream regex. This is the repo-wide guard that catches
// inlined names like "task.dispatch:v1" outside service main.go files. The
// regex matches a literal that is a stream name as a whole; a name inside a
// longer literal, such as a log message, is not matched. A listed directory
// that is missing or holds no production Go file fails the test, so a rename
// cannot switch its coverage off.
//
// Test files (*_test.go) are out of scope: a contract test legitimately asserts
// that a constant resolves to its exact wire value (e.g. that
// streams.ConsumerDeadLetterV1 == "consumer.dead_letter:v1"), and
// such an assertion needs the literal. Generated code (streams.gen.go) is the
// single source of the literals and lives in this package, not the scan dirs.
func TestNoStreamLiteralsInProductionGoFiles(t *testing.T) {
	root := repoRootFromTest(t)
	fset := token.NewFileSet()

	for _, rel := range streamLiteralScanDirs {
		dir := filepath.Join(root, rel)
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("scan dir %s: %v — a renamed or removed directory must be updated in streamLiteralScanDirs", rel, err)
			continue
		}
		scanned := 0
		walkErr := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, path, nil, parser.AllErrors)
			if perr != nil {
				t.Fatalf("parse %s: %v", path, perr)
			}
			scanned++
			ast.Inspect(f, func(n ast.Node) bool {
				bl, ok := n.(*ast.BasicLit)
				if !ok || bl.Kind != token.STRING {
					return true
				}
				val := strings.Trim(bl.Value, `"`)
				if streamLiteralRe.MatchString(val) {
					t.Errorf("%s:%d: stream-name literal %q — use a pkg/streams constant",
						path, fset.Position(bl.Pos()).Line, val)
				}
				return true
			})
			return nil
		})
		if walkErr != nil {
			t.Fatalf("walk %s: %v", dir, walkErr)
		}
		if scanned == 0 {
			t.Errorf("scan dir %s holds no production Go file, so the guard checks nothing there", rel)
		}
	}
}
