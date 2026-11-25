package deploycheck

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// retiredNamesFile is the list of retired names, a path from the repository
// root. Its format is described in the file's own header.
const retiredNamesFile = "retired-names.yaml"

// releasedSectionFiles are the files whose released sections describe their
// own versions and keep the words of their date. A released section starts at
// a `## [x.y.z]` or `## x.y.z` heading and runs to the next `## ` heading.
var releasedSectionFiles = map[string]bool{
	"deploy/continuo/CHANGELOG.md": true,
	"docs/release-notes.md":        true,
}

var (
	sectionHeading  = regexp.MustCompile(`^## `)
	releasedHeading = regexp.MustCompile(`^## \[?v?[0-9]+\.[0-9]+\.[0-9]+\]?(\s|$)`)
)

// glossaryFile is the name of every glossary file, the shared one and each
// component's. Its `_Avoid_:` lines name the retired words on purpose.
const glossaryFile = "GLOSSARY.md"

const avoidLinePrefix = "_Avoid_:"

// binarySniffBytes is how much of a file is read for a NUL byte, git's own
// test for a binary file.
const binarySniffBytes = 8000

// retiredNamesList is the YAML document of retiredNamesFile.
type retiredNamesList struct {
	Names *[]retiredNameEntry `yaml:"names"`
}

type retiredNameEntry struct {
	Pattern string   `yaml:"pattern"`
	Use     string   `yaml:"use"`
	Paths   []string `yaml:"paths"`
}

// retiredName is one entry of the list, compiled.
type retiredName struct {
	entry retiredNameEntry
	re    *regexp.Regexp
	paths []*regexp.Regexp
}

// appliesTo reports whether the name is retired in file: everywhere when the
// entry lists no paths, otherwise only in the files its paths cover.
func (n retiredName) appliesTo(file string) bool {
	if len(n.paths) == 0 {
		return true
	}
	for _, p := range n.paths {
		if p.MatchString(file) {
			return true
		}
	}
	return false
}

// retiredNameFinding is one occurrence of a retired name.
type retiredNameFinding struct {
	File  string
	Line  int
	Match string
	Use   string
}

func (f retiredNameFinding) String() string {
	return fmt.Sprintf("%s:%d: %q is a retired name; use %q", f.File, f.Line, f.Match, f.Use)
}

// parseRetiredNames reads the list and compiles each entry. It refuses an
// unknown key, a missing `names` key, a second YAML document, an entry without a pattern or a word to
// use, a pattern that does not compile or matches the empty string, and a
// path that is not a relative path from the repository root.
func parseRetiredNames(data []byte) ([]retiredName, error) {
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var list retiredNamesList
	if err := dec.Decode(&list); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%s: no names key", retiredNamesFile)
		}
		return nil, fmt.Errorf("%s: %w", retiredNamesFile, err)
	}
	if list.Names == nil {
		return nil, fmt.Errorf("%s: no names key", retiredNamesFile)
	}
	if err := dec.Decode(new(retiredNamesList)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: more than one YAML document", retiredNamesFile)
	}
	names := make([]retiredName, 0, len(*list.Names))
	for i, e := range *list.Names {
		where := fmt.Sprintf("%s: names[%d]", retiredNamesFile, i)
		if strings.TrimSpace(e.Pattern) == "" {
			return nil, fmt.Errorf("%s: no pattern", where)
		}
		where = fmt.Sprintf("%s (%s)", where, e.Pattern)
		if strings.TrimSpace(e.Use) == "" {
			return nil, fmt.Errorf("%s: no word to use", where)
		}
		inner, err := regexp.Compile(e.Pattern)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", where, err)
		}
		if inner.MatchString("") {
			return nil, fmt.Errorf("%s: the pattern matches the empty string", where)
		}
		n := retiredName{entry: e, re: regexp.MustCompile(`\b(?:` + e.Pattern + `)\b`)}
		for _, p := range e.Paths {
			re, err := pathPattern(p)
			if err != nil {
				return nil, fmt.Errorf("%s: path %q: %w", where, p, err)
			}
			n.paths = append(n.paths, re)
		}
		names = append(names, n)
	}
	return names, nil
}

// pathPattern compiles a path of the list into a regular expression over a
// file's path from the repository root. `*` and `?` match within one path
// segment, `**` across segments, and every other character is literal. The
// expression matches the path itself and every file under it.
func pathPattern(p string) (*regexp.Regexp, error) {
	p = strings.TrimSuffix(p, "/")
	switch {
	case p == "":
		return nil, errors.New("empty")
	case strings.HasPrefix(p, "/"), strings.HasPrefix(p, "./"), strings.Contains(p, `\`):
		return nil, errors.New("not a relative path from the repository root")
	case path.Clean(p) != p:
		return nil, errors.New("not a clean path")
	}
	var b strings.Builder
	b.WriteString("^")
	for i := 0; i < len(p); i++ {
		c := p[i]
		switch {
		case c == '*' && i+1 < len(p) && p[i+1] == '*':
			i++
			if i+1 < len(p) && p[i+1] == '/' {
				i++
				b.WriteString("(?:.*/)?")
			} else {
				b.WriteString(".*")
			}
		case c == '*':
			b.WriteString("[^/]*")
		case c == '?':
			b.WriteString("[^/]")
		default:
			b.WriteString(regexp.QuoteMeta(string(c)))
		}
	}
	b.WriteString("(?:/.*)?$")
	return regexp.Compile(b.String())
}

// trackedFiles lists the files git tracks under root, in git's order.
func trackedFiles(root string) ([]string, error) {
	out, err := exec.Command("git", "-C", root, "ls-files", "-z").Output() //nolint:gosec // G204: root is the repository root or a test's temporary directory, not external input
	if err != nil {
		return nil, fmt.Errorf("git ls-files in %s: %w", root, err)
	}
	var files []string
	for _, f := range strings.Split(string(out), "\x00") {
		if f != "" {
			files = append(files, f)
		}
	}
	return files, nil
}

// checkRetiredNames reads root's list of retired names and returns every
// occurrence of one in the files git tracks under root, in file, line and
// column order. It returns an error when the list cannot be read or one of
// its paths names no tracked file.
func checkRetiredNames(root string) ([]retiredNameFinding, error) {
	data, err := os.ReadFile(filepath.Join(root, retiredNamesFile)) //nolint:gosec // G304: root joined with a fixed file name, not external input
	if err != nil {
		return nil, err
	}
	names, err := parseRetiredNames(data)
	if err != nil {
		return nil, err
	}
	files, err := trackedFiles(root)
	if err != nil {
		return nil, err
	}
	for _, n := range names {
		for i, p := range n.paths {
			if !anyMatch(p, files) {
				return nil, fmt.Errorf("%s: %s: path %q names no tracked file", retiredNamesFile, n.entry.Pattern, n.entry.Paths[i])
			}
		}
	}
	var findings []retiredNameFinding
	for _, f := range files {
		if f == retiredNamesFile {
			continue
		}
		var applicable []retiredName
		for _, n := range names {
			if n.appliesTo(f) {
				applicable = append(applicable, n)
			}
		}
		if len(applicable) == 0 {
			continue
		}
		content, ok, err := readTrackedFile(root, f)
		if err != nil {
			return nil, err
		}
		if ok {
			findings = append(findings, scanForRetiredNames(f, content, applicable)...)
		}
	}
	return findings, nil
}

func anyMatch(re *regexp.Regexp, files []string) bool {
	for _, f := range files {
		if re.MatchString(f) {
			return true
		}
	}
	return false
}

// readTrackedFile reads one tracked file. It reports false, with no error,
// for a file that is not there to read as text: deleted from the working
// tree, a symbolic link, a submodule, or binary.
func readTrackedFile(root, file string) ([]byte, bool, error) {
	full := filepath.Join(root, filepath.FromSlash(file))
	info, err := os.Lstat(full)
	if errors.Is(err, os.ErrNotExist) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !info.Mode().IsRegular() {
		return nil, false, nil
	}
	content, err := os.ReadFile(full) //nolint:gosec // G304: full is a path git ls-files listed under root, not external input
	if err != nil {
		return nil, false, err
	}
	if bytes.IndexByte(content[:min(len(content), binarySniffBytes)], 0) >= 0 {
		return nil, false, nil
	}
	return content, true, nil
}

// scanForRetiredNames returns every occurrence of names in one file's
// content, leaving out a glossary's `_Avoid_:` lines and the released
// sections of releasedSectionFiles.
func scanForRetiredNames(file string, content []byte, names []retiredName) []retiredNameFinding {
	glossary := path.Base(file) == glossaryFile
	history := releasedSectionFiles[file]
	released := false
	var findings []retiredNameFinding
	for i, raw := range strings.Split(string(content), "\n") {
		line := strings.TrimSuffix(raw, "\r")
		if history && sectionHeading.MatchString(line) {
			released = releasedHeading.MatchString(line)
		}
		if released || (glossary && strings.HasPrefix(line, avoidLinePrefix)) {
			continue
		}
		var onLine []lineMatch
		for _, n := range names {
			for _, loc := range n.re.FindAllStringIndex(line, -1) {
				onLine = append(onLine, lineMatch{column: loc[0], finding: retiredNameFinding{
					File: file, Line: i + 1, Match: line[loc[0]:loc[1]], Use: n.entry.Use,
				}})
			}
		}
		sort.SliceStable(onLine, func(a, b int) bool { return onLine[a].column < onLine[b].column })
		for _, m := range onLine {
			findings = append(findings, m.finding)
		}
	}
	return findings
}

// lineMatch is a finding with the column it starts at, to order the findings
// of one line.
type lineMatch struct {
	column  int
	finding retiredNameFinding
}

// No tracked file names a retired name of retired-names.yaml outside the
// places that keep old words on purpose. A failure names the file, the line,
// the old name and the glossary word to write instead.
func TestNoRetiredNames(t *testing.T) {
	findings, err := checkRetiredNames(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range findings {
		t.Error(f)
	}
	if len(findings) > 0 {
		t.Logf("%d occurrences of retired names; write the glossary word instead (see %s)", len(findings), retiredNamesFile)
	}
}
