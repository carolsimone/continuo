package workflowfile

import (
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
)

// LoadInstall loads the flows and types of builtin, then the ones an install
// keeps in install over them. An install file whose kind and name are a
// built-in's replaces that built-in whole: the install's lists stand as
// written and none of the built-in's is kept. A file of another name is added.
// install may be nil, which loads the built-ins alone.
//
// The notes are what a person reading the startup log needs to know about the
// install's files, flows first and then types, each by name:
//   - "<kind> <name>: replaced by <file>" for a file that replaces a built-in
//     whose content differs (a byte for byte copy changes nothing and says
//     nothing);
//   - "<file>: based on <name>@<revision>, which is now @<revision>" for a
//     file whose based_on names a built-in that has changed since;
//   - "<file>: based on <name>, which is not a built-in <kind>" for a file
//     whose based_on names no built-in of its kind.
//
// A problem in either folder, among them two install files that define one
// kind and name, is returned as an error with the file and line, joined, and
// no set.
func LoadInstall(builtin, install fs.FS) (workflow.Set, []string, error) {
	base, err := Load(builtin)
	if err != nil {
		return workflow.Set{}, nil, err
	}
	if install == nil {
		return base, nil, nil
	}
	own, err := Load(install)
	if err != nil {
		return workflow.Set{}, nil, err
	}

	merged := workflow.Set{Flows: maps.Clone(base.Flows), Types: maps.Clone(base.Types)}
	var notes []string
	flowRevision := func(name string) (string, bool) {
		f, ok := base.Flows[name]
		return f.Revision, ok
	}
	for _, name := range slices.Sorted(maps.Keys(own.Flows)) {
		f := own.Flows[name]
		if prev, ok := base.Flows[name]; ok && prev.Revision != f.Revision {
			notes = append(notes, fmt.Sprintf("flow %s: replaced by %s", name, f.File))
		}
		notes = appendBasedOn(notes, f.File, f.BasedOn, "flow", flowRevision)
		merged.Flows[name] = f
	}
	typeRevision := func(name string) (string, bool) {
		t, ok := base.Types[name]
		return t.Revision, ok
	}
	for _, name := range slices.Sorted(maps.Keys(own.Types)) {
		t := own.Types[name]
		if prev, ok := base.Types[name]; ok && prev.Revision != t.Revision {
			notes = append(notes, fmt.Sprintf("type %s: replaced by %s", name, t.File))
		}
		notes = appendBasedOn(notes, t.File, t.BasedOn, "type", typeRevision)
		merged.Types[name] = t
	}
	return merged, notes, nil
}

// appendBasedOn appends the note a file's based_on earns, if any. basedOn is
// "" or one name and one revision joined by an @; revisionOf gives the
// revision of the built-in of that name and whether there is one.
func appendBasedOn(notes []string, file, basedOn, kind string, revisionOf func(name string) (string, bool)) []string {
	if basedOn == "" {
		return notes
	}
	name, revision, _ := strings.Cut(basedOn, "@")
	current, ok := revisionOf(name)
	switch {
	case !ok:
		return append(notes, fmt.Sprintf("%s: based on %s, which is not a built-in %s", file, name, kind))
	case current != revision:
		return append(notes, fmt.Sprintf("%s: based on %s@%s, which is now @%s", file, name, revision, current))
	}
	return notes
}

// LoadInstallFixtures loads the fixtures of builtin, then the ones an install
// keeps in install over them. A type fixture of the install replaces the
// built-in fixture of the same type; a flow fixture replaces the built-in
// fixture of the same flow and scenario; any other fixture is added. The
// result holds the built-in fixtures that were not replaced, in load order,
// then the install's, in load order. install may be nil, which loads the
// built-ins alone. A problem in either folder is returned as an error with the
// file and line, joined, and no fixture.
func LoadInstallFixtures(builtin, install fs.FS) ([]conformance.Fixture, error) {
	base, err := LoadFixtures(builtin)
	if err != nil {
		return nil, err
	}
	if install == nil {
		return base, nil
	}
	own, err := LoadFixtures(install)
	if err != nil {
		return nil, err
	}
	replaced := make(map[fixtureKey]bool, len(own))
	for _, f := range own {
		replaced[keyOf(f)] = true
	}
	out := make([]conformance.Fixture, 0, len(base)+len(own))
	for _, f := range base {
		if !replaced[keyOf(f)] {
			out = append(out, f)
		}
	}
	return append(out, own...), nil
}

// fixtureKey is what a fixture tests: a type, or a scenario of a flow.
type fixtureKey struct {
	typ      string
	flow     string
	scenario conformance.Scenario
}

func keyOf(f conformance.Fixture) fixtureKey {
	if f.Type != "" {
		return fixtureKey{typ: f.Type}
	}
	return fixtureKey{flow: f.Flow, scenario: f.Scenario}
}
