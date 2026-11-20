package workflowfile

import (
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
)

// InstallReport is what LoadInstall says about an install's files besides the
// set they make with the built-ins.
type InstallReport struct {
	// Notes are what a person reading the startup log needs to know about
	// the install's files, flows first and then types, each by name:
	//   - "<kind> <name>: replaced by <file>" for a file that replaces a
	//     built-in whose content differs (a byte for byte copy changes
	//     nothing and says nothing);
	//   - "<file>: based on <name>@<revision>, which is now @<revision>" for
	//     a file whose based_on names a built-in that has changed since;
	//   - "<file>: based on <name>, which is not a built-in <kind>" for a
	//     file whose based_on names no built-in of its kind.
	Notes []string
	// Changed names each flow and type the install's files add, or replace
	// with content that differs from the built-in's, as "flow <name>" or
	// "type <name>", flows first and then types, each by name. A byte for
	// byte copy of a built-in changes nothing and is not named.
	Changed []string
}

// LoadInstall loads the flows and types of builtin, then the ones an install
// keeps in install over them. An install file whose kind and name are a
// built-in's replaces that built-in whole: the install's lists stand as
// written and none of the built-in's is kept. A file of another name is added.
// install may be nil, which loads the built-ins alone.
//
// A problem in either folder, among them two install files that define one
// kind and name, is returned as an error with the file and line, joined, and
// no set. So is an install file at the path of a built-in file whose flow or
// type stays loaded because no install file replaces it: a path then names
// one loaded file, which the runner's file revisions rely on. An install file
// at a built-in's path that replaces what the built-in file defines, a byte
// for byte copy among them, loads. This is the flow and type part of
// CheckInstallPaths, the part the service needs, since it reads no fixture.
func LoadInstall(builtin, install fs.FS) (workflow.Set, InstallReport, error) {
	base, err := Load(builtin)
	if err != nil {
		return workflow.Set{}, InstallReport{}, err
	}
	if install == nil {
		return base, InstallReport{}, nil
	}
	own, err := Load(install)
	if err != nil {
		return workflow.Set{}, InstallReport{}, err
	}
	if errs := refuseSharedPaths(installDefinitions(own), stayingDefinitions(base, own)); len(errs) > 0 {
		return workflow.Set{}, InstallReport{}, errors.Join(errs...)
	}

	merged := workflow.Set{Flows: maps.Clone(base.Flows), Types: maps.Clone(base.Types)}
	var report InstallReport
	flowRevision := func(name string) (string, bool) {
		f, ok := base.Flows[name]
		return f.Revision, ok
	}
	for _, name := range slices.Sorted(maps.Keys(own.Flows)) {
		f := own.Flows[name]
		prev, ok := base.Flows[name]
		if ok && prev.Revision != f.Revision {
			report.Notes = append(report.Notes, fmt.Sprintf("flow %s: replaced by %s", name, f.File))
		}
		if !ok || prev.Revision != f.Revision {
			report.Changed = append(report.Changed, "flow "+name)
		}
		report.Notes = appendBasedOn(report.Notes, f.File, f.BasedOn, "flow", flowRevision)
		merged.Flows[name] = f
	}
	typeRevision := func(name string) (string, bool) {
		t, ok := base.Types[name]
		return t.Revision, ok
	}
	for _, name := range slices.Sorted(maps.Keys(own.Types)) {
		t := own.Types[name]
		prev, ok := base.Types[name]
		if ok && prev.Revision != t.Revision {
			report.Notes = append(report.Notes, fmt.Sprintf("type %s: replaced by %s", name, t.File))
		}
		if !ok || prev.Revision != t.Revision {
			report.Changed = append(report.Changed, "type "+name)
		}
		report.Notes = appendBasedOn(report.Notes, t.File, t.BasedOn, "type", typeRevision)
		merged.Types[name] = t
	}
	return merged, report, nil
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
// fixture of the same flow and scenario; any other fixture is added. kept is
// the built-in fixtures that were not replaced and own the install's, each in
// load order, so a caller knows which side each fixture comes from. install
// may be nil, which loads the built-ins alone. A problem in either folder is
// returned as an error with the file and line, joined, and no fixture. Where
// an install fixture's file sits is CheckInstallPaths' to check.
func LoadInstallFixtures(builtin, install fs.FS) (kept, own []conformance.Fixture, err error) {
	base, err := LoadFixtures(builtin)
	if err != nil {
		return nil, nil, err
	}
	if install == nil {
		return base, nil, nil
	}
	own, err = LoadFixtures(install)
	if err != nil {
		return nil, nil, err
	}
	replaced := make(map[fixtureKey]bool, len(own))
	for _, f := range own {
		replaced[keyOf(f)] = true
	}
	kept = make([]conformance.Fixture, 0, len(base))
	for _, f := range base {
		if !replaced[keyOf(f)] {
			kept = append(kept, f)
		}
	}
	return kept, own, nil
}

// CheckInstallPaths returns an error, with the file and line, for each install
// file at the path of a built-in file that holds a flow, a type or a fixture
// that stays loaded because no install file of its kind replaces it (the
// same kind and name, or the same fixture key), whatever the kinds of the two
// files. A path then names one loaded file across flows, types and fixtures,
// which remediation-check's file count and messages rely on. An install file
// at a built-in's path whose content replaces all the built-in file holds, a
// byte for byte copy among them, passes, and so does one at the path of a
// built-in that another install file replaces. A file either loader refuses
// is returned as its error. install may be nil, which holds no file.
// LoadInstall refuses the flow and type part of this at startup; the whole
// check needs the fixtures, which only remediation-check loads.
func CheckInstallPaths(builtin, install fs.FS) error {
	if install == nil {
		return nil
	}
	base, err := Load(builtin)
	if err != nil {
		return err
	}
	own, err := Load(install)
	if err != nil {
		return err
	}
	kept, ownFixtures, err := LoadInstallFixtures(builtin, install)
	if err != nil {
		return err
	}
	installs := append(installDefinitions(own), fixtureDefinitions(ownFixtures)...)
	staying := append(stayingDefinitions(base, own), fixtureDefinitions(kept)...)
	return errors.Join(refuseSharedPaths(installs, staying)...)
}

// definition is a flow, a type or a fixture as refuseSharedPaths reads it:
// the file and line it was loaded from, how a message names it, and the verb
// with which a message says a file has it ("defines" a flow or a type, "holds"
// a fixture).
type definition struct {
	file  string
	line  int
	label string
	verb  string
}

// fixtureDefinitions is a definition of each fixture, named by what it tests.
func fixtureDefinitions(fixtures []conformance.Fixture) []definition {
	defs := make([]definition, 0, len(fixtures))
	for _, f := range fixtures {
		label := fmt.Sprintf("the fixture of type %q", f.Type)
		if f.Type == "" {
			label = fmt.Sprintf("the fixture of flow %q and scenario %q", f.Flow, f.Scenario)
		}
		defs = append(defs, definition{file: f.File, line: f.Line, label: label, verb: "holds"})
	}
	return defs
}

// installDefinitions is every flow and type of an install's set.
func installDefinitions(own workflow.Set) []definition {
	defs := make([]definition, 0, len(own.Flows)+len(own.Types))
	for name, f := range own.Flows {
		defs = append(defs, flowDefinition(name, f))
	}
	for name, t := range own.Types {
		defs = append(defs, typeDefinition(name, t))
	}
	return defs
}

// stayingDefinitions is every flow and type of the built-in set base that the
// install's set own does not replace.
func stayingDefinitions(base, own workflow.Set) []definition {
	var defs []definition
	for name, f := range base.Flows {
		if _, replaced := own.Flows[name]; !replaced {
			defs = append(defs, flowDefinition(name, f))
		}
	}
	for name, t := range base.Types {
		if _, replaced := own.Types[name]; !replaced {
			defs = append(defs, typeDefinition(name, t))
		}
	}
	return defs
}

func flowDefinition(name string, f workflow.Flow) definition {
	return definition{file: f.File, line: f.Pos.Line, label: fmt.Sprintf("flow %q", name), verb: "defines"}
}

func typeDefinition(name string, t workflow.Type) definition {
	return definition{file: t.File, line: t.Pos.Line, label: fmt.Sprintf("type %q", name), verb: "defines"}
}

// refuseSharedPaths is one error for each install file at the path of a
// built-in file that holds a definition of staying, the built-in definitions
// the install leaves loaded. The error is at the line of the install file's
// first definition, names that definition and every staying one of the
// built-in file, in line order, each run of one verb said once (defines type
// "a" and holds the fixture of type "b"), and asks for another path. Errors
// come in file order.
func refuseSharedPaths(installs, staying []definition) []error {
	byLine := func(a, b definition) int {
		if c := strings.Compare(a.file, b.file); c != 0 {
			return c
		}
		return a.line - b.line
	}
	stays := map[string][]definition{}
	for _, d := range slices.SortedStableFunc(slices.Values(staying), byLine) {
		stays[d.file] = append(stays[d.file], d)
	}
	var errs []error
	reported := map[string]bool{}
	for _, d := range slices.SortedStableFunc(slices.Values(installs), byLine) {
		held, shared := stays[d.file]
		if !shared || reported[d.file] {
			continue
		}
		reported[d.file] = true
		which := "which stays loaded"
		if len(held) > 1 {
			which = "which stay loaded"
		}
		errs = append(errs, fmt.Errorf("%s:%d: %s is in a file with the path of the built-in file that %s, %s; give the install file another path",
			d.file, d.line, d.label, whatItHolds(held), which))
	}
	return errs
}

// whatItHolds says what a file holds from its definitions in order: the
// labels joined by "and", each run of one verb led by that verb once.
func whatItHolds(defs []definition) string {
	var parts []string
	for i := 0; i < len(defs); {
		j := i
		var labels []string
		for ; j < len(defs) && defs[j].verb == defs[i].verb; j++ {
			labels = append(labels, defs[j].label)
		}
		parts = append(parts, defs[i].verb+" "+strings.Join(labels, " and "))
		i = j
	}
	return strings.Join(parts, " and ")
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
