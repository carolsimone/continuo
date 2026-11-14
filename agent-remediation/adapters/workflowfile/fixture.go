package workflowfile

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"path"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/carolsimone/continuo/agent-remediation/domain/prompt"
	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/conformance"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// fixtureFile is the shape of a `kind: fixture` document. The trigger uses the
// field names of the remediation.requested payload.
type fixtureFile struct {
	Kind               string                 `yaml:"kind"`
	Version            int                    `yaml:"version"`
	Type               string                 `yaml:"type"`
	Flow               string                 `yaml:"flow"`
	Scenario           string                 `yaml:"scenario"`
	Trigger            triggerFile            `yaml:"trigger"`
	ServiceRepoPaths   map[string]string      `yaml:"service_repo_paths"`
	RepoFiles          map[string]string      `yaml:"repo_files"`
	Objects            map[string]string      `yaml:"objects"`
	Bundle             map[string]bundleFile  `yaml:"bundle"`
	Catalog            catalogFile            `yaml:"catalog"`
	Release            releaseFile            `yaml:"release"`
	Packager           packagerFile           `yaml:"packager"`
	Answers            answersFile            `yaml:"answers"`
	MemberAnswers      map[string]answersFile `yaml:"member_answers"`
	DeclineBeforeModel *patchFile             `yaml:"decline_before_model"`
	GateErrors         map[string]string      `yaml:"gate_errors"`
}

type triggerFile struct {
	Source           string     `yaml:"source"`
	ReleaseID        string     `yaml:"release_id"`
	RemediationRound int        `yaml:"remediation_round"`
	Repo             string     `yaml:"repo"`
	CommitSHA        string     `yaml:"commit_sha"`
	CodeBundleURI    string     `yaml:"code_bundle_uri"`
	Nodes            []nodeFile `yaml:"nodes"`
}

type nodeFile struct {
	NodeID               string         `yaml:"node_id"`
	RelationID           string         `yaml:"relation_id"`
	Category             string         `yaml:"category"`
	ErrorSignature       string         `yaml:"error_signature"`
	Reason               string         `yaml:"reason"`
	ErrorExcerpt         string         `yaml:"error_excerpt"`
	DBTLogURI            string         `yaml:"dbt_log_uri"`
	CandidateArtifactURI string         `yaml:"candidate_artifact_uri"`
	FilePath             string         `yaml:"file_path"`
	Service              string         `yaml:"service"`
	NodeType             string         `yaml:"node_type"`
	OtherService         string         `yaml:"other_service"`
	OtherFilePath        string         `yaml:"other_file_path"`
	ChangedAncestors     []ancestorFile `yaml:"changed_ancestors"`
}

type ancestorFile struct {
	NodeID   string `yaml:"node_id"`
	FilePath string `yaml:"file_path"`
	Service  string `yaml:"service"`
	Depth    int    `yaml:"depth"`
}

type bundleFile struct {
	Runtime string `yaml:"runtime"`
	RawCode string `yaml:"raw_code"`
}

type catalogFile struct {
	Locations       map[string]locationFile         `yaml:"locations"`
	CurrentVersions map[string]versionFile          `yaml:"current_versions"`
	UpstreamChanges map[string][]upstreamChangeFile `yaml:"upstream_changes"`
	Precedents      []precedentFile                 `yaml:"precedents"`
}

type locationFile struct {
	FilePath string `yaml:"file_path"`
	Service  string `yaml:"service"`
}

type versionFile struct {
	RawCode     string `yaml:"raw_code"`
	ContentHash string `yaml:"content_hash"`
	PromotedAt  string `yaml:"promoted_at"`
}

type upstreamChangeFile struct {
	NodeID     string `yaml:"node_id"`
	Depth      int    `yaml:"depth"`
	CodeDiff   string `yaml:"code_diff"`
	ConfigDiff string `yaml:"config_diff"`
	Truncated  bool   `yaml:"truncated"`
}

type precedentFile struct {
	ReleaseID      string `yaml:"release_id"`
	NodeID         string `yaml:"node_id"`
	Stage          string `yaml:"stage"`
	Category       string `yaml:"category"`
	Reason         string `yaml:"reason"`
	ErrorExcerpt   string `yaml:"error_excerpt"`
	RejectedAt     string `yaml:"rejected_at"`
	Resolved       bool   `yaml:"resolved"`
	ResolutionDiff string `yaml:"resolution_diff"`
	PRURL          string `yaml:"pr_url"`
}

type releaseFile struct {
	ImageTag     string            `yaml:"image_tag"`
	FailingNodes map[string]string `yaml:"failing_nodes"`
}

type packagerFile struct {
	Merged string `yaml:"merged"`
	Refuse string `yaml:"refuse"`
}

type answersFile struct {
	Pass    []answerFile `yaml:"pass"`
	Fail    []answerFile `yaml:"fail"`
	Decline []answerFile `yaml:"decline"`
}

// answerFile is one model answer, in the fields of the propose_fix tool.
type answerFile struct {
	ProposedSQL            string             `yaml:"proposed_sql"`
	ProposedContent        string             `yaml:"proposed_content"`
	TargetFile             string             `yaml:"target_file"`
	Files                  []proposedFileFile `yaml:"files"`
	Rationale              string             `yaml:"rationale"`
	Confidence             string             `yaml:"confidence"`
	SuspectedRootCauseNode string             `yaml:"suspected_root_cause_node"`
	Model                  string             `yaml:"model"`
}

type proposedFileFile struct {
	Path    string `yaml:"path"`
	Content string `yaml:"content"`
}

// patchFile is a change to the trigger: node holds the fields that replace
// those of every failing node.
type patchFile struct {
	Node nodeFile `yaml:"node"`
}

// LoadFixtures reads every `kind: fixture` document under the root of fsys,
// recursively, in path order, and skips every other document without
// decoding it. A fixture's fields are checked as a flow's and a type's are: an
// unknown field, a YAML alias, an unsupported version, a fixture that names
// both or neither of a type and a flow, a flow fixture with no scenario, and a
// trigger with no failing node are refused, each with the file and the line.
// LoadFixtures returns every problem it finds, joined, and no fixture when
// there is one.
func LoadFixtures(fsys fs.FS) ([]conformance.Fixture, error) {
	l := loader{}
	var out []conformance.Fixture
	walkErr := fs.WalkDir(fsys, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || path.Ext(p) != ".yaml" {
			return nil
		}
		data, err := fs.ReadFile(fsys, p)
		if err != nil {
			return err
		}
		out = append(out, l.loadFixtureFile(p, data)...)
		return nil
	})
	if walkErr != nil {
		l.errs = append(l.errs, walkErr)
	}
	if len(l.errs) > 0 {
		return nil, errors.Join(l.errs...)
	}
	return out, nil
}

func (l *loader) loadFixtureFile(file string, data []byte) []conformance.Fixture {
	var out []conformance.Fixture
	dec := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var doc yaml.Node
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			l.failYAML(file, err)
			return out
		}
		if len(doc.Content) == 0 {
			continue
		}
		root := doc.Content[0]
		if kind := valueOf(root, "kind"); kind == nil || kind.Value != kindFixture {
			continue
		}
		if f, ok := l.loadFixture(file, root); ok {
			out = append(out, f)
		}
	}
}

func (l *loader) loadFixture(file string, root *yaml.Node) (conformance.Fixture, bool) {
	before := len(l.errs)
	if refuseAliases(l, file, root) {
		return conformance.Fixture{}, false
	}
	l.checkFields(file, root, reflect.TypeFor[fixtureFile]())
	var f fixtureFile
	if err := root.Decode(&f); err != nil {
		l.failYAML(file, err)
	}
	if len(l.errs) > before {
		return conformance.Fixture{}, false
	}

	if f.Version != workflow.SupportedVersion {
		l.fail(file, keyLine(root, "version", root.Line), "unsupported version %d (supported: %d)", f.Version, workflow.SupportedVersion)
	}
	switch {
	case f.Type != "" && (f.Flow != "" || f.Scenario != ""):
		l.fail(file, root.Line, "a fixture names either the type it tests or a flow and a scenario, not both")
	case f.Type == "" && f.Flow == "":
		l.fail(file, root.Line, "a fixture names the type it tests, or a flow and a scenario")
	case f.Flow != "" && f.Scenario == "":
		l.fail(file, keyLine(root, "flow", root.Line), "a flow fixture names the scenario it plays")
	}
	triggerNode := valueOf(root, "trigger")
	if len(f.Trigger.Nodes) == 0 {
		l.fail(file, keyLine(root, "trigger", root.Line), "the fixture's trigger has no failing node")
	}
	for i, n := range f.Trigger.Nodes {
		if n.NodeID == "" {
			l.fail(file, nodeLine(triggerNode, i, root.Line), "a failing node of the trigger has no node_id")
		}
	}
	if len(l.errs) > before {
		return conformance.Fixture{}, false
	}

	fixture := conformance.Fixture{
		File:             file,
		Version:          f.Version,
		Type:             f.Type,
		Flow:             f.Flow,
		Scenario:         conformance.Scenario(f.Scenario),
		Trigger:          f.Trigger.toDomain(),
		ServiceRepoPaths: f.ServiceRepoPaths,
		RepoFiles:        f.RepoFiles,
		Objects:          f.Objects,
		Bundle:           bundleOf(f.Bundle),
		Catalog:          f.Catalog.toConformance(),
		Release:          conformance.Release{ImageTag: f.Release.ImageTag, FailingNodes: f.Release.FailingNodes},
		Packager:         conformance.Packager{Merged: f.Packager.Merged, Refuse: f.Packager.Refuse},
		Answers:          f.Answers.toConformance(),
		GateErrors:       f.GateErrors,
	}
	if len(f.MemberAnswers) > 0 {
		fixture.MemberAnswers = make(map[string]conformance.Answers, len(f.MemberAnswers))
		for node, a := range f.MemberAnswers {
			fixture.MemberAnswers[node] = a.toConformance()
		}
	}
	if f.DeclineBeforeModel != nil {
		patched, ok := l.patchedTrigger(file, f.Trigger, triggerNode,
			valueOf(valueOf(root, "decline_before_model"), "node"), keyLine(root, "decline_before_model", root.Line))
		if !ok {
			return conformance.Fixture{}, false
		}
		fixture.DeclineBeforeModel = &patched
	}
	return fixture, true
}

// patchedTrigger is the trigger with the fields of patch replacing those of
// every failing node. The patch is applied to each node's YAML, so a field it
// names replaces the node's value whole and a field it leaves out keeps the
// node's. A patch that is not a mapping is refused at line.
func (l *loader) patchedTrigger(file string, t triggerFile, triggerNode, patch *yaml.Node, line int) (remediation.Trigger, bool) {
	if patch == nil || patch.Kind != yaml.MappingNode {
		if patch != nil {
			line = patch.Line
		}
		l.fail(file, line, "decline_before_model.node must be a mapping of failing-node fields")
		return remediation.Trigger{}, false
	}
	nodes := valueOf(triggerNode, "nodes")
	patched := t
	patched.Nodes = make([]nodeFile, 0, len(t.Nodes))
	for _, n := range nodes.Content {
		merged := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Line: n.Line, Column: n.Column}
		merged.Content = append(merged.Content, n.Content...)
		for i := 0; i+1 < len(patch.Content); i += 2 {
			key, value := patch.Content[i], patch.Content[i+1]
			if j := keyIndex(merged, key.Value); j >= 0 {
				merged.Content[j+1] = value
				continue
			}
			merged.Content = append(merged.Content, key, value)
		}
		var nf nodeFile
		if err := merged.Decode(&nf); err != nil {
			l.failYAML(file, err)
			return remediation.Trigger{}, false
		}
		patched.Nodes = append(patched.Nodes, nf)
	}
	return patched.toDomain(), true
}

// checkFields refuses every mapping key under n that the Go type t gives no
// yaml field, with its line, through structs, maps, slices and pointers. A
// value whose shape does not fit t is left to the decoder.
func (l *loader) checkFields(file string, n *yaml.Node, t reflect.Type) {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if n == nil {
		return
	}
	switch t.Kind() {
	case reflect.Struct:
		if n.Kind != yaml.MappingNode {
			return
		}
		names, types := yamlFields(t)
		for i := 0; i+1 < len(n.Content); i += 2 {
			k := n.Content[i]
			ft, ok := types[k.Value]
			if !ok {
				l.fail(file, k.Line, "field %q is not known here (allowed: %s)", k.Value, strings.Join(names, ", "))
				continue
			}
			l.checkFields(file, n.Content[i+1], ft)
		}
	case reflect.Map:
		if n.Kind != yaml.MappingNode {
			return
		}
		for i := 1; i < len(n.Content); i += 2 {
			l.checkFields(file, n.Content[i], t.Elem())
		}
	case reflect.Slice:
		if n.Kind != yaml.SequenceNode {
			return
		}
		for _, c := range n.Content {
			l.checkFields(file, c, t.Elem())
		}
	}
}

// yamlFields is the yaml name of every field of the struct type t, in field
// order, and the type each name holds.
func yamlFields(t reflect.Type) (names []string, types map[string]reflect.Type) {
	types = make(map[string]reflect.Type, t.NumField())
	for i := range t.NumField() {
		f := t.Field(i)
		name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
		if name == "" || name == "-" {
			continue
		}
		names = append(names, name)
		types[name] = f.Type
	}
	return names, types
}

// keyIndex is the index of key in the mapping n's content, or -1.
func keyIndex(n *yaml.Node, key string) int {
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return i
		}
	}
	return -1
}

// nodeLine is the line of the i-th failing node of the trigger mapping, or
// fallback.
func nodeLine(trigger *yaml.Node, i, fallback int) int {
	nodes := valueOf(trigger, "nodes")
	if nodes == nil || nodes.Kind != yaml.SequenceNode || i >= len(nodes.Content) {
		return fallback
	}
	return nodes.Content[i].Line
}

func (t triggerFile) toDomain() remediation.Trigger {
	out := remediation.Trigger{
		Source:        t.Source,
		ReleaseID:     t.ReleaseID,
		Round:         max(t.RemediationRound, 1),
		Repo:          t.Repo,
		CommitSHA:     t.CommitSHA,
		CodeBundleURI: t.CodeBundleURI,
		Nodes:         make([]remediation.TriggerNode, 0, len(t.Nodes)),
	}
	for _, n := range t.Nodes {
		node := remediation.TriggerNode{
			NodeID:               n.NodeID,
			RelationID:           n.RelationID,
			ErrorSignature:       n.ErrorSignature,
			Category:             n.Category,
			Reason:               n.Reason,
			ErrorExcerpt:         n.ErrorExcerpt,
			DBTLogURI:            n.DBTLogURI,
			CandidateArtifactURI: n.CandidateArtifactURI,
			FilePath:             n.FilePath,
			Service:              n.Service,
			NodeType:             n.NodeType,
			OtherService:         n.OtherService,
			OtherFilePath:        n.OtherFilePath,
		}
		for _, a := range n.ChangedAncestors {
			node.ChangedAncestors = append(node.ChangedAncestors, remediation.ChangedAncestor(a))
		}
		out.Nodes = append(out.Nodes, node)
	}
	return out
}

func bundleOf(b map[string]bundleFile) map[string]ports.CandidateSource {
	if len(b) == 0 {
		return nil
	}
	out := make(map[string]ports.CandidateSource, len(b))
	for id, src := range b {
		out[id] = ports.CandidateSource{Runtime: src.Runtime, RawCode: src.RawCode}
	}
	return out
}

func (c catalogFile) toConformance() conformance.Catalog {
	out := conformance.Catalog{}
	if len(c.Locations) > 0 {
		out.Locations = make(map[string]conformance.Location, len(c.Locations))
		for id, loc := range c.Locations {
			out.Locations[id] = conformance.Location(loc)
		}
	}
	if len(c.CurrentVersions) > 0 {
		out.CurrentVersions = make(map[string]ports.CurrentVersion, len(c.CurrentVersions))
		for id, v := range c.CurrentVersions {
			out.CurrentVersions[id] = ports.CurrentVersion(v)
		}
	}
	if len(c.UpstreamChanges) > 0 {
		out.UpstreamChanges = make(map[string][]prompt.UpstreamChange, len(c.UpstreamChanges))
		for id, changes := range c.UpstreamChanges {
			for _, ch := range changes {
				out.UpstreamChanges[id] = append(out.UpstreamChanges[id], prompt.UpstreamChange(ch))
			}
		}
	}
	for _, p := range c.Precedents {
		out.Precedents = append(out.Precedents, prompt.Precedent{
			ReleaseID:      p.ReleaseID,
			NodeID:         p.NodeID,
			Stage:          p.Stage,
			Category:       p.Category,
			Reason:         p.Reason,
			ErrorExcerpt:   p.ErrorExcerpt,
			RejectedAt:     p.RejectedAt,
			Resolved:       p.Resolved,
			ResolutionDiff: p.ResolutionDiff,
			PRURL:          p.PRURL,
		})
	}
	return out
}

func (a answersFile) toConformance() conformance.Answers {
	return conformance.Answers{Pass: answersOf(a.Pass), Fail: answersOf(a.Fail), Decline: answersOf(a.Decline)}
}

func answersOf(in []answerFile) []ports.ProposeResult {
	if len(in) == 0 {
		return nil
	}
	out := make([]ports.ProposeResult, 0, len(in))
	for _, a := range in {
		r := ports.ProposeResult{
			ProposedSQL:            a.ProposedSQL,
			ProposedContent:        a.ProposedContent,
			TargetFile:             a.TargetFile,
			Rationale:              a.Rationale,
			Confidence:             a.Confidence,
			SuspectedRootCauseNode: a.SuspectedRootCauseNode,
			Model:                  a.Model,
		}
		for _, f := range a.Files {
			r.Files = append(r.Files, ports.ProposedFile(f))
		}
		out = append(out, r)
	}
	return out
}
