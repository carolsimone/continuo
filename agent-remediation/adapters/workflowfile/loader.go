// Package workflowfile loads the remediation flows and types an install keeps
// as YAML files into the plain values of domain/workflow. It checks the shape
// of every file (known fields, value types, a kind, unique names) and leaves
// the meaning of the values to the runner's startup checks. Every problem it
// reports names the file and the line.
package workflowfile

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
)

// revisionLength is how many hex characters of a file's sha256 identify it.
const revisionLength = 12

// lineInMessage finds the line a YAML library message names ("line 3: ...",
// "yaml: line 3: ...").
var lineInMessage = regexp.MustCompile(`line (\d+)`)

// Load reads every *.yaml file under the root of fsys, recursively, in path
// order. A file may hold several documents separated by `---`. A document with
// `kind: fixture` is skipped without decoding its body. Load returns every
// problem it finds, joined, and an empty Set when there is one.
func Load(fsys fs.FS) (workflow.Set, error) {
	l := loader{
		set:      workflow.Set{Flows: map[string]workflow.Flow{}, Types: map[string]workflow.Type{}},
		flowFile: map[string]workflow.Pos{},
		typeFile: map[string]workflow.Pos{},
	}
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
		l.loadFile(p, data)
		return nil
	})
	if walkErr != nil {
		l.errs = append(l.errs, walkErr)
	}
	if len(l.errs) > 0 {
		return workflow.Set{}, errors.Join(l.errs...)
	}
	return l.set, nil
}

type loader struct {
	set      workflow.Set
	errs     []error
	flowFile map[string]workflow.Pos
	typeFile map[string]workflow.Pos
}

func (l *loader) fail(file string, line int, format string, args ...any) {
	l.errs = append(l.errs, fmt.Errorf("%s:%d: %s", file, line, fmt.Sprintf(format, args...)))
}

// failYAML reports an error of the YAML library with the line it names.
func (l *loader) failYAML(file string, err error) {
	var te *yaml.TypeError
	if errors.As(err, &te) {
		for _, msg := range te.Errors {
			l.failMessage(file, msg)
		}
		return
	}
	l.failMessage(file, strings.TrimPrefix(err.Error(), "yaml: "))
}

func (l *loader) failMessage(file, msg string) {
	line := 1
	if m := lineInMessage.FindStringSubmatch(msg); m != nil {
		line, _ = strconv.Atoi(m[1])
		msg = strings.Replace(msg, m[0]+": ", "", 1)
	}
	l.fail(file, line, "%s", msg)
}

func (l *loader) loadFile(file string, data []byte) {
	sum := sha256.Sum256(data)
	revision := hex.EncodeToString(sum[:])[:revisionLength]

	dec := yaml.NewDecoder(bytes.NewReader(data))
	for {
		var doc yaml.Node
		err := dec.Decode(&doc)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			l.failYAML(file, err)
			return
		}
		if len(doc.Content) == 0 {
			continue
		}
		l.loadDocument(file, revision, doc.Content[0])
	}
}

func (l *loader) loadDocument(file, revision string, root *yaml.Node) {
	if root.Kind != yaml.MappingNode {
		l.fail(file, root.Line, "a document must be a mapping with a kind")
		return
	}
	kindNode := valueOf(root, "kind")
	if kindNode == nil {
		l.fail(file, root.Line, "document has no kind (flow, type or fixture)")
		return
	}
	switch kindNode.Value {
	case kindFixture:
		return
	case kindFlow:
		l.loadFlow(file, revision, root)
	case kindType:
		l.loadType(file, revision, root)
	default:
		l.fail(file, kindNode.Line, "unknown kind %q (flow, type or fixture)", kindNode.Value)
	}
}

func (l *loader) loadFlow(file, revision string, root *yaml.Node) {
	before := len(l.errs)
	l.checkKeys(file, root, flowKeys)
	l.checkKeys(file, valueOf(root, "output"), outputKeys)
	l.checkKeys(file, valueOf(valueOf(root, "output"), "on_pass"), outputOneKeys)
	l.checkKeys(file, valueOf(valueOf(root, "output"), "on_give_up"), outputOneKeys)

	var f flowFile
	if err := root.Decode(&f); err != nil {
		l.failYAML(file, err)
	}
	if len(l.errs) > before {
		return
	}

	at := func(line int) workflow.Pos { return workflow.Pos{File: file, Line: line} }
	pos := at(root.Line)
	flow := workflow.Flow{
		Version:        f.Version,
		Name:           f.Name,
		Trigger:        f.Trigger,
		Context:        l.refs(file, f.Context),
		AcrossClusters: l.refs(file, f.AcrossCluster),
		Gate:           l.refs(file, f.Gate),
		Tries:          f.Tries,
		OnPass:         workflow.Output{Action: workflow.OutputAction(f.Output.OnPass.Action), Emits: f.Output.OnPass.Emits, Pos: at(keyLine(valueOf(root, "output"), "on_pass", pos.Line))},
		OnGiveUp:       workflow.Output{Action: workflow.OutputAction(f.Output.OnGiveUp.Action), Emits: f.Output.OnGiveUp.Emits, Pos: at(keyLine(valueOf(root, "output"), "on_give_up", pos.Line))},
		Revision:       revision,
		File:           file,
		Pos:            pos,
	}
	flow.TypesPos = pos
	if f.Types.Kind != 0 {
		flow.TypesPos = at(keyLine(root, "types", pos.Line))
		for _, n := range f.Types.Content {
			if n.Kind != yaml.ScalarNode {
				l.fail(file, n.Line, "a type name must be a string")
				continue
			}
			flow.Types = append(flow.Types, n.Value)
		}
	}
	if len(l.errs) > before {
		return
	}
	if prev, dup := l.flowFile[flow.Name]; dup {
		l.fail(file, pos.Line, "flow %q is already defined at %s:%d", flow.Name, prev.File, prev.Line)
		return
	}
	l.flowFile[flow.Name] = pos
	l.set.Flows[flow.Name] = flow
}

func (l *loader) loadType(file, revision string, root *yaml.Node) {
	before := len(l.errs)
	l.checkKeys(file, root, typeKeys)
	l.checkKeys(file, valueOf(root, "match"), matchKeys)
	l.checkKeys(file, valueOf(root, "agent"), agentKeys)

	var f typeFile
	if err := root.Decode(&f); err != nil {
		l.failYAML(file, err)
	}
	if len(l.errs) > before {
		return
	}

	at := func(line int) workflow.Pos { return workflow.Pos{File: file, Line: line} }
	pos := at(root.Line)
	typ := workflow.Type{
		Version: f.Version,
		Name:    f.Name,
		Match: workflow.Match{
			Cluster: workflow.ClusterKind(f.Match.Cluster),
			Source:  f.Match.Source,
			Pos:     at(keyLine(root, "match", pos.Line)),
		},
		Context: l.refs(file, f.Context),
		Agent: workflow.Agent{
			Tools:  l.refs(file, f.Agent.Tools),
			Rounds: f.Agent.Rounds,
			Pos:    at(keyLine(root, "agent", pos.Line)),
		},
		Preflight: l.refs(file, f.Preflight),
		Revision:  revision,
		File:      file,
		Pos:       pos,
	}
	if f.Agent.Prompt != "" {
		typ.Agent.Prompt = workflow.StepRef{
			Name: workflow.StepName(f.Agent.Prompt),
			Pos:  at(keyLine(valueOf(root, "agent"), "prompt", pos.Line)),
		}
	}
	if f.Match.NodeTypes.Kind == yaml.SequenceNode {
		typ.Match.NodeTypes = []string{}
		for _, n := range f.Match.NodeTypes.Content {
			if n.Kind != yaml.ScalarNode {
				l.fail(file, n.Line, "a node type must be a string")
				continue
			}
			typ.Match.NodeTypes = append(typ.Match.NodeTypes, n.Value)
		}
	} else if f.Match.NodeTypes.Kind != 0 && f.Match.NodeTypes.Tag != "!!null" {
		l.fail(file, f.Match.NodeTypes.Line, "node_types must be a list")
	}
	if len(l.errs) > before {
		return
	}
	if prev, dup := l.typeFile[typ.Name]; dup {
		l.fail(file, pos.Line, "type %q is already defined at %s:%d", typ.Name, prev.File, prev.Line)
		return
	}
	l.typeFile[typ.Name] = pos
	l.set.Types[typ.Name] = typ
}

// refs maps a YAML list of step names to step references with their lines.
func (l *loader) refs(file string, nodes []yaml.Node) []workflow.StepRef {
	var out []workflow.StepRef
	for _, n := range nodes {
		if n.Kind != yaml.ScalarNode {
			l.fail(file, n.Line, "a step must be named by a string")
			continue
		}
		out = append(out, workflow.StepRef{Name: workflow.StepName(n.Value), Pos: workflow.Pos{File: file, Line: n.Line}})
	}
	return out
}

// checkKeys refuses every key of the mapping n that is not in allowed. A nil or
// non-mapping node is left to the decoder.
func (l *loader) checkKeys(file string, n *yaml.Node, allowed []string) {
	if n == nil || n.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		k := n.Content[i]
		if !slices.Contains(allowed, k.Value) {
			l.fail(file, k.Line, "field %q is not known here (allowed: %s)", k.Value, strings.Join(allowed, ", "))
		}
	}
}

// valueOf returns the value node of key in the mapping n, or nil.
func valueOf(n *yaml.Node, key string) *yaml.Node {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}

// keyLine returns the line of key in the mapping n, or fallback when it is
// absent.
func keyLine(n *yaml.Node, key string, fallback int) int {
	if n == nil || n.Kind != yaml.MappingNode {
		return fallback
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i].Line
		}
	}
	return fallback
}
