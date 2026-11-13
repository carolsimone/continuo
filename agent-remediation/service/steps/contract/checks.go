package contract

import (
	"context"
	"errors"
	"fmt"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/domain/remediation"
	"github.com/carolsimone/continuo/agent-remediation/domain/workflow"
	"github.com/carolsimone/continuo/agent-remediation/service/ports"
	"github.com/carolsimone/continuo/agent-remediation/service/runner/step"
)

// csvReadKey is the name a python-csv node's contract entry gives its one
// read: the "reads: {csv: <uri>}" key the runtime loads the file from.
const csvReadKey = "csv"

var (
	needsDraft        = []remediation.EvidenceKey{remediation.KeyDraft}
	needsDraftAndFile = []remediation.EvidenceKey{remediation.KeyDraft, remediation.KeyContractFile}
	preflightOnly     = []workflow.Stage{workflow.StagePreflight}
	errNoScratch      = errors.New("the check reads the pass's patched checkout, and the input holds none")
	errNoContractFile = errors.New("the evidence holds no contract file")
)

// proposedFiles are the draft's edits as the files the checkout is patched with.
func proposedFiles(d remediation.Draft) []ports.ProposedFile {
	files := make([]ports.ProposedFile, len(d.Edits))
	for i, e := range d.Edits {
		files[i] = ports.ProposedFile{Path: e.Path, Content: e.Content}
	}
	return files
}

// contractFileOf reads the contract file the context steps found.
func contractFileOf(in step.CheckInput, name workflow.StepName) (remediation.ContractFile, error) {
	file, ok := remediation.Get[remediation.ContractFile](in.Evidence, remediation.KeyContractFile)
	if !ok {
		return remediation.ContractFile{}, fmt.Errorf("%s: %w", name, errNoContractFile)
	}
	return file, nil
}

// result wraps findings as the output of the check called name.
func result(name workflow.StepName, findings []remediation.Finding) step.CheckOutput {
	return step.CheckOutput{Result: remediation.CheckResult{Check: string(name), Findings: findings}}
}

// --- contract_paths_inside_folder ---

// ContractPathsInsideFolder is the check `contract_paths_inside_folder`: the
// answer names files, every one inside the contract directory that declares the
// node, and none twice.
type ContractPathsInsideFolder struct{}

var _ step.Check = (*ContractPathsInsideFolder)(nil)

// NewContractPathsInsideFolder builds the contract_paths_inside_folder step.
func NewContractPathsInsideFolder() *ContractPathsInsideFolder { return &ContractPathsInsideFolder{} }

const nameContractPathsInsideFolder workflow.StepName = "contract_paths_inside_folder"

// Meta describes the step.
func (*ContractPathsInsideFolder) Meta() step.Meta {
	return step.Meta{
		Name: nameContractPathsInsideFolder,
		Kind: step.KindCheck,
		Description: "Fails a draft that changes no file, a file outside the contract directory that declares the node, " +
			"or the same file twice.",
		Needs:  needsDraftAndFile,
		Stages: preflightOnly,
	}
}

// Check finds a no_files finding when the draft has no edit, and for each edit
// a path_outside_contract_dir finding when its path does not resolve inside the
// contract directory and a path_twice finding when its cleaned path repeats an
// earlier edit's. Every breach is reported, in the order of the edits.
func (*ContractPathsInsideFolder) Check(_ context.Context, in step.CheckInput) (step.CheckOutput, error) {
	file, err := contractFileOf(in, nameContractPathsInsideFolder)
	if err != nil {
		return step.CheckOutput{}, err
	}
	return result(nameContractPathsInsideFolder, pathFindings(file.ContractDir, in.Draft.Edits)), nil
}

// pathFindings judges the paths of a draft against the contract directory. A
// path outside it is written straight onto a checkout, so it would edit files
// the fix has no business touching. A repeated path has no single answer:
// applying the list leaves the last entry on disk, which is what gets packaged
// and verified, while the recorded edits would describe an earlier entry. Paths
// are compared in cleaned form so two spellings of one file are one file.
func pathFindings(contractDir string, edits []remediation.Edit) []remediation.Finding {
	if len(edits) == 0 {
		return []remediation.Finding{{Code: "no_files", Message: "the model returned no files to change"}}
	}
	var found []remediation.Finding
	seen := make(map[string]struct{}, len(edits))
	for _, e := range edits {
		if !withinDir(contractDir, e.Path) {
			found = append(found, remediation.Finding{
				Code: "path_outside_contract_dir",
				Message: fmt.Sprintf(
					"the model proposed a change to %q, which is outside the contract directory %q that declares this node",
					e.Path, contractDir),
				File: e.Path,
			})
			continue
		}
		key := path.Clean(filepath.ToSlash(e.Path))
		if _, dup := seen[key]; dup {
			found = append(found, remediation.Finding{
				Code: "path_twice",
				Message: fmt.Sprintf(
					"the model returned %q more than once, so the content to record could not be told apart from the content to apply",
					e.Path),
				File: e.Path,
			})
			continue
		}
		seen[key] = struct{}{}
	}
	return found
}

// withinDir reports whether a model-proposed repository path resolves inside
// dir.
func withinDir(dir, p string) bool {
	if p == "" || filepath.IsAbs(p) || path.IsAbs(p) {
		return false
	}
	clean := path.Clean(filepath.ToSlash(p))
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return false
	}
	dir = path.Clean(filepath.ToSlash(dir))
	if dir == "." {
		return true // the contract directory is the repository root
	}
	return clean == dir || strings.HasPrefix(clean, dir+"/")
}

// --- yaml_reads ---

// YAMLReads is the check `yaml_reads`: every contract yaml file the answer
// returns must be readable as a contract.
type YAMLReads struct {
	inspector ports.ContractInspector
}

var _ step.Check = (*YAMLReads)(nil)

// NewYAMLReads builds the yaml_reads step.
func NewYAMLReads(inspector ports.ContractInspector) *YAMLReads {
	return &YAMLReads{inspector: inspector}
}

const nameYAMLReads workflow.StepName = "yaml_reads"

// Meta describes the step.
func (*YAMLReads) Meta() step.Meta {
	return step.Meta{
		Name:        nameYAMLReads,
		Kind:        step.KindCheck,
		Description: "Fails a draft with a .yaml or .yml file whose new content cannot be read as a contract.",
		Needs:       needsDraft,
		Stages:      preflightOnly,
	}
}

// Check parses the new content of every .yaml and .yml edit and finds one
// unreadable_yaml finding for each that does not parse. The model's own content
// failing to parse is the answer's fault; the original that does not parse is
// of no concern here.
func (c *YAMLReads) Check(_ context.Context, in step.CheckInput) (step.CheckOutput, error) {
	var found []remediation.Finding
	for _, f := range proposedFiles(in.Draft) {
		if !isContractYAMLPath(f.Path) {
			continue
		}
		if _, err := c.inspector.Declarations(f.Content); err != nil {
			found = append(found, unreadableYAML(f.Path, err))
		}
	}
	return result(nameYAMLReads, found), nil
}

func unreadableYAML(p string, err error) remediation.Finding {
	return remediation.Finding{
		Code:    "unreadable_yaml",
		Message: fmt.Sprintf("the model returned %q, which is not a readable contract file: %v", p, err),
		File:    p,
	}
}

// isContractYAMLPath reports whether a repository path is one the contract
// search would parse. Only those files can declare a node, so only those are
// compared across the edit; a script or a data file the answer also returns
// carries no declaration to preserve.
func isContractYAMLPath(p string) bool {
	switch strings.ToLower(path.Ext(filepath.ToSlash(p))) {
	case ".yml", ".yaml":
		return true
	default:
		return false
	}
}

// --- the checks that compare declarations before and after the edit ---

// declarationCheck is a check that compares the node declarations the answer's
// own files held before it was applied with the ones they hold after, and
// judges the difference. Only the returned files are examined, and that is
// complete: a declaration can only change if the file carrying it changed. Both
// sides are read as a whole rather than file by file, so moving an entry
// between two of the answer's own files, which packages identically, is not
// mistaken for deleting it from one.
type declarationCheck struct {
	meta      step.Meta
	inspector ports.ContractInspector
	judge     func(before, after map[string]ports.NodeDeclaration) []remediation.Finding
}

var _ step.Check = (*declarationCheck)(nil)

// Meta describes the step.
func (c *declarationCheck) Meta() step.Meta { return c.meta }

// Check reads the originals from the pass's patched checkout, builds both
// sides' declarations and returns the judge's findings. It finds nothing when
// it cannot judge: a file the answer wrote that does not parse (yaml_reads
// reports it) and a path outside the checkout (contract_paths_inside_folder
// reports it).
func (c *declarationCheck) Check(ctx context.Context, in step.CheckInput) (step.CheckOutput, error) {
	name := c.meta.Name
	if in.Scratch == nil {
		return step.CheckOutput{}, fmt.Errorf("%s: %w", name, errNoScratch)
	}
	files := proposedFiles(in.Draft)
	_, originals, err := in.Scratch.PatchedCheckout(ctx, files)
	if errors.Is(err, step.ErrPathOutsideCheckout) {
		return result(name, nil), nil
	}
	if err != nil {
		return step.CheckOutput{}, err
	}
	before, after, ok := buildDeclarationMaps(c.inspector, files, originals)
	if !ok {
		return result(name, nil), nil
	}
	return result(name, c.judge(before, after)), nil
}

// buildDeclarationMaps parses every contract yaml file in an answer, before and
// after the edit, into declarations keyed by identityKey. A prior content that
// does not parse declared nothing the answer can be held to: the contract
// search skips such a file too, so the node it might have named was never the
// node under repair. ok is false when the answer's own new content does not
// parse.
func buildDeclarationMaps(insp ports.ContractInspector, files []ports.ProposedFile, originals map[string]string) (before, after map[string]ports.NodeDeclaration, ok bool) {
	before = map[string]ports.NodeDeclaration{}
	after = map[string]ports.NodeDeclaration{}
	for _, f := range files {
		if !isContractYAMLPath(f.Path) {
			continue
		}
		if decls, err := insp.Declarations(originals[f.Path]); err == nil {
			for _, d := range decls {
				before[identityKey(d.Identity)] = d
			}
		}
		decls, err := insp.Declarations(f.Content)
		if err != nil {
			return nil, nil, false
		}
		for _, d := range decls {
			after[identityKey(d.Identity)] = d
		}
	}
	return before, after, true
}

// NewNodeIdentityUnchanged builds the node_identity_unchanged step.
func NewNodeIdentityUnchanged(inspector ports.ContractInspector) step.Check {
	return &declarationCheck{
		meta: step.Meta{
			Name: "node_identity_unchanged",
			Kind: step.KindCheck,
			Description: "Fails a draft after which a node the answer's files declared is no longer declared, or is declared under a " +
				"changed identity (schema, table, script, kind, owner, schedule, criticality or secret_ref).",
			Needs:  needsDraft,
			Stages: preflightOnly,
		},
		inspector: inspector,
		judge:     identityFindings,
	}
}

// identityFindings finds one identity_changed finding for each node the answer
// no longer declares or declares under a changed identity. A fix corrects what
// a node declares, never which node is declared, and never what kind of node it
// is: a python-csv node quietly flipped to python-node would validate under a
// different set of rules than the ones the fix was judged against. Nodes are
// walked in key order so an answer that moves several always reports them in
// the same order.
func identityFindings(before, after map[string]ports.NodeDeclaration) []remediation.Finding {
	var found []remediation.Finding
	for _, k := range sortedDeclarationKeys(before) {
		was := before[k]
		now, still := after[k]
		switch {
		case !still:
			found = append(found, remediation.Finding{
				Code: "identity_changed",
				Message: fmt.Sprintf(
					"the model's answer no longer declares %s. A fix corrects what a node declares; removing or renaming "+
						"the node itself would leave the release nothing to validate, so the change could never be proven correct", k),
			})
		case now.Identity != was.Identity:
			found = append(found, remediation.Finding{
				Code: "identity_changed",
				Message: fmt.Sprintf(
					"the model's answer changed the fields that identify %s (%s). Those fields say which node the entry is, "+
						"so changing one makes it a different node rather than a repaired one", k, identityDelta(was.Identity, now.Identity)),
			})
		}
	}
	return found
}

// NewReadsKept builds the reads_kept step.
func NewReadsKept(inspector ports.ContractInspector) step.Check {
	return &declarationCheck{
		meta: step.Meta{
			Name: "reads_kept",
			Kind: step.KindCheck,
			Description: "Fails a draft after which a node the answer's files declared no longer declares a read it declared before. " +
				"A read may be corrected or replaced, never dropped.",
			Needs:  needsDraft,
			Stages: preflightOnly,
		},
		inspector: inspector,
		judge:     readsKeptFindings,
	}
}

// readsKeptFindings finds one read_dropped finding for each node that no longer
// declares a read it declared before. Validation bind-checks the reads a
// contract still declares, so deleting one hides the failure instead of
// repairing it. The rule holds for every node the answer's files touch, not
// only the failing one, because a sibling's script is never edited by the fix.
// A node the answer removes altogether is the identity check's finding.
func readsKeptFindings(before, after map[string]ports.NodeDeclaration) []remediation.Finding {
	var found []remediation.Finding
	for _, k := range sortedDeclarationKeys(before) {
		now, still := after[k]
		if !still {
			continue
		}
		dropped := droppedReads(before[k].ReadKeys, now.ReadKeys)
		if len(dropped) == 0 {
			continue
		}
		found = append(found, remediation.Finding{
			Code: "read_dropped",
			Message: fmt.Sprintf(
				"the model's answer no longer declares the read %s of %s. Validation bind-checks the reads a contract still "+
					"declares, so deleting one hides the failure instead of repairing it: the node's script — which this fix "+
					"does not change — still performs that read, and no release would catch it again. A read may be corrected "+
					"or replaced, never dropped", strings.Join(quoteAll(dropped), ", "), k),
		})
	}
	return found
}

// NewCSVReadKept builds the csv_read_kept step.
func NewCSVReadKept(inspector ports.ContractInspector) step.Check {
	return &declarationCheck{
		meta: step.Meta{
			Name: "csv_read_kept",
			Kind: step.KindCheck,
			Description: "Fails a draft after which a node that declared a \"csv\" read no longer declares one. " +
				"The read's uri may be corrected, but the key may never be removed or renamed.",
			Needs:  needsDraft,
			Stages: preflightOnly,
		},
		inspector: inspector,
		judge:     csvReadKeptFindings,
	}
}

// csvReadKeptFindings finds one csv_read_dropped finding for each node that
// declared a "csv" read before the edit and does not afterwards. That key names
// the file the runtime loads, and the read's value (the uri) is the one thing a
// python-csv fix is allowed to change, so the finding names the csv read
// specifically. A read of another name is reads_kept's finding.
func csvReadKeptFindings(before, after map[string]ports.NodeDeclaration) []remediation.Finding {
	var found []remediation.Finding
	for _, k := range sortedDeclarationKeys(before) {
		now, still := after[k]
		if !still {
			continue
		}
		if hasReadKey(before[k].ReadKeys, csvReadKey) && !hasReadKey(now.ReadKeys, csvReadKey) {
			found = append(found, remediation.Finding{
				Code: "csv_read_dropped",
				Message: fmt.Sprintf(
					"the model's answer no longer declares the %q read of %s. A python-csv node's single read names the "+
						"file its runtime loads; that read's uri may be corrected, but the %q key itself may never be "+
						"removed or renamed, or the node would have nothing left for the runtime to read the file from",
					csvReadKey, k, csvReadKey),
			})
		}
	}
	return found
}

// NewNoNewSecretRef builds the no_new_secret_ref step.
func NewNoNewSecretRef(inspector ports.ContractInspector) step.Check {
	return &declarationCheck{
		meta: step.Meta{
			Name:        "no_new_secret_ref",
			Kind:        step.KindCheck,
			Description: "Fails a draft that adds a node naming a Secret. A fix may add nodes but never grants a node credentials.",
			Needs:       needsDraft,
			Stages:      preflightOnly,
		},
		inspector: inspector,
		judge:     secretRefFindings,
	}
}

// secretRefFindings finds one secret_ref_added finding for each node the answer
// adds that names a Secret. A node that already existed is covered by the
// identity check, which compares its secret_ref.
func secretRefFindings(before, after map[string]ports.NodeDeclaration) []remediation.Finding {
	var found []remediation.Finding
	for _, k := range sortedDeclarationKeys(after) {
		if _, existed := before[k]; !existed && after[k].Identity.SecretRef != "" {
			found = append(found, remediation.Finding{
				Code:    "secret_ref_added",
				Message: fmt.Sprintf("the answer adds node %s with a secret_ref; a fix may not grant credentials", k),
			})
		}
	}
	return found
}

// sortedDeclarationKeys returns a declaration map's identity keys, sorted, so
// every walk reports the same node first for the same answer.
func sortedDeclarationKeys(m map[string]ports.NodeDeclaration) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// droppedReads returns the read names present before an edit and absent after
// it, sorted. Names are compared verbatim: a read's name is how the node's
// script asks for it, so a re-spelling is a rename, and a rename drops the name
// the script uses.
func droppedReads(was, now []string) []string {
	kept := make(map[string]struct{}, len(now))
	for _, k := range now {
		kept[k] = struct{}{}
	}
	var gone []string
	for _, k := range was {
		if _, ok := kept[k]; !ok {
			gone = append(gone, k)
		}
	}
	sort.Strings(gone)
	return gone
}

// quoteAll wraps each value in quotes so a message naming several reads reads
// as a list of names rather than as running text.
func quoteAll(vs []string) []string {
	out := make([]string, len(vs))
	for i, v := range vs {
		out[i] = fmt.Sprintf("%q", v)
	}
	return out
}

// hasReadKey reports whether name appears among keys, compared verbatim.
func hasReadKey(keys []string, name string) bool {
	for _, k := range keys {
		if k == name {
			return true
		}
	}
	return false
}

// identityKey is the node id a contract entry declares, "<schema>.<table>",
// lowercased, the same form every trigger and release verdict names a node by.
// Folding case is what makes a re-spelling of a name show up as a mutated entry
// rather than as one node deleted and an unrelated one added.
func identityKey(id ports.NodeIdentity) string {
	return strings.ToLower(id.Schema + "." + id.Table)
}

// identityDelta renders the identity fields that differ between two versions of
// one entry, so a finding says which field moved instead of only that something
// did.
func identityDelta(was, now ports.NodeIdentity) string {
	fields := []struct {
		name     string
		was, now string
	}{
		{"schema", was.Schema, now.Schema},
		{"table", was.Table, now.Table},
		{"script", was.Script, now.Script},
		{"kind", was.Kind, now.Kind},
		{"owner", was.Owner, now.Owner},
		{"schedule", was.Schedule, now.Schedule},
		{"criticality", was.Criticality, now.Criticality},
		{"secret_ref", was.SecretRef, now.SecretRef},
	}
	changed := make([]string, 0, len(fields))
	for _, f := range fields {
		if f.was != f.now {
			changed = append(changed, fmt.Sprintf("%s %q -> %q", f.name, f.was, f.now))
		}
	}
	return strings.Join(changed, ", ")
}

// --- declared_once ---

// DeclaredOnce is the check `declared_once`: after the answer is applied, the
// node is still declared by exactly one contract file, inside the directory the
// fix packages.
type DeclaredOnce struct {
	locator ports.ContractLocator
}

var _ step.Check = (*DeclaredOnce)(nil)

// NewDeclaredOnce builds the declared_once step.
func NewDeclaredOnce(locator ports.ContractLocator) *DeclaredOnce {
	return &DeclaredOnce{locator: locator}
}

const nameDeclaredOnce workflow.StepName = "declared_once"

// Meta describes the step.
func (*DeclaredOnce) Meta() step.Meta {
	return step.Meta{
		Name: nameDeclaredOnce,
		Kind: step.KindCheck,
		Description: "Fails a draft after which the node is no longer declared by exactly one contract file, " +
			"or is declared in another directory than the one the fix packages.",
		Needs:  needsDraftAndFile,
		Stages: preflightOnly,
	}
}

// Check searches the patched checkout for the node again. The before and after
// comparison sees only the files the answer returned; this covers what it
// cannot: the node must still be declared, exactly once, and still inside the
// directory this fix packages. A node that is not declared once, or is declared
// elsewhere, is a not_declared_once finding. A search that fails is returned so
// the work is tried again. A draft with a path outside the checkout is never
// applied: it is reported as path_outside_contract_dir, as the path check does.
func (c *DeclaredOnce) Check(ctx context.Context, in step.CheckInput) (step.CheckOutput, error) {
	file, err := contractFileOf(in, nameDeclaredOnce)
	if err != nil {
		return step.CheckOutput{}, err
	}
	root, out, done, err := patchedRoot(ctx, in, nameDeclaredOnce, file)
	if done || err != nil {
		return out, err
	}
	nodeID := in.Failure.Target.NodeID
	relocated, err := c.locator.Locate(root, file.Schema, file.Table)
	switch {
	case errors.Is(err, ports.ErrNodeNotDeclared), errors.Is(err, ports.ErrAmbiguousDeclaration):
		return result(nameDeclaredOnce, []remediation.Finding{{
			Code: "not_declared_once",
			Message: fmt.Sprintf(
				"after applying the model's answer, %s is no longer declared by exactly one contract file (%s); "+
					"a fix may correct what a node declares but never change which node is declared", nodeID, err),
		}}), nil
	case err != nil:
		return step.CheckOutput{}, fmt.Errorf("re-locate contract for %s after applying the fix: %w", nodeID, err)
	case relocated.ContractDir != file.ContractDir:
		return result(nameDeclaredOnce, []remediation.Finding{{
			Code: "not_declared_once",
			Message: fmt.Sprintf(
				"the model's answer moved %s out of the contract directory %q into %q, so the fix would not be packaged with the node it repairs",
				nodeID, file.ContractDir, relocated.ContractDir),
		}}), nil
	}
	return result(nameDeclaredOnce, nil), nil
}

// patchedRoot returns the root of the pass's patched checkout for a check that
// goes on to read or package it. done is true when the check is already
// answered: the draft names a path outside the checkout, so nothing was
// applied, and out holds the path findings.
func patchedRoot(ctx context.Context, in step.CheckInput, name workflow.StepName, file remediation.ContractFile) (root string, out step.CheckOutput, done bool, err error) {
	if in.Scratch == nil {
		return "", step.CheckOutput{}, false, fmt.Errorf("%s: %w", name, errNoScratch)
	}
	root, _, err = in.Scratch.PatchedCheckout(ctx, proposedFiles(in.Draft))
	if errors.Is(err, step.ErrPathOutsideCheckout) {
		if found := pathFindings(file.ContractDir, in.Draft.Edits); len(found) > 0 {
			return "", result(name, found), true, nil
		}
	}
	if err != nil {
		return "", step.CheckOutput{}, false, err
	}
	return root, step.CheckOutput{}, false, nil
}
