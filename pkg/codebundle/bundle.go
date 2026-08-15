// Package codebundle is the shared reader for the code-bundle contract
// document manifest-controller writes once per release
// (code-bundles/<release_id>/bundle.json).
//
// The bundle exists so no consumer outside manifest-controller ever parses a dbt
// manifest: its shape is continuo-owned and versioned, and it is runtime-neutral
// — every node declares a runtime, and shared code units are plain ids plus
// source, whether they are dbt macros or Python modules.
package codebundle

import (
	"encoding/json"
	"fmt"
)

// SupportedContractVersion is the only bundle contract_version this build can
// read. A newer bundle is rejected rather than partially interpreted.
const SupportedContractVersion = 1

// Node is one node's entry in the bundle, keyed in Bundle.Nodes by its continuo
// unique_id ("<schema>.<table>", lowercased).
type Node struct {
	Runtime        string         `json:"runtime"`
	RawCode        string         `json:"raw_code"`
	CompiledCode   string         `json:"compiled_code"`
	Config         map[string]any `json:"config"`
	SourceHash     string         `json:"source_hash"`
	SharedCodeHash string         `json:"shared_code_hash"`
	ConfigHash     string         `json:"config_hash"`
	ContentHash    string         `json:"content_hash"`
	// CodeUnitIDs are the node's DIRECT shared-code dependencies. The full set
	// in scope is their closure through SharedCodeUnit.DependsOn.
	CodeUnitIDs []string `json:"code_unit_ids"`
}

// SharedCodeUnit is one shared-code unit's entry, keyed in Bundle.SharedCode by
// its service-namespaced unit id ("<service>:<unit id>"). The namespace keeps
// two services' copies of a same-named unit distinct.
type SharedCodeUnit struct {
	Source    string   `json:"source"`
	Checksum  string   `json:"checksum"`
	DependsOn []string `json:"depends_on"`
}

// Bundle is the whole contract document for one release.
type Bundle struct {
	ContractVersion int                       `json:"contract_version"`
	ReleaseID       string                    `json:"release_id"`
	Nodes           map[string]Node           `json:"nodes"`
	SharedCode      map[string]SharedCodeUnit `json:"shared_code"`
}

// Decode parses and validates a bundle document. Every error it returns means
// the object can never be interpreted, however often it is re-read — the caller
// maps them to a permanent, acknowledge-and-drop failure.
func Decode(raw []byte) (Bundle, error) {
	var b Bundle
	if err := json.Unmarshal(raw, &b); err != nil {
		return Bundle{}, fmt.Errorf("decode code bundle: %w", err)
	}
	if b.ContractVersion != SupportedContractVersion {
		return Bundle{}, fmt.Errorf("unsupported code bundle contract_version %d (this build reads %d)",
			b.ContractVersion, SupportedContractVersion)
	}
	for uniqueID, n := range b.Nodes {
		// content_hash is the whole write predicate: a node without one cannot be
		// compared against the graph, and skipping it silently would drop that
		// node's history for good.
		if n.ContentHash == "" {
			return Bundle{}, fmt.Errorf("code bundle node %q has empty content_hash", uniqueID)
		}
	}
	return b, nil
}
