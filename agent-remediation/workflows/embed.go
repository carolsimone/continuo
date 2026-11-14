// Package workflows holds continuo's built-in remediation files: the flows, the
// types they list, and the conformance fixtures every listed type and the flow
// are tested with. They are built into the binary.
package workflows

import "embed"

// Builtin is the built-in files, with the flows under flows/, the types under
// types/ and the fixtures under fixtures/. Every file names its kind, so a
// loader needs no layout.
//
//go:embed flows types fixtures
var Builtin embed.FS
