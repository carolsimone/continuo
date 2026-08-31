package streams_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// copyInstruction matches a Dockerfile COPY instruction, with or without flags.
// Continuation lines and comments never start with COPY, so a line-leading
// match is the whole instruction set.
var copyInstruction = regexp.MustCompile(`(?i)^\s*COPY\s`)

// TestDbtServiceImagesCopyAsTheDbtUser asserts every COPY in every in-repo dbt
// service image carries --chown=dbt:dbt.
//
// The base image (dbt/base/Dockerfile) creates the non-root `dbt` user and
// chowns /project, but that only covers what the base stage itself copied: a
// plain COPY in a derived image writes root-owned files regardless of USER,
// leaving the project unwritable by the user the container actually runs as.
//
// A shadow release verifying a proposed fix lays its source overlay over the
// checked-in project with `cp -R /shared/overlay/. ./` inside the team image.
// Against a root-owned project that copy fails with EACCES and every dbt fix
// verification fails — a failure that appears only on the remediation path, so
// a root-owned project would ship unnoticed until a fix is proposed.
func TestDbtServiceImagesCopyAsTheDbtUser(t *testing.T) {
	root := repoRootFromTest(t)

	dockerfiles, err := filepath.Glob(filepath.Join(root, "dbt", "services", "*", "Dockerfile*"))
	if err != nil {
		t.Fatal(err)
	}
	if len(dockerfiles) == 0 {
		t.Fatalf("no dbt service Dockerfiles found under %s/dbt/services", root)
	}

	checked := 0
	for _, file := range dockerfiles {
		content, err := os.ReadFile(file) //nolint:gosec // repo-relative path from a glob
		if err != nil {
			t.Fatal(err)
		}
		rel, err := filepath.Rel(root, file)
		if err != nil {
			rel = file
		}
		for i, line := range strings.Split(string(content), "\n") {
			if !copyInstruction.MatchString(line) {
				continue
			}
			checked++
			if !strings.Contains(line, "--chown=dbt:dbt") {
				t.Errorf("%s:%d: COPY must carry --chown=dbt:dbt so the team image's own "+
					"user owns the project and a shadow release's source overlay can be "+
					"copied over it:\n\t%s", rel, i+1, strings.TrimSpace(line))
			}
		}
	}
	if checked == 0 {
		t.Fatal("no COPY instructions found in the dbt service Dockerfiles: the guard checks nothing")
	}
}
