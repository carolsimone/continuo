package fixer

import (
	"context"
	"errors"
	"fmt"
	"path"
	"strings"

	"github.com/carolsimone/continuo/agent-remediation/service/ports"
)

// gatherSourceFile reads the offending file named by in.FilePath under the
// repo prefix of service and, when it is a .sql, its co-located yml siblings
// and the service's dbt_project.yml as best-effort context. It serves every
// lane whose fix is one source file the model picks from what it is shown.
// A missing prefix or a 404 on the offending file is a skip, returned as its
// reason; any other read error is transient and returned so the driver
// redelivers.
func gatherSourceFile(ctx context.Context, svc Services, in Input, service string) (Gathered, string, error) {
	g, skipReason, err := readOffendingFile(ctx, svc, in, service, in.FilePath)
	if err != nil || skipReason != "" {
		return Gathered{}, skipReason, err
	}
	offending := g.Primary
	prefix := svc.ServiceRepoPaths[service]

	// Best-effort extra context, only when the offending file is a .sql.
	if strings.HasSuffix(offending, ".sql") {
		dir := path.Dir(offending)
		if paths, derr := svc.Source.ListDir(ctx, in.Repo, in.CommitSHA, dir); derr == nil {
			for _, p := range paths {
				if p == offending {
					continue
				}
				if strings.HasSuffix(p, ".yml") || strings.HasSuffix(p, ".yaml") {
					addFile(ctx, svc, in, &g, p)
				}
			}
		}
		addFile(ctx, svc, in, &g, path.Join(prefix, "dbt_project.yml"))
	}
	return g, "", nil
}

// readOffendingFile resolves filePath under service's repository prefix and
// reads it at the trigger's commit, returning it as a one-file Gathered. It is
// the read every source-file lane starts from; the compile and parse lanes
// add best-effort context on top of it. A missing prefix or a 404 is a skip,
// returned as its reason; any other read error is transient and returned so
// the driver redelivers.
func readOffendingFile(ctx context.Context, svc Services, in Input, service, filePath string) (Gathered, string, error) {
	prefix, ok := svc.ServiceRepoPaths[service]
	if !ok {
		return Gathered{}, fmt.Sprintf("service %q has no repository path mapping, so its source cannot be read", service), nil
	}
	offending := path.Join(prefix, filePath)
	content, err := svc.Source.ReadFile(ctx, in.Repo, in.CommitSHA, offending)
	if err != nil {
		if errors.Is(err, ports.ErrSourceNotFound) {
			return Gathered{}, fmt.Sprintf("the offending file %s does not exist at commit %s", offending, in.CommitSHA), nil
		}
		return Gathered{}, "", err // transient: redeliver
	}
	return Gathered{Files: map[string]string{offending: content}, Order: []string{offending}, Primary: offending}, "", nil
}

// addFile reads one best-effort context file, ignoring a not-found result.
func addFile(ctx context.Context, svc Services, in Input, g *Gathered, p string) {
	if _, seen := g.Files[p]; seen {
		return
	}
	c, err := svc.Source.ReadFile(ctx, in.Repo, in.CommitSHA, p)
	if err != nil {
		return // context is optional; skip on any error
	}
	g.Files[p] = c
	g.Order = append(g.Order, p)
}
