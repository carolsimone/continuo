package handlers

import (
	"context"
	"fmt"
	"strings"
	"time"

	pkg_model "github.com/carolsimone/continuo/pkg/domain/model"
	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// SeedServiceProd derives one service_prod pointer per distinct project in the
// production topology topo of release releaseID and upserts it. The manifest S3
// key for each project is taken verbatim from existingKeys (project_name -> s3
// key); a project present in topo but absent from existingKeys is an error (we
// cannot point at a manifest we do not know). image_tag is the first non-empty tag among the
// project's nodes. manifest_kind is derived from the project's own nodes: any
// node whose NodeType.IsPython() is true (python-node, python-csv or python-api)
// makes the project a python project, otherwise ManifestKindDbt; a project whose
// nodes mix python and non-python types is an error (a project is either a
// dbt project or a python project, never both). A python project is
// ManifestKindPython, and its manifest key must name an odcs-bundle.json:
// a python project whose key names anything else is an error. Idempotent:
// re-running upserts the same rows.
// Returns the count of projects seeded.
func SeedServiceProd(
	ctx context.Context,
	releaseID string,
	topo release.Topology,
	existingKeys map[string]string,
	repo repository.ServiceProdRepository,
	now time.Time,
) (int, error) {
	// Group nodes by project name; track the first non-empty image tag and
	// which node kinds (dbt vs python) were seen, per project.
	type svcEntry struct {
		imageTag  string
		hasDbt    bool
		hasPython bool
	}
	entries := map[string]*svcEntry{}
	orderSeen := []string{} // deterministic iteration order

	for _, node := range topo {
		if node.ProjectName == "" {
			continue
		}
		if _, exists := entries[node.ProjectName]; !exists {
			entries[node.ProjectName] = &svcEntry{}
			orderSeen = append(orderSeen, node.ProjectName)
		}
		e := entries[node.ProjectName]
		if e.imageTag == "" && node.ImageTag != "" {
			e.imageTag = node.ImageTag
		}
		if pkg_model.NodeType(node.NodeType).IsPython() {
			e.hasPython = true
		} else {
			e.hasDbt = true
		}
	}

	// Validate every project has a manifest key, an unambiguous kind and, for a
	// python project, an odcs-bundle.json key BEFORE writing anything, so any
	// failure aborts the whole seed atomically rather than leaving a partial
	// set of pointers behind.
	for _, project := range orderSeen {
		if _, ok := existingKeys[project]; !ok {
			return 0, fmt.Errorf("no manifest S3 key provided for project %q", project)
		}
		if entries[project].hasDbt && entries[project].hasPython {
			return 0, fmt.Errorf("project %q has both dbt and python nodes; a project must be a single kind", project)
		}
		if entries[project].hasPython && !strings.HasSuffix(existingKeys[project], "/"+odcsBundleName) {
			return 0, fmt.Errorf("python project %q has manifest key %q, which is not an %s; release it as kind python (one ODCS contract per node) and seed from that release",
				project, existingKeys[project], odcsBundleName)
		}
	}

	for _, project := range orderSeen {
		kind := release.ManifestKindDbt
		if entries[project].hasPython {
			kind = release.ManifestKindPython
		}
		sp := release.NewServiceProd(project, releaseID, existingKeys[project], entries[project].imageTag, kind, now)
		if err := repo.Upsert(ctx, sp); err != nil {
			return 0, fmt.Errorf("upsert service_prod for %q: %w", project, err)
		}
	}

	return len(orderSeen), nil
}
