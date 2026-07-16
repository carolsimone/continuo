package handlers

import (
	"context"
	"fmt"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/release"
	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// SeedServiceProd derives one service_prod pointer per distinct service in the
// current_prod snapshot and upserts it. The manifest S3 key for each service is
// taken verbatim from existingKeys (service_name -> s3 key); a service present
// in the snapshot but absent from existingKeys is an error (we cannot point at
// a manifest we do not know). image_tag is the first non-empty tag among the
// service's nodes. The runtime manifest reference is derived from the
// snapshot's own nodes via runtimeManifestForService, so a snapshot carrying
// pinned artifacts seeds pointers that keep those pins rather than reverting
// every service to the legacy in-Job parse path; a service whose nodes
// disagree on the reference fails the whole seed rather than silently seeding
// a zero ref. Idempotent: re-running upserts the same rows.
// Returns the count of services seeded.
func SeedServiceProd(
	ctx context.Context,
	cp *release.CurrentProd,
	existingKeys map[string]string,
	repo repository.ServiceProdRepository,
	now time.Time,
) (int, error) {
	// Group nodes by service name; track the first non-empty image tag per service.
	type svcEntry struct {
		imageTag string
	}
	entries := map[string]*svcEntry{}
	orderSeen := []string{} // deterministic iteration order

	for _, node := range cp.TopologySnapshot() {
		if node.ServiceName == "" {
			continue
		}
		if _, exists := entries[node.ServiceName]; !exists {
			entries[node.ServiceName] = &svcEntry{}
			orderSeen = append(orderSeen, node.ServiceName)
		}
		if entries[node.ServiceName].imageTag == "" && node.ImageTag != "" {
			entries[node.ServiceName].imageTag = node.ImageTag
		}
	}

	// Validate every service has a manifest key BEFORE writing anything, so a
	// missing key fails the whole seed atomically rather than leaving a partial
	// set of pointers behind.
	for _, svc := range orderSeen {
		if _, ok := existingKeys[svc]; !ok {
			return 0, fmt.Errorf("no manifest S3 key provided for service %q", svc)
		}
	}

	topo := cp.TopologySnapshot()
	for _, svc := range orderSeen {
		ref, err := runtimeManifestForService(topo, svc)
		if err != nil {
			return 0, fmt.Errorf("resolve runtime manifest for service %q: %w", svc, err)
		}
		sp := release.NewServiceProdWithRuntime(svc, cp.ReleaseID(), existingKeys[svc], entries[svc].imageTag, ref, now)
		if err := repo.Upsert(ctx, sp); err != nil {
			return 0, fmt.Errorf("upsert service_prod for %q: %w", svc, err)
		}
	}

	return len(orderSeen), nil
}
