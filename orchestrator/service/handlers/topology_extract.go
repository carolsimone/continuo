package handlers

import "sort"

// scheduleAndMetadataFromNodes derives the sorted-unique schedule-names slice
// and the per-service metadata map from a generic node slice. Empty schedule
// names are filtered out. For per-service metadata, the FIRST node seen for a
// given service_name wins — subsequent conflicting image_tag or manifest
// version values are silently ignored (the caller may log a warning for
// conflicts, keeping this helper pure and logging-free).
//
// Generic over node type T so both event.ManifestLoadedNode and
// event.ReleasePromotedNode slices flow through one code path; keeps the
// schedules.loaded:v1 outbox emission byte-shape identical between the
// manifest.loaded:v1 and release.promoted:v1 consumers.
//
// The accessor func returns (schedule, service, imageTag, manifestVersion).
func scheduleAndMetadataFromNodes[T any](
	nodes []T,
	accessor func(T) (schedule, service, imageTag, manifestVersion string),
) ([]string, map[string]map[string]string) {
	schedSet := make(map[string]struct{})
	metadata := make(map[string]map[string]string)

	for _, n := range nodes {
		sched, svc, tag, mv := accessor(n)
		if sched != "" {
			schedSet[sched] = struct{}{}
		}
		if svc != "" {
			if _, seen := metadata[svc]; !seen {
				metadata[svc] = map[string]string{
					"manifest_version": mv,
					"image_tag":        tag,
				}
			}
		}
	}

	schedules := make([]string, 0, len(schedSet))
	for k := range schedSet {
		schedules = append(schedules, k)
	}
	sort.Strings(schedules)

	return schedules, metadata
}
