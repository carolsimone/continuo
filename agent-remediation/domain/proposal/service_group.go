package proposal

import (
	"sort"
	"strings"
)

// ServiceForPath resolves which configured service owns a repository path, and
// returns that service's project root within the repo. The owner is the service
// whose root is the longest prefix of the path, so a repo where one service's
// root nests inside another's still resolves to the nearest one. A service
// mapped to the repository root itself ("") owns any path no other service
// claims. Ties between equally specific roots go to the smaller service name, so
// the answer does not depend on map iteration order.
func ServiceForPath(serviceRepoPaths map[string]string, filePath string) (service, prefix string, ok bool) {
	best := -1
	for name, root := range serviceRepoPaths {
		if root != "" && !strings.HasPrefix(filePath, root+"/") {
			continue
		}
		if len(root) < best || (len(root) == best && name >= service) {
			continue
		}
		service, prefix, best = name, root, len(root)
	}
	return service, prefix, best >= 0
}

// GroupEditsByService buckets edits by the service owning each path per
// ServiceForPath. A path no service claims — or every path when the map is
// empty — lands under the legacy "" key, which downstream code treats as
// "one PR for the whole proposal".
func GroupEditsByService(serviceRepoPaths map[string]string, edits []FileEdit) map[string][]FileEdit {
	groups := make(map[string][]FileEdit)
	for _, e := range edits {
		service, _, ok := ServiceForPath(serviceRepoPaths, e.Path)
		if !ok {
			service = ""
		}
		groups[service] = append(groups[service], e)
	}
	return groups
}

// IntersectSorted returns the sorted intersection of members and fixed: the
// members an attempt actually repaired. A per-service PR claims only the nodes
// that service's edits address AND the attempt fixed, so a member the attempt
// skipped or failed is never claimed by a PR. Both inputs are treated as sets;
// the result is sorted and free of duplicates.
func IntersectSorted(members, fixed []string) []string {
	want := make(map[string]struct{}, len(fixed))
	for _, f := range fixed {
		want[f] = struct{}{}
	}
	seen := make(map[string]struct{}, len(members))
	out := make([]string, 0, len(members))
	for _, m := range members {
		if _, ok := want[m]; !ok {
			continue
		}
		if _, dup := seen[m]; dup {
			continue
		}
		seen[m] = struct{}{}
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

// MembersOfEdits is the sorted union of the edits' MemberNodeIDs. When no
// edit carries members (rows written before the split), fallback is returned
// so legacy single-PR flows keep resolving the whole set.
func MembersOfEdits(edits []FileEdit, fallback []string) []string {
	set := map[string]struct{}{}
	for _, e := range edits {
		for _, m := range e.MemberNodeIDs {
			set[m] = struct{}{}
		}
	}
	if len(set) == 0 {
		return fallback
	}
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}
