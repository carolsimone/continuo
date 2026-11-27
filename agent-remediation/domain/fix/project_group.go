package fix

import (
	"sort"
	"strings"
)

// ProjectForPath resolves which configured project owns a repository path, and
// returns that project's root within the repo. The owner is the project
// whose root is the longest prefix of the path, so a repo where one project's
// root nests inside another's still resolves to the nearest one. A project
// mapped to the repository root itself ("") owns any path no other project
// claims. Ties between equally specific roots go to the smaller project name, so
// the answer does not depend on map iteration order.
func ProjectForPath(projectRepoPaths map[string]string, filePath string) (project, prefix string, ok bool) {
	best := -1
	for name, root := range projectRepoPaths {
		if root != "" && !strings.HasPrefix(filePath, root+"/") {
			continue
		}
		if len(root) < best || (len(root) == best && name >= project) {
			continue
		}
		project, prefix, best = name, root, len(root)
	}
	return project, prefix, best >= 0
}

// GroupEditsByProject buckets edits by the project owning each path per
// ProjectForPath. A path no project claims — or every path when the map is
// empty — lands under the legacy "" key, which downstream code treats as
// "one PR for the whole fix".
func GroupEditsByProject(projectRepoPaths map[string]string, edits []FileEdit) map[string][]FileEdit {
	groups := make(map[string][]FileEdit)
	for _, e := range edits {
		project, _, ok := ProjectForPath(projectRepoPaths, e.Path)
		if !ok {
			project = ""
		}
		groups[project] = append(groups[project], e)
	}
	return groups
}

// IntersectSorted returns the sorted intersection of members and fixed: the
// members an attempt actually repaired. A per-project PR claims only the nodes
// that project's edits address AND the attempt fixed, so a member the attempt
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
