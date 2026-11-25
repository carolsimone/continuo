import type { ProjectsResponse } from './types';

// fetchNodeProjects returns the projects of the nodes the Nodes tab lists,
// the live release's nodes continuo builds, a project whose nodes never ran
// included. It feeds the Project filters of the Nodes and Remediation tabs.
export function fetchNodeProjects(): Promise<string[]> {
  return fetch('/api/nodes/projects')
    .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
    .then((data: ProjectsResponse) => data.projects ?? []);
}
