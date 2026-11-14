import type { ServicesResponse } from './types';

// fetchNodeServices returns the services of the nodes the Nodes tab lists,
// the live release's nodes continuo builds, a service whose nodes never ran
// included. It feeds the Service filters of the Nodes and Remediation tabs.
export function fetchNodeServices(): Promise<string[]> {
  return fetch('/api/nodes/services')
    .then(r => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
    .then((data: ServicesResponse) => data.services ?? []);
}
