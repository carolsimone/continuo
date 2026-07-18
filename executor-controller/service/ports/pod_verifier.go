package ports

import "context"

// PodVerifier confirms a worker pod's identity against the live cluster before a
// lease binds to it.
//
// A worker names its own pod through the downward API and sends that name and
// UID when it claims. The executor trusts it only once it has confirmed the pod
// is real, belongs to the authenticated pool, and carries the UID the worker
// claimed. That identity is the one cancellation and lease-expiry reaping later
// delete by, so binding an unverified name would let a caller aim a delete at a
// pod it does not own, or omit its identity so its own process could never be
// fenced.
type PodVerifier interface {
	// VerifyPod returns nil when podName/podUID is a pod of poolKey: it exists in
	// the pool's namespace, its UID matches, and it carries the pool's worker
	// labels. It returns an error otherwise — a blank identity, a missing pod, a
	// UID mismatch, or a pod of another pool are all rejected.
	VerifyPod(ctx context.Context, poolKey, podName, podUID string) error
}
