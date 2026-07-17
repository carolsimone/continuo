package ports

import (
	"context"

	pkgmodel "github.com/carolsimone/continuo/pkg/domain/model"
)

// WorkerPoolSpec is the pool of reusable workers the reconciler wants to exist.
// It describes the desired state in full, so Ensure never has to read back what
// it wrote to know what to do.
type WorkerPoolSpec struct {
	PoolKey     string
	ServiceName string
	ImageTag    string
	// RuntimeManifest is the prebuilt artifact this pool's workers execute
	// against. Its digest travels to the pods, which reject any descriptor that
	// does not match it.
	RuntimeManifest pkgmodel.RuntimeManifestRef
	// ControllerContextJSON is the canonical parse context the controller
	// resolved for the pool's service.
	ControllerContextJSON string
	// Credential is the raw pool credential to place in the pool's Secret.
	//
	// Empty means the Secret already holds the pool's credential and must be
	// left exactly as it is. A credential is never read back out of the runtime,
	// so a pool whose Secret is intact is reconciled without one ever being in
	// hand — only a new pool, or one whose Secret went missing, carries a value
	// here.
	Credential      string
	DesiredReplicas int32
}

// PoolStatus is what the cluster currently holds for one pool.
type PoolStatus struct {
	// DesiredReplicas is the replica count the pool's Deployment asks for, and
	// ReadyReplicas how many of its pods are serving.
	DesiredReplicas int
	ReadyReplicas   int
	// SecretExists reports whether the pool's credential Secret is present. It
	// is reported independently of the Deployment, because a Secret deleted out
	// from under a live pool is exactly the case that must be repaired: the
	// stored digest can no longer be matched by any credential anyone holds.
	SecretExists bool
}

// WorkerPoolRuntime is where worker pools actually run.
type WorkerPoolRuntime interface {
	// Ensure brings the pool's resources to spec, creating them when absent and
	// updating them when they differ. It is idempotent: reconciling an unchanged
	// pool changes nothing.
	Ensure(ctx context.Context, spec WorkerPoolSpec) error
	// Status reports what the cluster holds for poolKey. The bool is false when
	// the pool has no Deployment, which is not an error: a pool registered but
	// never yet reconciled, and one whose Deployment an operator removed, both
	// answer this way.
	Status(ctx context.Context, poolKey string) (PoolStatus, bool, error)
	// PodTerminator stops a single pod of a pool this runtime runs.
	PodTerminator
}
