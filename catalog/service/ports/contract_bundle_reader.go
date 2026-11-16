package ports

import (
	"context"
	"errors"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
)

// ErrContractBundleNotFound means the contract bundle a promotion names is
// absent from object storage. The caller retries: an object that is merely
// late becomes readable, and one that never appears exhausts the delivery
// budget and is dead-lettered.
var ErrContractBundleNotFound = errors.New("contract bundle not found")

// ErrContractBundleInvalid means the promotion does not name a readable
// contract bundle: its URI names no object, the object exceeds the size a
// reader accepts, its SHA-256 differs from the event's, it is not a document
// of a schema version this build reads, it names no tenant, or one of its
// contracts is not one catalog can version. Re-reading the same object cannot help, so the caller
// fails the message permanently.
var ErrContractBundleInvalid = errors.New("contract bundle invalid")

// ContractBundle is what a release's contract bundle holds for catalog: the
// tenant and release it belongs to, the continuo-contracts version that wrote
// it, and one Incoming per contract, sorted by unique id.
type ContractBundle struct {
	TenantID          string
	ReleaseID         string
	ContinuoContracts string
	Contracts         []contractversion.Incoming
}

// ContractBundleReader reads a release's contract bundle and verifies it
// against the SHA-256 its promotion carried.
type ContractBundleReader interface {
	Fetch(ctx context.Context, uri, sha256 string) (ContractBundle, error)
}
