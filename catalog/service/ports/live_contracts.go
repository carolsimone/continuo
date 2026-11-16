package ports

import (
	"context"
	"errors"
	"time"
)

// LiveIndexSchemaVersion is the version of the live folder's index layout
// this build writes and reads.
const LiveIndexSchemaVersion = 1

// ErrInvalidLiveFile means a tenant id or a unique id cannot name an object of
// the live contracts folder: it is empty, "." or "..", or holds a "/".
var ErrInvalidLiveFile = errors.New("the id cannot name a live contracts file")

// LiveIndex is the index of a tenant's live contracts folder: the promotion
// it was written for, the fingerprint of the publishing settings its files
// were rendered with, and one entry per file, sorted by unique id. A rewrite
// that puts files first writes an interim index, listing every file about to
// be put at version 0 with an empty contract hash, and writes the final index
// last, once every put and delete has succeeded. So an entry at version 0
// names a file whose bytes no index vouches for.
type LiveIndex struct {
	SchemaVersion  int
	TenantID       string
	PromotionSeq   int64
	ReleaseID      string
	WrittenAt      time.Time
	SettingsSHA256 string
	Files          []LiveIndexFile
}

// LiveIndexFile is one published file of the live folder: the node, the
// counter of its current contract and that contract's hash.
type LiveIndexFile struct {
	UniqueID     string
	Version      int64
	ContractHash string
}

// ErrLiveIndexUnreadable is the error of an index the store holds but cannot
// read: not JSON, of another schema version, too large or with a bad field.
// No rewrite can go on from it; deleting the index lets the next rewrite put
// every file again.
var ErrLiveIndexUnreadable = errors.New("the live contracts index cannot be read")

// LiveContractsStore writes a tenant's live contracts folder in object
// storage: one published contract file per unique id, and the folder's
// index.
type LiveContractsStore interface {
	// Put writes body as uniqueID's published contract file.
	Put(ctx context.Context, tenantID, uniqueID string, body []byte) error
	// Delete removes uniqueID's published contract file. A file that does not
	// exist is not an error.
	Delete(ctx context.Context, tenantID, uniqueID string) error
	// ReadIndex reads the tenant's index; found is false when the folder has
	// none. An index it cannot read is ErrLiveIndexUnreadable, naming the
	// index's key.
	ReadIndex(ctx context.Context, tenantID string) (idx LiveIndex, found bool, err error)
	// PutIndex writes the tenant's index.
	PutIndex(ctx context.Context, tenantID string, idx LiveIndex) error
}

// PublishedRenderer renders the published form of a contract as the bytes of
// its live file. The same document always renders the same bytes.
type PublishedRenderer interface {
	Render(doc map[string]any) ([]byte, error)
}
