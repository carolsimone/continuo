package ports

import (
	"context"

	"github.com/carolsimone/continuo/catalog/service/graphbackup"
)

// RecordWriter receives a tenant's backup: the header first, then every node,
// then every relationship.
type RecordWriter interface {
	WriteHeader(graphbackup.Header) error
	WriteNode(graphbackup.NodeRecord) error
	WriteRel(graphbackup.RelRecord) error
}

// RecordReader hands back a backup: its header, then each record in the
// order it was written, an graphbackup.NodeRecord or an graphbackup.RelRecord, and
// io.EOF after the last.
type RecordReader interface {
	Header() graphbackup.Header
	Next() (any, error)
}

// GraphSaver reads a tenant's graph out of the catalog. It is called while
// the tenant's backup lease is held, so no write changes the graph under it.
type GraphSaver interface {
	// Save writes the tenant's header, then streams its nodes and its
	// relationships to out. It never holds the whole graph in memory. The
	// tenant's backup lease node is not backed up, and no node of another
	// tenant is.
	Save(ctx context.Context, tenantID string, out RecordWriter) error
}

// GraphLoader loads a backup back into the catalog. Both methods are
// called while the tenant's backup lease is held for the restore.
type GraphLoader interface {
	// Wipe deletes every node of the tenant, with its relationships, except
	// the tenant's backup lease node.
	Wipe(ctx context.Context, tenantID string) error
	// Load loads in into the tenant's wiped graph: nodes, then
	// relationships. It refuses a record of another tenant, of a label or
	// type outside the catalog's schema, or counts that disagree with in's
	// header; a backup lease node in the backup is not loaded.
	Load(ctx context.Context, tenantID string, in RecordReader) error
}

// GraphTenants lists the tenants the catalog holds a graph for.
type GraphTenants interface {
	Tenants(ctx context.Context) ([]string, error)
}
