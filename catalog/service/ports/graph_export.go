package ports

import (
	"context"

	"github.com/carolsimone/continuo/catalog/domain/export"
)

// RecordWriter receives a tenant's export: the header first, then every node,
// then every relationship.
type RecordWriter interface {
	WriteHeader(export.Header) error
	WriteNode(export.NodeRecord) error
	WriteRel(export.RelRecord) error
}

// RecordReader hands back an export: its header, then each record in the
// order it was written, an export.NodeRecord or an export.RelRecord, and
// io.EOF after the last.
type RecordReader interface {
	Header() export.Header
	Next() (any, error)
}

// GraphExporter reads a tenant's graph out of the catalog. It is called while
// the tenant's export lease is held, so no write changes the graph under it.
type GraphExporter interface {
	// Export writes the tenant's header, then streams its nodes and its
	// relationships to out. It never holds the whole graph in memory. The
	// tenant's export lease node is not exported, and no node of another
	// tenant is.
	Export(ctx context.Context, tenantID string, out RecordWriter) error
}

// GraphImporter loads an export back into the catalog. Both methods are
// called while the tenant's export lease is held for the restore.
type GraphImporter interface {
	// Wipe deletes every node of the tenant, with its relationships, except
	// the tenant's export lease node.
	Wipe(ctx context.Context, tenantID string) error
	// Import loads in into the tenant's wiped graph: nodes, then
	// relationships. It refuses a record of another tenant, of a label or
	// type outside the catalog's schema, or counts that disagree with in's
	// header; an export lease node in the export is not loaded.
	Import(ctx context.Context, tenantID string, in RecordReader) error
}

// GraphTenants lists the tenants the catalog holds a graph for.
type GraphTenants interface {
	Tenants(ctx context.Context) ([]string, error)
}
