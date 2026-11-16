package s3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/carolsimone/continuo/catalog/domain/contractversion"
	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/pkg/contractbundle"
	"github.com/carolsimone/continuo/pkg/objectstore"
)

// ContractBundleReader implements ports.ContractBundleReader over an
// object-store reader.
type ContractBundleReader struct {
	objects objectstore.Reader
}

// Compile-time assertion that the adapter satisfies the application port.
var _ ports.ContractBundleReader = (*ContractBundleReader)(nil)

// NewContractBundleReader builds a ContractBundleReader over objects. A bundle
// URI without a bucket reads the store's default bucket.
func NewContractBundleReader(objects objectstore.Reader) *ContractBundleReader {
	return &ContractBundleReader{objects: objects}
}

// Fetch reads the contract bundle at uri, verifies it against sha256Hex and
// returns its contracts sorted by unique id. The store's outcomes map onto the
// port's errors: a missing object is ports.ErrContractBundleNotFound
// (retried); a uri that names no object, an object over
// contractbundle.MaxObjectBytes, a checksum mismatch, an undecodable or
// unsupported document, a document with no tenant_id and a contract catalog
// cannot version are ports.ErrContractBundleInvalid (permanent); any other
// error keeps its cause and wraps neither, so the consumer's classifier
// recognises an outage.
//
// A contract is refused when its hash is empty or its document is not an
// object whose id equals the unique id the bundle keys it by: the bundle holds
// one contract per unique id, and catalog identifies a version by the pair.
func (r *ContractBundleReader) Fetch(ctx context.Context, uri, sha256Hex string) (ports.ContractBundle, error) {
	ref, err := objectstore.ParseRef(uri)
	if err != nil {
		return ports.ContractBundle{}, fmt.Errorf("%w: %w", ports.ErrContractBundleInvalid, err)
	}
	body, err := r.objects.Get(ctx, ref, contractbundle.MaxObjectBytes)
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return ports.ContractBundle{}, fmt.Errorf("%w: %s: %w", ports.ErrContractBundleNotFound, uri, err)
	case errors.Is(err, objectstore.ErrTooLarge):
		return ports.ContractBundle{}, fmt.Errorf("%w: %s: %w", ports.ErrContractBundleInvalid, uri, err)
	case err != nil:
		return ports.ContractBundle{}, fmt.Errorf("get contract bundle %s: %w", uri, err)
	}

	doc, err := contractbundle.Decode(body, sha256Hex)
	if err != nil {
		return ports.ContractBundle{}, fmt.Errorf("%w: %s: %w", ports.ErrContractBundleInvalid, uri, err)
	}
	if strings.TrimSpace(doc.TenantID) == "" {
		return ports.ContractBundle{}, fmt.Errorf("%w: %s: the bundle has no tenant_id", ports.ErrContractBundleInvalid, uri)
	}
	contracts, err := incomingContracts(doc.Contracts)
	if err != nil {
		return ports.ContractBundle{}, fmt.Errorf("%w: %s: %w", ports.ErrContractBundleInvalid, uri, err)
	}
	return ports.ContractBundle{
		TenantID:          doc.TenantID,
		ReleaseID:         doc.ReleaseID,
		ContinuoContracts: doc.ContinuoContracts,
		Contracts:         contracts,
	}, nil
}

// incomingContracts turns the bundle's contracts into one Incoming each,
// sorted by unique id, decoding each stored document to a map. It refuses the
// first contract, in unique-id order, that has no hash or whose document is not
// an object carrying its unique id as `id`.
func incomingContracts(contracts map[string]contractbundle.Contract) ([]contractversion.Incoming, error) {
	ids := make([]string, 0, len(contracts))
	for id := range contracts {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	incoming := make([]contractversion.Incoming, 0, len(ids))
	for _, id := range ids {
		c := contracts[id]
		if strings.TrimSpace(c.ContractHash) == "" {
			return nil, fmt.Errorf("contract %q has no contract_hash", id)
		}
		document, err := decodeContractDocument(c.Document)
		if err != nil {
			return nil, fmt.Errorf("contract %q: %w", id, err)
		}
		if documentID, _ := document["id"].(string); documentID != id {
			return nil, fmt.Errorf("contract %q holds a document whose id is %v", id, document["id"])
		}
		incoming = append(incoming, contractversion.Incoming{
			UniqueID:     id,
			ContractHash: c.ContractHash,
			ContentHash:  c.ContentHash,
			Family:       c.Family,
			Service:      c.Service,
			SourcePath:   c.Source.Path,
			Document:     document,
		})
	}
	return incoming, nil
}

// decodeContractDocument decodes a stored contract to a map: objects as
// map[string]any, arrays as []any, numbers as json.Number, the text the bundle
// holds, so a number keeps the exact value the contract hash was taken over.
// A document that is not a JSON object is an error.
func decodeContractDocument(raw json.RawMessage) (map[string]any, error) {
	var document map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&document); err != nil {
		return nil, fmt.Errorf("the document is not a JSON object: %w", err)
	}
	if document == nil {
		return nil, errors.New("the document is not a JSON object")
	}
	return document, nil
}
