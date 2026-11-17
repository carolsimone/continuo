package s3

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/carolsimone/continuo/catalog/service/ports"
	"github.com/carolsimone/continuo/pkg/objectstore"
)

// A tenant's live contracts folder holds one published contract per live
// node and the folder's index:
//
//	tenants/<tenant>/contracts/live/<unique_id>.odcs.yaml
//	tenants/<tenant>/contracts/live/_index.json
const (
	liveKeyRoot        = "tenants/"
	liveKeyDir         = "/contracts/live/"
	liveFileSuffix     = ".odcs.yaml"
	liveIndexName      = "_index.json"
	liveFileType       = "application/yaml"
	liveIndexType      = "application/json"
	maxLiveIndexBytes  = 64 << 20
	liveIndexTimeStamp = time.RFC3339
)

// liveObjects is the part of an object store the live folder uses: reading
// the index, writing objects and deleting one.
type liveObjects interface {
	objectstore.Reader
	objectstore.Writer
	objectstore.Deleter
}

// LiveContractsStore implements ports.LiveContractsStore in the object
// store's default bucket, at the layout above.
type LiveContractsStore struct {
	objects liveObjects
}

var _ ports.LiveContractsStore = (*LiveContractsStore)(nil)

// NewLiveContractsStore builds a LiveContractsStore over objects.
func NewLiveContractsStore(objects liveObjects) *LiveContractsStore {
	return &LiveContractsStore{objects: objects}
}

// liveIndexDTO is the JSON shape of _index.json.
type liveIndexDTO struct {
	SchemaVersion  int                `json:"schema_version"`
	TenantID       string             `json:"tenant_id"`
	PromotionSeq   int64              `json:"promotion_seq"`
	ReleaseID      string             `json:"release_id"`
	WrittenAt      string             `json:"written_at"`
	SettingsSHA256 string             `json:"settings_sha256"`
	Files          []liveIndexFileDTO `json:"files"`
}

// liveIndexFileDTO is one file of the index; key is the file's name within
// the folder.
type liveIndexFileDTO struct {
	UniqueID     string `json:"unique_id"`
	Key          string `json:"key"`
	Version      int64  `json:"version"`
	ContractHash string `json:"contract_hash"`
}

// Put writes body as uniqueID's published contract file.
func (s *LiveContractsStore) Put(ctx context.Context, tenantID, uniqueID string, body []byte) error {
	ref, err := liveFileRef(tenantID, uniqueID)
	if err != nil {
		return err
	}
	if err := s.objects.Put(ctx, ref, body, liveFileType); err != nil {
		return fmt.Errorf("put the live contract %s: %w", ref.Key, err)
	}
	return nil
}

// Delete removes uniqueID's published contract file; a missing file is no
// error.
func (s *LiveContractsStore) Delete(ctx context.Context, tenantID, uniqueID string) error {
	ref, err := liveFileRef(tenantID, uniqueID)
	if err != nil {
		return err
	}
	if err := s.objects.Delete(ctx, ref); err != nil && !errors.Is(err, objectstore.ErrNotFound) {
		return fmt.Errorf("delete the live contract %s: %w", ref.Key, err)
	}
	return nil
}

// ReadIndex reads the tenant's index. A missing index is found false; an
// index that is not JSON, of another schema version, too large or with an
// unreadable written_at is ports.ErrLiveIndexUnreadable, naming its key.
func (s *LiveContractsStore) ReadIndex(ctx context.Context, tenantID string) (ports.LiveIndex, bool, error) {
	ref, err := liveIndexRef(tenantID)
	if err != nil {
		return ports.LiveIndex{}, false, err
	}
	body, err := s.objects.Get(ctx, ref, maxLiveIndexBytes)
	switch {
	case errors.Is(err, objectstore.ErrNotFound):
		return ports.LiveIndex{}, false, nil
	case errors.Is(err, objectstore.ErrTooLarge):
		return ports.LiveIndex{}, false, fmt.Errorf("%w: the live index %s: %w", ports.ErrLiveIndexUnreadable, ref.Key, err)
	case err != nil:
		return ports.LiveIndex{}, false, fmt.Errorf("read the live index %s: %w", ref.Key, err)
	}
	var dto liveIndexDTO
	if err := json.Unmarshal(body, &dto); err != nil {
		return ports.LiveIndex{}, false, fmt.Errorf("%w: decode the live index %s: %w", ports.ErrLiveIndexUnreadable, ref.Key, err)
	}
	if dto.SchemaVersion != ports.LiveIndexSchemaVersion {
		return ports.LiveIndex{}, false, fmt.Errorf("%w: the live index %s has schema_version %d, this build reads %d",
			ports.ErrLiveIndexUnreadable, ref.Key, dto.SchemaVersion, ports.LiveIndexSchemaVersion)
	}
	writtenAt, err := time.Parse(liveIndexTimeStamp, dto.WrittenAt)
	if err != nil {
		return ports.LiveIndex{}, false, fmt.Errorf("%w: decode the live index %s: written_at: %w", ports.ErrLiveIndexUnreadable, ref.Key, err)
	}
	idx := ports.LiveIndex{
		SchemaVersion:  dto.SchemaVersion,
		TenantID:       dto.TenantID,
		PromotionSeq:   dto.PromotionSeq,
		ReleaseID:      dto.ReleaseID,
		WrittenAt:      writtenAt,
		SettingsSHA256: dto.SettingsSHA256,
		Files:          make([]ports.LiveIndexFile, 0, len(dto.Files)),
	}
	for _, f := range dto.Files {
		idx.Files = append(idx.Files, ports.LiveIndexFile{UniqueID: f.UniqueID, Version: f.Version, ContractHash: f.ContractHash})
	}
	return idx, true, nil
}

// PutIndex writes the tenant's index, its files in the order given.
func (s *LiveContractsStore) PutIndex(ctx context.Context, tenantID string, idx ports.LiveIndex) error {
	ref, err := liveIndexRef(tenantID)
	if err != nil {
		return err
	}
	dto := liveIndexDTO{
		SchemaVersion:  idx.SchemaVersion,
		TenantID:       idx.TenantID,
		PromotionSeq:   idx.PromotionSeq,
		ReleaseID:      idx.ReleaseID,
		WrittenAt:      idx.WrittenAt.UTC().Format(liveIndexTimeStamp),
		SettingsSHA256: idx.SettingsSHA256,
		Files:          make([]liveIndexFileDTO, 0, len(idx.Files)),
	}
	for _, f := range idx.Files {
		dto.Files = append(dto.Files, liveIndexFileDTO{
			UniqueID: f.UniqueID, Key: f.UniqueID + liveFileSuffix, Version: f.Version, ContractHash: f.ContractHash,
		})
	}
	body, err := json.Marshal(dto)
	if err != nil {
		return fmt.Errorf("encode the live index %s: %w", ref.Key, err)
	}
	if err := s.objects.Put(ctx, ref, body, liveIndexType); err != nil {
		return fmt.Errorf("put the live index %s: %w", ref.Key, err)
	}
	return nil
}

// liveFileRef is the object of uniqueID's published contract in tenantID's
// folder.
func liveFileRef(tenantID, uniqueID string) (objectstore.Ref, error) {
	if err := livePathSegment(tenantID); err != nil {
		return objectstore.Ref{}, fmt.Errorf("tenant %w", err)
	}
	if err := livePathSegment(uniqueID); err != nil {
		return objectstore.Ref{}, fmt.Errorf("unique id %w", err)
	}
	return objectstore.Ref{Key: liveKeyRoot + tenantID + liveKeyDir + uniqueID + liveFileSuffix}, nil
}

// liveIndexRef is the object of tenantID's index.
func liveIndexRef(tenantID string) (objectstore.Ref, error) {
	if err := livePathSegment(tenantID); err != nil {
		return objectstore.Ref{}, fmt.Errorf("tenant %w", err)
	}
	return objectstore.Ref{Key: liveKeyRoot + tenantID + liveKeyDir + liveIndexName}, nil
}

// livePathSegment refuses an id that would name no directory or file, or
// another one: empty, "." or "..", or holding a "/".
func livePathSegment(id string) error {
	if id == "" || id == "." || id == ".." || strings.Contains(id, "/") {
		return fmt.Errorf("%q: %w", id, ports.ErrInvalidLiveFile)
	}
	return nil
}
