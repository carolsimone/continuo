package s3

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/carolsimone/continuo/orchestrator/service/ports"
	"github.com/carolsimone/continuo/pkg/topologyartifact"
)

// topologyArtifactCacheSize bounds the decoded topology documents kept in
// memory. The topology group and the versions group read the same promotion,
// so a small window serves both reads of every promotion from one fetch.
const topologyArtifactCacheSize = 8

// objectGetter reads one object's bytes. It wraps ports.ErrTopologyArtifactNotFound
// for a missing object and ports.ErrTopologyArtifactCorrupt for an oversized
// one; every other error keeps its cause in the %w chain and wraps neither, so
// the consumer's classifier recognises an outage.
type objectGetter func(ctx context.Context, bucket, key string) ([]byte, error)

// TopologyArtifactReader reads release topology artifacts from S3, verifies each
// against the SHA-256 its promotion carried, and keeps the most recently read
// documents in memory. An artifact never changes once written, so an entry keyed
// by URI and checksum stays valid for as long as it is held.
type TopologyArtifactReader struct {
	get           objectGetter
	defaultBucket string

	mu      sync.Mutex
	entries map[artifactKey]*list.Element
	lru     *list.List
	maxSize int
}

type artifactKey struct{ uri, sha256 string }

type artifactEntry struct {
	key artifactKey
	doc topologyartifact.Document
}

// Compile-time assertion that the adapter satisfies the application port.
var _ ports.TopologyArtifactReader = (*TopologyArtifactReader)(nil)

// NewTopologyArtifactReader builds an S3-backed TopologyArtifactReader with the
// same endpoint and credential rules as NewCodeBundleReader.
func NewTopologyArtifactReader(ctx context.Context, endpointURL, bucket, region, accessKeyID, secretKey string) (*TopologyArtifactReader, error) {
	client, err := newClient(ctx, endpointURL, region, accessKeyID, secretKey)
	if err != nil {
		return nil, err
	}
	return newTopologyArtifactReader(s3ObjectGetter(client), bucket, topologyArtifactCacheSize), nil
}

func newTopologyArtifactReader(get objectGetter, bucket string, maxSize int) *TopologyArtifactReader {
	return &TopologyArtifactReader{
		get:           get,
		defaultBucket: bucket,
		entries:       make(map[artifactKey]*list.Element),
		lru:           list.New(),
		maxSize:       maxSize,
	}
}

// Load returns the topology document at uri once its bytes hash to sha256Hex.
func (r *TopologyArtifactReader) Load(ctx context.Context, uri, sha256Hex string) (topologyartifact.Document, error) {
	key := artifactKey{uri: uri, sha256: sha256Hex}
	if doc, ok := r.cached(key); ok {
		return doc, nil
	}

	bucket, objectKey := parseS3URI(uri)
	if bucket == "" {
		bucket = r.defaultBucket
	}
	if objectKey == "" {
		return topologyartifact.Document{}, fmt.Errorf("%w: %q names no object", ports.ErrTopologyArtifactCorrupt, uri)
	}
	body, err := r.get(ctx, bucket, objectKey)
	if err != nil {
		return topologyartifact.Document{}, err
	}

	doc, err := topologyartifact.Decode(body, sha256Hex)
	if err != nil {
		if errors.Is(err, topologyartifact.ErrChecksumMismatch) {
			actual := sha256.Sum256(body)
			return topologyartifact.Document{}, fmt.Errorf("%w: %s: expected sha256 %s, object has %s",
				ports.ErrTopologyArtifactCorrupt, uri, sha256Hex, hex.EncodeToString(actual[:]))
		}
		return topologyartifact.Document{}, fmt.Errorf("%w: %s: %v", ports.ErrTopologyArtifactCorrupt, uri, err)
	}
	r.put(key, doc)
	return doc, nil
}

func (r *TopologyArtifactReader) cached(key artifactKey) (topologyartifact.Document, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	elem, ok := r.entries[key]
	if !ok {
		return topologyartifact.Document{}, false
	}
	r.lru.MoveToFront(elem)
	return elem.Value.(*artifactEntry).doc, true
}

func (r *TopologyArtifactReader) put(key artifactKey, doc topologyartifact.Document) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if elem, ok := r.entries[key]; ok {
		r.lru.MoveToFront(elem)
		return
	}
	r.entries[key] = r.lru.PushFront(&artifactEntry{key: key, doc: doc})
	for r.lru.Len() > r.maxSize {
		oldest := r.lru.Back()
		r.lru.Remove(oldest)
		delete(r.entries, oldest.Value.(*artifactEntry).key)
	}
}

// s3ObjectGetter reads objects through client, bounded by the artifact size cap.
func s3ObjectGetter(client *awss3.Client) objectGetter {
	return func(ctx context.Context, bucket, key string) ([]byte, error) {
		out, err := client.GetObject(ctx, &awss3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			var nsk *s3types.NoSuchKey
			if errors.As(err, &nsk) {
				return nil, fmt.Errorf("%w: s3://%s/%s", ports.ErrTopologyArtifactNotFound, bucket, key)
			}
			return nil, fmt.Errorf("get topology artifact s3://%s/%s: %w", bucket, key, err)
		}
		defer func() { _ = out.Body.Close() }()

		if out.ContentLength != nil && *out.ContentLength > topologyartifact.MaxObjectBytes {
			return nil, fmt.Errorf("%w: s3://%s/%s is %d bytes, over the %d-byte ceiling",
				ports.ErrTopologyArtifactCorrupt, bucket, key, *out.ContentLength, topologyartifact.MaxObjectBytes)
		}
		body, err := io.ReadAll(io.LimitReader(out.Body, topologyartifact.MaxObjectBytes+1))
		if err != nil {
			return nil, fmt.Errorf("read topology artifact s3://%s/%s: %w", bucket, key, err)
		}
		if len(body) > topologyartifact.MaxObjectBytes {
			return nil, fmt.Errorf("%w: s3://%s/%s exceeds the %d-byte ceiling",
				ports.ErrTopologyArtifactCorrupt, bucket, key, topologyartifact.MaxObjectBytes)
		}
		return body, nil
	}
}
