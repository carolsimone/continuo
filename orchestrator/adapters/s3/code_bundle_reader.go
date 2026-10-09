package s3

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/carolsimone/continuo/orchestrator/service/ports"
	"github.com/carolsimone/continuo/pkg/codebundle"
)

// maxBundleBytes caps the code-bundle document orchestrator will hold in memory.
// It is far above any realistic estate — a bundle is source text for every node
// in the release — and exists so a pathological object cannot exhaust the
// container's memory limit during the read-then-decode step.
const maxBundleBytes int64 = 64 * 1024 * 1024

// CodeBundleReader fetches code-bundle documents from S3.
type CodeBundleReader struct {
	client        *awss3.Client
	defaultBucket string
}

// Compile-time assertion that the adapter satisfies the application port.
var _ ports.CodeBundleReader = (*CodeBundleReader)(nil)

// NewCodeBundleReader builds an S3-backed CodeBundleReader (see newClient for
// the endpoint and credential rules).
func NewCodeBundleReader(ctx context.Context, endpointURL, bucket, region, accessKeyID, secretKey string) (*CodeBundleReader, error) {
	client, err := newClient(ctx, endpointURL, region, accessKeyID, secretKey)
	if err != nil {
		return nil, err
	}
	return &CodeBundleReader{client: client, defaultBucket: bucket}, nil
}

// Fetch reads and decodes the bundle at uri. A missing object yields
// ports.ErrBundleNotFound; an uninterpretable one yields ports.ErrBundleMalformed.
func (r *CodeBundleReader) Fetch(ctx context.Context, uri string) (codebundle.Bundle, error) {
	if strings.TrimSpace(uri) == "" {
		return codebundle.Bundle{}, ports.ErrBundleNotFound
	}
	bucket, key := parseS3URI(uri)
	if bucket == "" {
		bucket = r.defaultBucket
	}
	out, err := r.client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return codebundle.Bundle{}, fmt.Errorf("%w: %s", ports.ErrBundleNotFound, uri)
		}
		return codebundle.Bundle{}, fmt.Errorf("get code bundle %s: %w", uri, err)
	}
	defer func() { _ = out.Body.Close() }()

	// Bound the read before allocating. Decoding also allocates a second
	// representation of the document, so an unbounded object — a pathological
	// compiled model, or a bundle far larger than any real estate — could exhaust
	// the container's memory limit before the per-node size guard ever runs.
	if out.ContentLength != nil && *out.ContentLength > maxBundleBytes {
		return codebundle.Bundle{}, fmt.Errorf("%w: %s is %d bytes, over the %d-byte ceiling",
			ports.ErrBundleTooLarge, uri, *out.ContentLength, maxBundleBytes)
	}
	body, err := io.ReadAll(io.LimitReader(out.Body, maxBundleBytes+1))
	if err != nil {
		return codebundle.Bundle{}, fmt.Errorf("read code bundle %s: %w", uri, err)
	}
	if int64(len(body)) > maxBundleBytes {
		return codebundle.Bundle{}, fmt.Errorf("%w: %s exceeds the %d-byte ceiling",
			ports.ErrBundleTooLarge, uri, maxBundleBytes)
	}
	bundle, err := codebundle.Decode(body)
	if err != nil {
		return codebundle.Bundle{}, fmt.Errorf("%w: %s: %v", ports.ErrBundleMalformed, uri, err)
	}
	return bundle, nil
}
