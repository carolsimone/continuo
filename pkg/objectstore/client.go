package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Client is the S3 implementation of every role in roles.go. It is safe for
// concurrent use.
type Client struct {
	s3     *awss3.Client
	bucket string
}

// New builds a Client for cfg: path-style requests against cfg.EndpointURL
// when it is set (MinIO) and AWS's endpoint otherwise, signed with the
// credentials awsConfig resolves. cfg.Bucket, the default bucket, is
// required.
func New(ctx context.Context, cfg Config) (*Client, error) {
	return newClient(ctx, cfg)
}

// newClient is New with extra S3 client options applied last; tests use it to
// turn the SDK's retries off.
func newClient(ctx context.Context, cfg Config, extra ...func(*awss3.Options)) (*Client, error) {
	if cfg.Bucket == "" {
		return nil, errors.New("objectstore: a default bucket is required")
	}
	if cfg.Region == "" && cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		// Static credentials carry no region; the default chain can resolve one
		// from the environment, so only this path insists on it.
		return nil, errors.New("objectstore: a region is required with static credentials")
	}
	awsCfg, err := awsConfig(ctx, cfg.Region, cfg.AccessKeyID, cfg.SecretAccessKey)
	if err != nil {
		return nil, err
	}
	opts := []func(*awss3.Options){func(o *awss3.Options) {
		o.UsePathStyle = true
		// An object stored without a checksum comes back without one. The SDK
		// would log a plain-text warning on every such read; the JSON service
		// logs carry no line the service did not write.
		o.DisableLogOutputChecksumValidationSkipped = true
	}}
	if cfg.EndpointURL != "" {
		endpoint := cfg.EndpointURL
		opts = append(opts, func(o *awss3.Options) { o.BaseEndpoint = aws.String(endpoint) })
	}
	opts = append(opts, extra...)
	return &Client{s3: awss3.NewFromConfig(awsCfg, opts...), bucket: cfg.Bucket}, nil
}

// Bucket returns the default bucket.
func (c *Client) Bucket() string {
	return c.bucket
}

// resolve names the default bucket in a Ref that names none.
func (c *Client) resolve(ref Ref) Ref {
	if ref.Bucket == "" {
		ref.Bucket = c.bucket
	}
	return ref
}

// Get implements Reader. The object's declared Content-Length is checked
// against maxBytes before the body is read, and the bytes read are checked
// again, so no more than maxBytes+1 bytes are ever held. An unusable maxBytes
// and a Ref that names no object are refused before any request. A body cut
// short keeps io.ErrUnexpectedEOF and an S3 5xx keeps its HTTP status.
func (c *Client) Get(ctx context.Context, ref Ref, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 || maxBytes == math.MaxInt64 {
		return nil, fmt.Errorf("objectstore: read limit %d is not usable", maxBytes)
	}
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	ref = c.resolve(ref)
	out, err := c.s3.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: aws.String(ref.Bucket),
		Key:    aws.String(ref.Key),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, fmt.Errorf("%w: %s", ErrNotFound, ref)
		}
		return nil, fmt.Errorf("get %s: %w", ref, err)
	}
	defer func() { _ = out.Body.Close() }()
	if out.ContentLength != nil && *out.ContentLength > maxBytes {
		return nil, fmt.Errorf("%w: %s is %d bytes, over the %d-byte limit",
			ErrTooLarge, ref, *out.ContentLength, maxBytes)
	}
	body, err := io.ReadAll(io.LimitReader(out.Body, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", ref, err)
	}
	if int64(len(body)) > maxBytes {
		return nil, fmt.Errorf("%w: %s exceeds the %d-byte limit", ErrTooLarge, ref, maxBytes)
	}
	return body, nil
}

// Put implements Writer. A Ref that names no object is refused before any
// request.
func (c *Client) Put(ctx context.Context, ref Ref, body []byte, contentType string) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	ref = c.resolve(ref)
	_, err := c.s3.PutObject(ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(ref.Bucket),
		Key:         aws.String(ref.Key),
		Body:        bytes.NewReader(body),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("put %s: %w", ref, err)
	}
	return nil
}

// DeletePrefix implements PrefixDeleter. It lists the prefix in the default
// bucket page by page (ListObjectsV2, at most 1,000 keys a page) and deletes
// each page with one DeleteObjects request. Nothing matching the prefix sends
// no delete. A key the store refuses to delete fails the call with an error
// naming how many keys of that batch failed and the first of them; keys
// already deleted stay deleted. An empty prefix names the whole bucket and is
// refused.
func (c *Client) DeletePrefix(ctx context.Context, prefix string) error {
	if prefix == "" {
		return errors.New("objectstore: refusing to delete an empty prefix")
	}
	paginator := awss3.NewListObjectsV2Paginator(c.s3, &awss3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String(prefix),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list s3://%s/%s: %w", c.bucket, prefix, err)
		}
		if len(page.Contents) == 0 {
			continue
		}
		ids := make([]s3types.ObjectIdentifier, 0, len(page.Contents))
		for _, obj := range page.Contents {
			ids = append(ids, s3types.ObjectIdentifier{Key: obj.Key})
		}
		out, err := c.s3.DeleteObjects(ctx, &awss3.DeleteObjectsInput{
			Bucket: aws.String(c.bucket),
			Delete: &s3types.Delete{Objects: ids, Quiet: aws.Bool(true)},
		})
		if err != nil {
			return fmt.Errorf("delete under s3://%s/%s: %w", c.bucket, prefix, err)
		}
		if len(out.Errors) > 0 {
			first := out.Errors[0]
			return fmt.Errorf("delete under s3://%s/%s: %d of %d keys failed, first %s: %s: %s",
				c.bucket, prefix, len(out.Errors), len(ids),
				aws.ToString(first.Key), aws.ToString(first.Code), aws.ToString(first.Message))
		}
	}
	return nil
}
