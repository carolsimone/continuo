package objectstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const (
	// partSize is the size of every part of a multipart upload but the last.
	// S3 refuses a part under 5 MiB unless it is the last, and a writer holds
	// one part in memory at a time.
	partSize = 8 << 20

	// abortTimeout bounds the AbortMultipartUpload request, which runs even
	// when the context the writer was opened with is cancelled.
	abortTimeout = 30 * time.Second
)

// OpenWriter implements StreamWriter. It creates the multipart upload before it
// returns, so a store that refuses the upload fails the call. The ctx governs
// every request of the writer except the cleanup of Abort, which is not
// cancelled with it.
func (c *Client) OpenWriter(ctx context.Context, ref Ref, contentType string) (ObjectWriter, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	ref = c.resolve(ref)
	out, err := c.s3.CreateMultipartUpload(ctx, &awss3.CreateMultipartUploadInput{
		Bucket:      aws.String(ref.Bucket),
		Key:         aws.String(ref.Key),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return nil, fmt.Errorf("open writer for %s: %w", ref, err)
	}
	return &multipartWriter{
		c:           c,
		ctx:         ctx,
		ref:         ref,
		contentType: contentType,
		uploadID:    out.UploadId,
		nextPart:    1,
	}, nil
}

// writerState is where a multipartWriter is in its life.
type writerState int

const (
	writerOpen writerState = iota
	writerCompleted
	writerAborted
)

// multipartWriter is the ObjectWriter of a Client: an S3 multipart upload fed
// from a buffer of one part. A full buffer is sent as the next part at once, so
// at most one part is ever held, and Close sends what is left as the last part
// and completes the upload. Parts are sent one after the other, numbered from
// 1. It is not safe for concurrent use.
type multipartWriter struct {
	c           *Client
	ctx         context.Context
	ref         Ref // the resolved Ref: its Bucket is named
	contentType string
	// uploadID names the open upload; nil once the store no longer holds it.
	uploadID *string
	buf      []byte
	parts    []s3types.CompletedPart
	// nextPart is the number of the next part to send.
	nextPart int32
	state    writerState
	// err is the first failure to reach the store; every later Write and Close
	// returns it.
	err error
}

// withoutChecksum sends a part without the SDK's default CRC32 checksum. The
// upload is created without a checksum algorithm, and a part's checksum has to
// belong to the upload's.
func withoutChecksum(o *awss3.Options) {
	o.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
}

// fail records err as the writer's failure, unless it already has one, and
// returns the failure.
func (w *multipartWriter) fail(err error) error {
	if w.err == nil {
		w.err = err
	}
	return w.err
}

// usable reports why the writer cannot take a Write or a Close, or nil.
func (w *multipartWriter) usable() error {
	if w.state != writerOpen {
		return fmt.Errorf("%w: %s", ErrWriterClosed, w.ref)
	}
	if w.err != nil {
		return w.err
	}
	if err := w.ctx.Err(); err != nil {
		return w.fail(fmt.Errorf("object writer for %s: %w", w.ref, err))
	}
	return nil
}

// Write buffers p and sends each part the buffer fills.
func (w *multipartWriter) Write(p []byte) (int, error) {
	if err := w.usable(); err != nil {
		return 0, err
	}
	if w.buf == nil {
		w.buf = make([]byte, 0, partSize)
	}
	written := 0
	for len(p) > 0 {
		n := min(len(p), partSize-len(w.buf))
		w.buf = append(w.buf, p[:n]...)
		p = p[n:]
		written += n
		if len(w.buf) == partSize {
			if err := w.sendPart(); err != nil {
				return written, err
			}
		}
	}
	return written, nil
}

// sendPart uploads the buffer as the next part and empties it.
func (w *multipartWriter) sendPart() error {
	number := w.nextPart
	out, err := w.c.s3.UploadPart(w.ctx, &awss3.UploadPartInput{
		Bucket:     aws.String(w.ref.Bucket),
		Key:        aws.String(w.ref.Key),
		UploadId:   w.uploadID,
		PartNumber: aws.Int32(number),
		Body:       bytes.NewReader(w.buf),
	}, withoutChecksum)
	if err != nil {
		return w.fail(fmt.Errorf("upload part %d of %s: %w", number, w.ref, err))
	}
	w.parts = append(w.parts, s3types.CompletedPart{ETag: out.ETag, PartNumber: aws.Int32(number)})
	w.nextPart++
	w.buf = w.buf[:0]
	return nil
}

// Close sends the buffered bytes as the last part and completes the upload,
// which makes the object visible. A part of exactly the part size is sent when
// it fills, so an object that is a whole number of parts has no empty last
// part. An object of no bytes has no part to complete, which S3 refuses, so it
// is stored with one PutObject instead and the upload is aborted.
func (w *multipartWriter) Close() error {
	if err := w.usable(); err != nil {
		return err
	}
	if len(w.parts) == 0 && len(w.buf) == 0 {
		return w.closeEmpty()
	}
	if len(w.buf) > 0 {
		if err := w.sendPart(); err != nil {
			return err
		}
	}
	_, err := w.c.s3.CompleteMultipartUpload(w.ctx, &awss3.CompleteMultipartUploadInput{
		Bucket:          aws.String(w.ref.Bucket),
		Key:             aws.String(w.ref.Key),
		UploadId:        w.uploadID,
		MultipartUpload: &s3types.CompletedMultipartUpload{Parts: w.parts},
	})
	if err != nil {
		return w.fail(fmt.Errorf("complete %s: %w", w.ref, err))
	}
	w.finish(writerCompleted)
	return nil
}

// closeEmpty stores the empty object with PutObject. The upload is aborted
// first, so a Close that fails has stored nothing.
func (w *multipartWriter) closeEmpty() error {
	if err := w.abortUpload(w.ctx); err != nil {
		return w.fail(err)
	}
	_, err := w.c.s3.PutObject(w.ctx, &awss3.PutObjectInput{
		Bucket:      aws.String(w.ref.Bucket),
		Key:         aws.String(w.ref.Key),
		Body:        bytes.NewReader(nil),
		ContentType: aws.String(w.contentType),
	})
	if err != nil {
		return w.fail(fmt.Errorf("put %s: %w", w.ref, err))
	}
	w.finish(writerCompleted)
	return nil
}

// Abort discards the upload and every part sent so far. It runs with a context
// that outlives the cancellation of the writer's own, bounded by abortTimeout.
// An upload the store no longer holds counts as aborted. Abort of a writer
// that was closed or aborted does nothing; when the request fails, the writer
// stays abortable and Write and Close fail.
func (w *multipartWriter) Abort() error {
	if w.state != writerOpen {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), abortTimeout)
	defer cancel()
	if err := w.abortUpload(ctx); err != nil {
		_ = w.fail(err) // later Writes and Closes report the failure; Abort returns its own
		return err
	}
	w.finish(writerAborted)
	return nil
}

// abortUpload sends AbortMultipartUpload for the open upload, if there is one.
func (w *multipartWriter) abortUpload(ctx context.Context) error {
	if w.uploadID == nil {
		return nil
	}
	_, err := w.c.s3.AbortMultipartUpload(ctx, &awss3.AbortMultipartUploadInput{
		Bucket:   aws.String(w.ref.Bucket),
		Key:      aws.String(w.ref.Key),
		UploadId: w.uploadID,
	})
	var gone *s3types.NoSuchUpload
	if err != nil && !errors.As(err, &gone) {
		return fmt.Errorf("abort upload of %s: %w", w.ref, err)
	}
	w.uploadID = nil
	return nil
}

// finish ends the writer in state and releases its buffer.
func (w *multipartWriter) finish(state writerState) {
	w.state = state
	w.buf = nil
	w.parts = nil
}
