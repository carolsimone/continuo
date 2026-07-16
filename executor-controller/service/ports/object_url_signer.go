package ports

import (
	"context"
	"time"
)

// ObjectURLSigner mints time-limited URLs for single objects in the object
// store. A signed URL is a capability: whoever holds it can perform exactly the
// one operation it was signed for, on exactly the one object it names, until it
// expires. Worker pods hold no object-store credentials of their own and read
// and write only through these.
type ObjectURLSigner interface {
	// PresignGet signs a read of the object at s3URI, valid for ttl.
	PresignGet(ctx context.Context, s3URI string, ttl time.Duration) (string, error)
	// PresignPut signs an upload of contentType to s3URI, valid for ttl.
	PresignPut(ctx context.Context, s3URI, contentType string, ttl time.Duration) (string, error)
}
