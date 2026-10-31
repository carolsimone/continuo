// Package objectstore is continuo's access to S3-compatible object storage:
// AWS S3, or MinIO in compose and single-node installs. It moves bytes and
// nothing else.
//
// roles.go declares one-method role interfaces: Reader and Writer for whole
// objects held in memory, StreamReader and StreamWriter for objects too large
// to hold whole, Lister, Deleter and PrefixDeleter, and ReadWriter for an
// adapter that both reads and writes. An adapter depends on the narrowest role
// it uses and never on *Client. Only a service's main builds a Client, with
// New, and passes it to each adapter as the role that adapter uses. Client is
// the one production implementation of every role.
//
// Object storage is infrastructure. A service's domain and application code
// depends only on ports of its own (service/ports or domain/repository),
// phrased in its own vocabulary. An adapter implements such a port by
// composing a role with the codec of its document, and translates this
// package's errors, ErrNotFound, ErrTooLarge and ErrInvalidRef, into the
// port's. A URI that arrives from outside the process becomes a Ref through
// ParseRef once, in the adapter that receives it.
//
// Package objectstoretest holds test support for code built on this package.
package objectstore
