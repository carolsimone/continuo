// Package objectstoretest holds test support for code built on
// pkg/objectstore.
//
// Memory is an in-memory implementation of every role (objectstore.Reader,
// Writer, StreamReader, StreamWriter, Lister, Deleter and PrefixDeleter). An
// adapter's unit tests pass it where production passes a
// *objectstore.Client; pkg/objectstore's contract tests hold both to the same
// behaviour. Server stands in for S3 over HTTP, for
// pkg/objectstore's own client tests and for each service's one test that a
// real client, built without static keys, signs with the default credential
// chain; IsolateAWSEnvironment and UseChainCredentials prepare that chain.
package objectstoretest
