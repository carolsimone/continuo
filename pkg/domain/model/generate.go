package model

// The vocabularies declared in pkg/streams/contract.yaml are generated into
// this package: vocabulary.gen.go holds each type, its values, and its
// IsValid/Healable predicates; vocabulary_test_access.gen.go holds the
// accessor the parity test reads. The same generator run also writes the
// stream and consumer-group constants into pkg/streams.
//go:generate go run ../../streams/cmd/gen-streams
