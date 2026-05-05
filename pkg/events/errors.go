package events

import "errors"

// ErrPermanent signals that an error is non-retryable. Handlers wrap it via
// fmt.Errorf("%w: ...", ErrPermanent, ...) when input is permanently bad
// (validation failure, deterministic data corruption). Consumers and outbox
// processors recognise it via errors.Is and short-circuit retries.
var ErrPermanent = errors.New("permanent: retry will not help")
