// Package maintenance holds what every service shares about maintenance mode:
// the refusal message, the error that carries it, and the marker a gRPC
// refusal carries so a client can tell it from an outage.
package maintenance

import "errors"

// Message is the text every maintenance refusal carries.
const Message = "continuo is in maintenance mode: new work is not accepted until it is turned off"

// Code is the machine-readable code an HTTP refusal carries in its JSON body.
const Code = "maintenance"

// TrailerKey is the gRPC trailer a maintenance refusal sets to "true". A client
// reads it to tell a maintenance refusal from any other UNAVAILABLE.
const TrailerKey = "continuo-maintenance"

// RetryAfterSeconds is the Retry-After an HTTP refusal suggests.
const RetryAfterSeconds = 300

// ErrActive is returned by an application handler that refuses new work
// because maintenance mode is on.
var ErrActive = errors.New(Message)
