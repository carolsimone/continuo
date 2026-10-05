package model

import "time"

// ReplayHorizon is how long a message stays replayable: consumers keep their
// dedup records at least this long, dead letters can be redriven until their
// original message is this old, and older dead letters are deleted.
const ReplayHorizon = 30 * 24 * time.Hour
