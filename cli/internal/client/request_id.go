package client

import (
	"crypto/rand"
	"fmt"
	"os"
	"sync"
)

// requestIDEnv names the environment variable that fixes the request id of
// every command of the process. A caller that must make a repeated invocation
// idempotent, such as the chat executing an approved tool call, sets it.
const requestIDEnv = "CONTINUO_REQUEST_ID"

var (
	processRequestIDOnce sync.Once
	processRequestID     string
)

// RequestID returns the request id a command that starts or cancels a run
// sends. It is CONTINUO_REQUEST_ID when that is set, otherwise one UUIDv4 for
// the whole process, so a command the client retries repeats the id of its
// first try and one invocation never names two.
func RequestID() string {
	if id := os.Getenv(requestIDEnv); id != "" {
		return id
	}
	processRequestIDOnce.Do(func() { processRequestID = newUUIDv4() })
	return processRequestID
}

// newUUIDv4 returns a random RFC 4122 version 4 UUID in its canonical text form.
func newUUIDv4() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on supported platforms; a process that
		// cannot read randomness cannot mint an id, so it stops.
		panic(fmt.Sprintf("read random bytes for a request id: %v", err))
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// RequestIDHelp is the paragraph a command that sends a request id adds to its
// help, so a reader knows what repeating the command does.
const RequestIDHelp = `Each invocation sends one request id (the CONTINUO_REQUEST_ID environment
variable when set, otherwise a fresh UUID). CONTINUO_REQUEST_ID must be a UUID:
any other value is refused (usage, exit 2). The state service answers a repeat
of the same id and the same arguments with the run the first call started or
cancelled and writes nothing, so a lost response is safe to retry. The same id
with other arguments is refused (conflict, exit 4, not retryable): no retry of
it can succeed, so send other arguments under a new id. A call refused because
the state service is unavailable, or because concurrent calls of the same id
kept rolling back, is sent again with the same id up to twice before it is
reported (unavailable or conflict, retryable).`
