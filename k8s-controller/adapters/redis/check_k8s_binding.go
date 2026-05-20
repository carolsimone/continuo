package redis

import (
	"strconv"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// checkAfterElapsed reports whether a check.k8s:v1 message is due for
// processing. A message carrying a "check_after" Unix-second timestamp in the
// future is not yet due and should be re-circulated. A missing or unparseable
// timestamp is treated as ready (process now).
func checkAfterElapsed(msg goredis.XMessage, now time.Time) bool {
	s, ok := msg.Values["check_after"].(string)
	if !ok || s == "" {
		return true
	}
	checkAfter, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return true
	}
	return now.Unix() >= checkAfter
}
