package redis

import (
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

func TestCheckAfterElapsed(t *testing.T) {
	now := time.Unix(1000, 0)

	if !checkAfterElapsed(goredis.XMessage{Values: map[string]interface{}{}}, now) {
		t.Fatal("missing check_after should be treated as ready")
	}
	if !checkAfterElapsed(goredis.XMessage{Values: map[string]interface{}{"check_after": "900"}}, now) {
		t.Fatal("past check_after should be ready")
	}
	if checkAfterElapsed(goredis.XMessage{Values: map[string]interface{}{"check_after": "1100"}}, now) {
		t.Fatal("future check_after should NOT be ready")
	}
	if !checkAfterElapsed(goredis.XMessage{Values: map[string]interface{}{"check_after": "bogus"}}, now) {
		t.Fatal("unparseable check_after should be treated as ready")
	}
}
