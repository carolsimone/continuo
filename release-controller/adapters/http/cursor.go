package http

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/carolsimone/continuo/release-controller/domain/repository"
)

// encodeCursor renders a ListCursor as base64("<unixnano>|<release_id>").
func encodeCursor(c *repository.ListCursor) string {
	if c == nil {
		return ""
	}
	raw := fmt.Sprintf("%d|%s", c.CreatedAt.UTC().UnixNano(), c.ReleaseID)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor parses a base64 cursor. Empty string yields (nil, nil).
func decodeCursor(s string) (*repository.ListCursor, error) {
	if s == "" {
		return nil, nil
	}
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode cursor: %w", err)
	}
	parts := strings.SplitN(string(b), "|", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("malformed cursor")
	}
	ns, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("cursor timestamp: %w", err)
	}
	return &repository.ListCursor{CreatedAt: time.Unix(0, ns).UTC(), ReleaseID: parts[1]}, nil
}
