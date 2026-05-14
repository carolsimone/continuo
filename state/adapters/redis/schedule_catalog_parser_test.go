package redis

import (
	"testing"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseScheduleCatalogLoaded_HappyPath(t *testing.T) {
	eventID := uuid.New()
	payload := `{
		"event_id": "` + eventID.String() + `",
		"schedule_names": ["s1", "s2"],
		"service_metadata": {
			"svcA": {"manifest_version": "m1", "image_tag": "v1"}
		}
	}`
	msg := goredis.XMessage{
		ID:     "1-0",
		Values: map[string]interface{}{"payload": payload},
	}
	evt, err := ParseScheduleCatalogLoaded(msg)
	require.NoError(t, err)
	assert.Equal(t, eventID, evt.EventID)
	assert.Equal(t, []string{"s1", "s2"}, evt.ScheduleNames)
	assert.Equal(t, "m1", evt.ServiceMetadata["svcA"].ManifestVersion)
	assert.Equal(t, "v1", evt.ServiceMetadata["svcA"].ImageTag)
}

func TestParseScheduleCatalogLoaded_MissingPayload(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{}}
	_, err := ParseScheduleCatalogLoaded(msg)
	require.Error(t, err)
}

func TestParseScheduleCatalogLoaded_BadJSON(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": "{nope"}}
	_, err := ParseScheduleCatalogLoaded(msg)
	require.Error(t, err)
}

func TestParseScheduleCatalogLoaded_BadEventID(t *testing.T) {
	msg := goredis.XMessage{ID: "1-0", Values: map[string]interface{}{"payload": `{"event_id":"not-a-uuid","schedule_names":[]}`}}
	_, err := ParseScheduleCatalogLoaded(msg)
	require.Error(t, err)
}
