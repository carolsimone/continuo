package events

import (
	"encoding/json"
	"fmt"
)

// ManifestKey names one project's artifact in a release.requested:v1 payload.
// Kind is "dbt", "python" or "ontology" and is always written; a reader
// treats an absent kind as "dbt".
type ManifestKey struct {
	Project string `json:"project"`
	S3URI   string `json:"s3_uri"`
	Kind    string `json:"kind"`
}

// ReleaseRequested is the payload of a release.requested:v1 entry: the release
// topology-controller resolves, the artifact to fetch for each project, and
// the image tag each project's nodes run in. topology-controller joins
// ImageTags onto every node of the candidate topology it writes.
//
// The stream carries this JSON as its single "payload" field, with no envelope
// fields beside it.
type ReleaseRequested struct {
	ReleaseID    string            `json:"release_id"`
	ManifestKeys []ManifestKey     `json:"manifest_keys"`
	ImageTags    map[string]string `json:"image_tags"`
}

// ReleaseRequestedPayload renders p as the payload of one release.requested:v1
// entry. manifest_keys is always a list and image_tags always an object, so a
// reader never meets a null.
func ReleaseRequestedPayload(p ReleaseRequested) ([]byte, error) {
	if p.ManifestKeys == nil {
		p.ManifestKeys = []ManifestKey{}
	}
	if p.ImageTags == nil {
		p.ImageTags = map[string]string{}
	}
	b, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("marshal release.requested payload: %w", err)
	}
	return b, nil
}
