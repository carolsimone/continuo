package run

// ServiceMetadata is the per-service topology snapshot pinned on a Run at
// activation time. The adapter persists it as a JSON map keyed by service name
// on scheduler_tracker.service_metadata; the JSON field names live on the
// adapter-side carrier, keeping this value object free of serialization tags.
type ServiceMetadata struct {
	ManifestVersion string
	ImageTag        string
}
