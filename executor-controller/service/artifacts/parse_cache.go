// Package artifacts is the single home for the S3 URI layout of the compile
// leg's dbt artifacts (manifest.json and the partial-parse cache). Both the
// application handler (service/handlers/compile_requested_handler.go) and the
// k8s adapter (adapters/k8s/client.go) build these addresses; centralizing
// the string layout here keeps the two call sites from drifting apart. These
// are pure string builders — no S3 client, no infrastructure import — but the
// addressing scheme itself is infrastructure vocabulary, so it lives in the
// service layer rather than domain.
package artifacts

// ParseCacheProdURI is the canonical S3 URI of the prod-context partial-parse
// artifact. Keyed by (service, image_tag), NOT release: production run Jobs
// carry only ServiceName+ImageTag, and cache validity is a property of the
// image + env, not of the release that first shipped the tag.
func ParseCacheProdURI(bucket, service, imageTag string) string {
	return "s3://" + bucket + "/" + service + "/parse-cache/" + imageTag + "/partial_parse.msgpack"
}

// ParseCacheCandidateURI is the canonical S3 URI of the candidate-context
// partial-parse artifact — a sibling of the release's manifest.json, because
// DBT_TARGET_SCHEMA embeds the per-release candidate schema.
func ParseCacheCandidateURI(bucket, service, releaseID string) string {
	return "s3://" + bucket + "/" + service + "/" + releaseID + "/partial_parse.candidate.msgpack"
}

// ManifestURI is the canonical S3 URI of a release's compiled manifest.json,
// uploaded by the compile Job's upload container and later fetched by
// topology-controller.
func ManifestURI(bucket, service, releaseID string) string {
	return "s3://" + bucket + "/" + service + "/" + releaseID + "/manifest.json"
}
