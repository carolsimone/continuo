package deploy

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
