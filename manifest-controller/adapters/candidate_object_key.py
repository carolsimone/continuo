"""The single source of the candidate-artifact S3 key convention.

Every per-node validation input for a release lives under one prefix —
candidate-sql/<release_id>/ — regardless of its shape: release-controller's
retention job and the bucket's lifecycle rule both prune by that prefix, so a
python node's .json spec belongs there exactly as a dbt node's .sql does. The
extension is what distinguishes them.
"""


def candidate_object_key(release_id: str, unique_id: str, extension: str) -> str:
    return f"candidate-sql/{release_id}/candidate_{unique_id}.{extension}"
