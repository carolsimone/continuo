"""The three-part content-hash fold shared by every runtime's parser.

content_hash = "sha256:" + sha256("<source_hash>|<shared_code_hash>|<config_hash>")
— the single change-detection fingerprint: any component change flips it.
dbt parts are computed by parse_manifest from manifest.json; python parts
arrive CI-computed in the wire contract, and the fold is recomputed there to
verify the embedded content_hash.
"""
import hashlib


def content_hash_fold(source_hash: str, shared_code_hash: str, config_hash: str) -> str:
    return "sha256:" + hashlib.sha256(
        f"{source_hash}|{shared_code_hash}|{config_hash}".encode()
    ).hexdigest()
