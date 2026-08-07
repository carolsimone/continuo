import hashlib

from service.content_hash import content_hash_fold


def test_fold_is_sha256_of_pipe_joined_parts_with_prefix():
    expected = "sha256:" + hashlib.sha256(b"aaa|bbb|ccc").hexdigest()
    assert content_hash_fold("aaa", "bbb", "ccc") == expected


def test_empty_shared_code_part_folds_as_empty_string():
    expected = "sha256:" + hashlib.sha256(b"aaa||ccc").hexdigest()
    assert content_hash_fold("aaa", "", "ccc") == expected


def test_any_part_change_flips_the_fold():
    base = content_hash_fold("a", "b", "c")
    assert content_hash_fold("x", "b", "c") != base
    assert content_hash_fold("a", "x", "c") != base
    assert content_hash_fold("a", "b", "x") != base
