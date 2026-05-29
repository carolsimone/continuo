from domain.candidate import CandidateParseFailure


def test_candidate_parse_failure_carries_class_and_detail():
    exc = CandidateParseFailure(
        error_class="UnqualifiedTableReference",
        error_detail="ref 'foo' unresolved",
    )
    assert exc.error_class == "UnqualifiedTableReference"
    assert exc.error_detail == "ref 'foo' unresolved"
    assert "UnqualifiedTableReference" in str(exc)
    assert "ref 'foo' unresolved" in str(exc)
