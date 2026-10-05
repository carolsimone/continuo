from adapters.redis.redrive import route


def test_plain_entry_is_for_every_group():
    ok, fields = route({b"k": b"v"}, "g")
    assert ok and fields == {b"k": b"v"}


def test_redrive_for_other_group_is_acked():
    ok, _ = route({"k": "v", "redrive_group": "other"}, "g")
    assert not ok


def test_redrive_for_this_group_strips_redrive_fields():
    ok, fields = route({b"k": b"v", b"redrive_group": b"g", b"redriven_from": b"dl"}, "g")
    assert ok and fields == {b"k": b"v"}


def test_outbox_redrive_without_group_goes_to_every_group():
    ok, fields = route({"k": "v", "redriven_from": "dl"}, "g")
    assert ok and fields == {"k": "v"}
