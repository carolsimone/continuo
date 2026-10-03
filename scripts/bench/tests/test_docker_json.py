import docker_json


def test_network_aliases_lists_every_network_with_its_aliases():
    inspect = [{"NetworkSettings": {"Networks": {
        "proj_default": {"Aliases": ["proj-postgres-1", "postgres"]},
        "kind": {"Aliases": None},
    }}}]
    assert docker_json.network_aliases(inspect) == [("kind", []), ("proj_default", ["proj-postgres-1", "postgres"])]
