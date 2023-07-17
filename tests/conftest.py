import pytest


@pytest.fixture(scope="module")
def vcr_config():
    return {"cassette_library_dir": "tests/cassettes", "serializer": "json"}
