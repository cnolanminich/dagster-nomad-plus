import pytest

pytest_plugins: list[str] = []


@pytest.fixture
def nomad_address() -> str:
    return "http://nomad.test.local:4646"
