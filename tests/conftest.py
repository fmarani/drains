import pytest

pytest_plugins = "pytest_asgi_server"


@pytest.fixture
def xserver(xserver_factory):
    yield xserver_factory(
        appstr="tests.asgi_app:app", env={"PYTHONDONTWRITEBYTECODE": "1"}
    )
