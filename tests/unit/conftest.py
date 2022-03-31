import os

import pytest

from accountability_api import create_app


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    """Remove HTTP client usage from all tests."""
    monkeypatch.delattr("requests.sessions.Session.request")
    monkeypatch.delattr("elasticsearch.connection.base.Connection.perform_request")
    monkeypatch.delattr("elasticsearch.connection.http_requests.RequestsHttpConnection.perform_request")


@pytest.fixture(scope='module')
def test_client():
    env = os.environ.get("FLASK_ENV", "development")
    flask_app = create_app(f"accountability_api.settings.{env.capitalize()}Config")

    # Create a test client using the Flask application configured for testing
    with flask_app.test_client() as testing_client:
        with flask_app.app_context():
            yield testing_client
