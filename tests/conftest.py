import os

import pytest

from accountability_api import create_app


@pytest.fixture(scope='module')
def test_client():
    env = os.environ.get("FLASK_ENV", "development")
    flask_app = create_app(f"accountability_api.settings.{env.capitalize()}Config")

    # Create a test client using the Flask application configured for testing
    with flask_app.test_client() as testing_client:
        with flask_app.app_context():
            yield testing_client
