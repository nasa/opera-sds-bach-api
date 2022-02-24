import os
from accountability_api import create_app

env = os.environ.get("FLASK_ENV", "development")
app = create_app("accountability_api.settings.%sConfig" % env.capitalize())
