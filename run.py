from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import

import os

from accountability_api import create_app

from future import standard_library

standard_library.install_aliases()

if __name__ == "__main__":
    env = os.environ.get("FLASK_ENV", "development")
    app = create_app(f"accountability_api.settings.{env.capitalize()}Config")

    app.run(host="0.0.0.0", port=8875, debug=False)
