"""
For running the app with an external debugger (IDE) attached.
This only partially enables Flask development mode.
For running the app with a fully enabled Flask development mode using the Flask CLI, see README.md.
"""

import os
import accountability_api

if __name__ == "__main__":
    env = os.environ.get("FLASK_ENV", "development")
    app = accountability_api.create_app(f"accountability_api.settings.{env.capitalize()}Config")
    app.run(
        host="0.0.0.0",
        port=8875,
        # The following args are present for improved external debugger support.
        #  See Flask documentation documentation (https://flask.palletsprojects.com/en/2.0.x/debugging/?highlight=debugger#external-debuggers)
        debug=True,
        use_debugger=False,
        use_reloader=False
    )
