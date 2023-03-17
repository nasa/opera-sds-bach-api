from flask import Blueprint, render_template
from flask_restx import Api, apidoc
from .reports import api as reports_api

from .data import api as data_api

from accountability_api.configuration_obj import ConfigurationObj

version = "2.0"
blueprint = Blueprint("lsmd_h5_flask", __name__, url_prefix="/")

api = Api(
    blueprint,
    title="NISAR BACH API",
    version=version,
    description="API to support the Bach Data Accountability UI and reports for NISAR",
    doc="/doc/",
)

config = ConfigurationObj()


@apidoc.apidoc.add_app_template_global
def swagger_static(filename):
    """
    A hack to solve serving swagger related files in docker

    Problem:
        When running this in factotum, the application is running internally on a custom port example:8080
        So, it would run at http://localhost:8080
        And swagger doc is at http://localhost:8080/3.0/doc/
        And related swagger files will be called at http://localhost:8080/swaggerui/swagger-ui-bundle.js

        But there is a reverse proxy to expose to the network with prefix /api

        So, from outside network, it is called at https://<ip>/api
        swagger is called from outside network at https://<ip>/api/3.0/doc/
        Related swagger files should be called at https://<ip>/api/swaggerui/swagger-ui-bundle.js

        This fails because swagger does not know the /api prefix and callingthem at https://<ip>/swaggerui/swagger-ui-bundle.js
        This  results in 404 for related swagger files

    Example: http://localhost:8080/swaggerui/swagger-ui-bundle.js

    Ref: https://github.com/noirbizarre/flask-restplus/issues/262

    :param filename:
    :return:
    """
    return "{}/swaggerui/{}".format(
        config.get_item("swagger_base", default=""), filename
    )


@api.documentation
def custom_ui():
    """
    Part of the above solution where swagger.json needs to be in the correct place.
    """
    return render_template(
        "swagger-ui.html",
        title=api.title,
        specs_url="{}/{}/swagger.json".format(
            config.get_item("swagger_base", default=""), version
        ),
    )


# Register namespaces
api.add_namespace(reports_api)
api.add_namespace(data_api)
