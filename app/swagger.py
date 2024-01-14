# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

from flasgger import LazyString, Swagger
from flask import request
from flask_swagger_ui import get_swaggerui_blueprint

"""
docs:
- https://github.com/CryceTruly/bookmarker-api
- https://kanoki.org/2020/07/18/python-api-documentation-using-flask-swagger/
"""

# TODO: Change flasgger to flask_swagger_ui
# Handle default config from the swagger
_swagger_config = Swagger.DEFAULT_CONFIG
_swagger_config.update({
    # 'swagger_ui_bundle_js': '//unpkg.com/swagger-ui-dist@3/swagger-ui-bundle.js',
    # 'swagger_ui_standalone_preset_js': '//unpkg.com/swagger-ui-dist@3/swagger-ui-standalone-preset.js',
    # 'jquery_js': '//unpkg.com/jquery@2.2.4/dist/jquery.min.js',
    # 'swagger_ui_css': '//unpkg.com/swagger-ui-dist@3/swagger-ui.css',
})

swagger_config: dict = {
    **_swagger_config, **{
        "headers": [],
        "specs": [
            {
                "endpoint": 'api',
                "route": '/api.json',
                # "rule_filter": lambda rule: True,  # all in
                # Filter of werkzeug.routing.Rule object
                "rule_filter": lambda rule: 'apikey' in rule.endpoint,  # all in
                "model_filter": lambda tag: True,  # all in
            },
        #     {
        #         "endpoint": 'apispec2',
        #         "route": '/apispec2.json',
        #         "rule_filter": lambda rule: True,  # all in
        #         "model_filter": lambda tag: True,  # all in
        #     }
        ],
        "static_url_path": "/flasgger_static",
        # # "static_folder": "static",  # must be set by user
        "swagger_ui": True,
        "specs_route": "/api/ai/docs",
        "openapi": "3.0.2",
    }
}

# The LazyString values will be evaluated only when jsonify encodes the value
# at runtime, so you have access to Flask request, session, g, etc..
# and also may want to access a database
swagger_template: dict = {
    "swagger": "3.0",
    "info": {
        "title": "AI API",
        "description": "API for AI",
        "contact": {
            "responsibleOrganization": "Data Developer & Engineer",
            "responsibleDeveloper": "",
            "email": "korawica@mail.com",
            "url": "www.twitter.com/demo-korawica",
        },
        "termsOfService": "www.github.com/korawica",
        "version": "0.0.1"
    },
    # "host": 'localhost:5000',  #LazyString(lambda: request.host),
    "host": LazyString(lambda: str(request.host)),
    # the base path for blueprint registration.
    "basePath": "/apikey",  # "/api/ai"
    "schemes": [
        'http',
        # LazyString(lambda: 'https' if request.is_secure else 'http'),
    ],
    "securityDefinitions": {
        "Bearer": {
            "type": "apiKey",
            "name": "Authorization",
            "in": "header",
            "description": (
                    "JWT Authorization header using the Bearer scheme. "
                    "Example: \"Authorization: Bearer {token}\""
            )
        }
    },
}


SWAGGER_URL = '/api/ai/docs'
API_URL = '/swagger.json'
SWAGGER_UI_BLUEPRINT = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={
        'app_name': "Python-Flask-REST-Data-Application"
    }
)
