# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

from flask import request
from flasgger import (
    Swagger,
    LazyString,
)

"""
docs:
    - https://github.com/CryceTruly/bookmarker-api
    - https://kanoki.org/2020/07/18/python-api-documentation-using-flask-swagger/
"""


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
                "endpoint": 'apispec',
                "route": '/apispec.json',
                "rule_filter": lambda rule: True,  # all in
                "model_filter": lambda tag: True,  # all in
            },
            {
                "endpoint": 'apispec2',
                "route": '/apispec2.json',
                "rule_filter": lambda rule: True,  # all in
                "model_filter": lambda tag: True,  # all in
            }
        ],
        "static_url_path": "/flasgger_static",
        # "static_folder": "static",  # must be set by user
        "swagger_ui": True,
        "specs_route": "/api/ai/docs",
    }
}

# The LazyString values will be evaluated only when jsonify encodes the value at runtime,
# so you have access to Flask request, session, g, etc.. and also may want to access a database
swagger_template: dict = {
    "swagger": "2.0",
    "info": {
        "title": "AI API",
        "description": "API for AI",
        "contact": {
            "responsibleOrganization": "",
            "responsibleDeveloper": "",
            # "email": "demo@gmail.com",
            # "url": "www.twitter.com/demo",
        },
        # "termsOfService": "www.twitter.com/demo",
        "version": "0.0.1"
    },
    "host": LazyString(lambda: request.host),
    # the base path for blueprint registration.
    "basePath": "/api/ai",
    "schemes": [LazyString(lambda: 'https' if request.is_secure else 'http')],
    "securityDefinitions": {
        "Bearer": {
            "type": "apiKey",
            "name": "Authorization",
            "in": "header",
            "description": "JWT Authorization header using "
                           "the Bearer scheme. Example: \"Authorization: Bearer {token}\""
        }
    },
}
