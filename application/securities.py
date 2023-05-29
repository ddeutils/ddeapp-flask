# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import functools
from datetime import timedelta
from flask import (
    request,
    jsonify,
    make_response,
    current_app,
)
from conf import settings


def is_valid(api_key: str):
    """Check the value of `api_key` that must equal APIKEY, setup in .env file
    """
    return api_key == os.environ.get('APIKEY', 'NULL')


def apikey_required(func):
    """Required APIKEY decorator function for header of request checking before
    do everything with this application framework.
    """
    @functools.wraps(func)
    def decorator(*args, **kwargs):
        if not (api_key := request.headers.get("APIKEY")):
            resp = jsonify({"message": "Please provide an APIKEY in header"})
            resp.status_code = 400
            return resp
        if request.method in {"GET", "POST", "PUT", "DELETE"} and is_valid(api_key):
            return func(*args, **kwargs)
        resp = jsonify({"message": f"The provided API key, {api_key!r}, is not valid"})
        resp.status_code = 403
        return resp
    return decorator


def check_origin(func):
    """Define a custom decorator to check the origin of the request
    usage:
        ..> @app.route('/api/data')
        ... @check_origin
        ... def get_data():
        ...     data = {'message': 'Hello, world!'}
        ...     return jsonify(data)
    """
    def wrapper(*args, **kwargs):
        origin = request.headers.get('Origin')
        if origin in settings.ALLOWED_ORIGINS:
            response = func(*args, **kwargs)
            response.headers['Access-Control-Allow-Origin'] = origin
            response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
            return response
        else:
            return make_response(jsonify({'error': 'Invalid origin'}), 401)
    return wrapper


def login_manager_wrapper(login_manager):
    ...


def authenticate():
    ...


def crossdomain(
        origin=None,
        methods=None,
        headers=None,
        max_age=21600,
        attach_to_all=True,
        automatic_options=True
):
    """
    usage:
        ..> @app.route('/')
        ... @crossdomain(origin='*')
        ... def hello_world():
        ...     return 'Hello, World!'


        @app.route('/example')
        @crossdomain(origin='https://example.com')
        def example():
            return 'This route only allows requests from example.com'
    """
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))

    if headers is not None and not isinstance(headers, str):
        headers = ', '.join(x.upper() for x in headers)

    if not isinstance(origin, str):
        origin = ', '.join(origin)

    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))

            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers
            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return functools.update_wrapper(wrapped_function, f)
    return decorator
