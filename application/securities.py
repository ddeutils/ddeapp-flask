import os
import functools
from flask import (
    request,
    jsonify,
)


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
