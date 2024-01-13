# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

from functools import wraps
import flask
import werkzeug
import werkzeug.wrappers
from werkzeug.datastructures import MultiDict
from urllib.parse import (
    urljoin,
    urlparse,
    unquote,
)


def response(*, mimetype: str = None, template_file: str = None):
    def response_inner(f):
        @wraps(f)
        def view_method(*args, **kwargs):
            response_val = f(*args, **kwargs)

            if isinstance(response_val, werkzeug.wrappers.Response):
                return response_val
            if isinstance(response_val, flask.Response):
                return response_val

            model = dict(response_val) if isinstance(response_val, dict) else dict()
            if template_file and not isinstance(response_val, dict):
                raise ValueError(
                    f"Invalid return type {type(response_val)}, "
                    f"we expected a dict as the return value."
                )

            if template_file:
                response_val = flask.render_template(template_file, **response_val)

            resp = flask.make_response(response_val)
            resp.model = model
            if mimetype:
                resp.mimetype = mimetype

            return resp
        return view_method
    return response_inner


class RequestDictionary(dict):
    def __init__(self, *args, default_val=None, **kwargs):
        self.default_val = default_val
        super().__init__(*args, **kwargs)

    def __getattr__(self, key):
        return self.get(key, self.default_val)


def create(default_val=None, **route_args) -> RequestDictionary:
    request = flask.request

    # Adding this retro actively. Some folks are experiencing issues where they
    # are getting a list rather than plain dict. I think it's from multiple
    # entries in the multi-dict. This should fix it.
    args = request.args
    if isinstance(request.args, MultiDict):
        args = request.args.to_dict()

    form = request.form
    if isinstance(request.args, MultiDict):
        form = request.form.to_dict()

    data = {
        **args,  # The key/value pairs in the URL query string
        **request.headers,  # Header values
        **form,  # The key/value pairs in the body, from a HTML post form
        **route_args  # And additional arguments the method access, if they want them merged.
    }

    return RequestDictionary(data, default_val=default_val)


"""
docs: https://speakerdeck.com/mitsuhiko/advanced-flask-patterns-1?slide=42
"""


def is_safe_url(target):
    ref_url = urlparse(flask.request.host_url)
    test_url = urlparse(urljoin(flask.request.host_url, target))
    return (test_url.scheme in {'http', 'https'}) and (ref_url.netloc == test_url.netloc)


def is_different_url(url):
    this_parts = urlparse(flask.request.url)
    other_parts = urlparse(url)
    return (this_parts[:4] != other_parts[:4]) and (unquote(this_parts.query) != unquote(other_parts.query))


def redirect_back(fallback):
    next_url = flask.request.args.get('next') or flask.request.referrer
    if next_url and is_safe_url(next_url) and is_different_url(next_url):
        return flask.redirect(next_url)
    return flask.redirect(fallback)
