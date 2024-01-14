from functools import wraps

from flask import (
    flash,
    make_response,
    redirect,
    request,
    url_for,
)
from flask_login import current_user


def admin_required(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        if current_user.is_authenticated and any("Administrator" in role.name for role in current_user.roles):
            return f(*args, **kwargs)
        flash("You need to be an admin to view this page.", "danger")

        if 'Hx-Request' in request.headers:
            _response = make_response()
            _response.headers["HX-Redirect"] = request.args.get('next') or url_for('home')
            _response.status_code = 200
            return _response
        return redirect(url_for('home'))
    return wrap
