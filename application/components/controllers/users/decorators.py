import jwt
from functools import wraps
from datetime import datetime
from flask import (
    flash,
    redirect,
    url_for,
    session,
    request,
    jsonify,
    current_app,
    abort,
    g
)
from flask_login import current_user
from .models import User


def check_expired(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        if datetime.utcnow() > current_user.account_expires:
            flash("Your account has expired. Update your billing info.")
            return redirect(url_for('account_billing'))
        return func(*args, **kwargs)

    return decorated_function


def login_required_session(fn):
    @wraps(fn)
    def inner(*args, **kwargs):
        if session.get('logged_in'):
            return fn(*args, **kwargs)
        return redirect(url_for('login', next=request.path))
    return inner


# Note:
# The next value will exist in request.args after a GET request for the login page.
# You’ll have to pass it along when sending the POST request from the login form.
# You can do this with a hidden input tag, then retrieve it from request.form when
# logging the user in.
#
#       `<input type="hidden" value="{{ request.args.get('next', '') }}"/>`
#
# docs: https://flask.palletsprojects.com/en/latest/patterns/viewdecorators/
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if g.user is None:
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function


def requires_roles(*roles):
    def wrapper(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            # Check if the user is authenticated
            if not current_user.is_authenticated:
                return redirect(url_for('login'))

            if any(role.name not in roles for role in current_user.roles):
                return abort(403)
            return f(*args, **kwargs)

        return wrapped

    return wrapper


def token_required(f):
    """
    docs: https://www.bacancytechnology.com/blog/flask-jwt-authentication
    """

    @wraps(f)
    def decorator(*args, **kwargs):
        token = None
        if 'x-access-tokens' in request.headers:
            token = request.headers['x-access-tokens']
        if not token:
            return jsonify({'message': 'a valid token is missing'})
        try:
            data = jwt.decode(
                token,
                current_app.config['SECRET_KEY'],
                algorithms=["HS256"]
            )
            _current_user = User.query.filter_by(public_id=data['public_id']).first()
        except jwt.InvalidTokenError:
            return jsonify({'message': 'token is invalid'})
        return f(_current_user, *args, **kwargs)

    return decorator


def permission_required(permission):
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Check if the user is authenticated
            if not current_user.is_authenticated:
                return redirect(url_for('login'))

            # Check if the user has the required permission
            if not current_user.has_permission(permission):
                return abort(403)
            return f(*args, **kwargs)

        return decorated_function

    return decorator
