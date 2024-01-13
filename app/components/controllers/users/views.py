# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import secrets
from PIL import Image
from markupsafe import Markup
from flask import (
    Blueprint,
    render_template,
    flash,
    redirect,
    url_for,
    request,
    current_app,
    make_response,
)
from flask_login import (
    login_user,
    current_user,
    logout_user,
    login_required,
)

from ....extensions import (
    bcrypt,
    db,
    limiter,
)
from .forms import (
    RegistrationForm,
    LoginForm,
    UpdateAccountForm,
    RequestResetForm,
    ResetPasswordForm
)
from .tasks import (
    send_reset_token,
    send_reset_email,
)
from .models import User

users = Blueprint('users', __name__, template_folder='templates')


@users.get("/register")
def register_get():
    form = RegistrationForm()
    if 'Hx-Request' in request.headers:
        return render_template('users/partials/register.html', form=form)
    return render_template('users/users_base.html', target='register', form=form)


@users.post("/register")
@limiter.limit("1/second", override_defaults=False)
def register_post():
    if current_user.is_authenticated:
        _response = make_response()
        _response.headers["HX-Redirect"] = url_for('home')
        _response.status_code = 200
        return _response

    form = RegistrationForm()
    if form.validate_on_submit():
        hashed_password = bcrypt.generate_password_hash(form.password.data).decode('utf-8')
        user = User(**{
            'username': form.username.data,
            'email': form.email.data,
            'password': hashed_password
        })
        db.session.add(user)
        db.session.commit()
        flash(
            'Your account has been created! You are now able to log in',
            'success',
        )
        return redirect(url_for('users.login_get'))
    return render_template('users/partials/register.html', form=form)


@users.get("/login")
def login_get():
    form = LoginForm()
    if 'Hx-Request' in request.headers:
        return render_template('users/partials/login.html', form=form)
    return render_template('users/users_base.html', target='login', form=form)


@users.post("/login")
def login_post():
    if current_user.is_authenticated:
        _response = make_response()
        _response.headers["HX-Redirect"] = url_for('home')
        _response.status_code = 200
        return _response

    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if user and bcrypt.check_password_hash(user.password, form.password.data):
            login_user(user, remember=form.remember.data)
            _response = make_response()
            _response.headers["HX-Redirect"] = request.args.get('next') or url_for('home')
            _response.status_code = 200
            return _response
        else:
            flash(
                Markup(render_template(
                    '/users/flash/alert_card.html',
                    topic='Login Unsuccessful!',
                    message='Please check email and password'
                )),
                'danger',
            )
    return render_template('users/partials/login.html', form=form)


@users.route("/logout")
@login_required
def logout():
    logout_user()
    _response = make_response()
    _response.headers["HX-Redirect"] = url_for('home')
    _response.status_code = 200
    return _response


def save_picture(from_picture):
    random_hex = secrets.token_hex(8)

    from werkzeug.utils import secure_filename
    filename = secure_filename(from_picture.filename)

    _, f_ext = os.path.splitext(filename)
    picture_fn = random_hex + f_ext
    picture_path = os.path.join(current_app.root_path, 'static/images', picture_fn)

    output_size = (125, 125)
    i = Image.open(from_picture)
    i.thumbnail(output_size)
    i.save(picture_path)

    return picture_fn


@users.route("/account", methods=["GET", "POST"])
@login_required
def account():
    form = UpdateAccountForm()
    if form.validate_on_submit():
        if form.picture.data:
            picture_file = save_picture(form.picture.data)
            current_user.image_file = picture_file
        current_user.username = form.username.data
        current_user.email = form.email.data
        db.session.commit()
        flash('your account has been updated!', 'success')
        return redirect(url_for('users.account'))
    elif request.method == 'GET':
        form.username.data = current_user.username
        form.email.data = current_user.email
    image_file = url_for(
        'static', filename=f'assets/images/{current_user.image_file}'
    )
    return render_template('users/account.html', image_file=image_file, form=form)


@users.route("/reset_password", methods=["GET", "POST"])
def reset_request():
    if current_user.is_authenticated:
        _response = make_response()
        _response.headers["HX-Redirect"] = url_for('home')
        _response.status_code = 200
        return _response

    form = RequestResetForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        send_reset_email(user)
        send_reset_token(user)
        flash('An email has been sent with instructions to reset your password.', 'info')
        return redirect(url_for('users.login_get'))

    return render_template('users/users_base.html', target='reset_request', form=form)


@users.route("/reset_password/<token>", methods=["GET", "POST"])
def reset_token(token):
    if current_user.is_authenticated:
        _response = make_response()
        _response.headers["HX-Redirect"] = url_for('home')
        _response.status_code = 200
        return _response

    user = User.verify_reset_token(token)
    if user is None:
        flash('That is an invalid token or expired token', 'warning')
        return redirect(url_for('users.reset_request'))
    form = ResetPasswordForm()
    if form.validate_on_submit():
        hashed_password = bcrypt.generate_password_hash(form.password.data).decode('utf-8')
        user.password = hashed_password
        db.session.commit()
        flash('Your password has been updated! You are now able to log in', 'success')
        return redirect(url_for('users.login_get'))
    return render_template('users/reset_token.html', form=form)
