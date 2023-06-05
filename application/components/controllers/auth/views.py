# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import pytz
from datetime import datetime
from flask import (
    Blueprint,
    request,
    jsonify,
)
from flask_jwt_extended import (
    jwt_required,
    create_access_token,
    create_refresh_token,
    get_jwt_identity,
    get_jwt,
    get_jti,
    current_user,
)
from flasgger import swag_from
from email_validator import validate_email
from ....core.constants import (
    HTTP_200_OK,
    HTTP_201_CREATED,
    HTTP_401_UNAUTHORIZED,
    HTTP_400_BAD_REQUEST,
    HTTP_409_CONFLICT,
)
from ....extensions import (
    bcrypt,
    db
)
from ..users.models import User
from .models import TokenBlockList


auth = Blueprint("auth", __name__)


@auth.post('/register')
# @swag_from('./docs/auth/register.yaml')
def register():
    username = request.json['username']
    email = request.json['email']
    password = request.json['password']

    if len(password) < 6:
        return jsonify({'error': "Password is too short"}), HTTP_400_BAD_REQUEST

    if len(username) < 3:
        return jsonify({'error': "User is too short"}), HTTP_400_BAD_REQUEST

    if not username.isalnum() or " " in username:
        return jsonify({
            'error': "Username should be alphanumeric, also no spaces"
        }), HTTP_400_BAD_REQUEST

    if not validate_email(email):
        return jsonify({'error': "Email is not valid"}), HTTP_400_BAD_REQUEST

    if User.query.filter_by(email=email).first() is not None:
        return jsonify({'error': "Email is taken"}), HTTP_409_CONFLICT

    if User.query.filter_by(username=username).first() is not None:
        return jsonify({'error': "username is taken"}), HTTP_409_CONFLICT

    hashed_password = bcrypt.generate_password_hash(password).decode('utf-8')
    user = User(
        username=username,
        email=email,
        password=hashed_password
    )
    db.session.add(user)
    db.session.commit()

    return jsonify({
        'message': "Your account has been created! You are now able to log in",
        'user': {
            'username': username,
            "email": email
        },
    }), HTTP_201_CREATED


@auth.post('/token')
@auth.post('/login')
@swag_from('./docs/auth/login.yaml')
def login():
    email = request.json.get('email', '')
    password = request.json.get('password', '')

    if user := User.query.filter_by(email=email).first():
        if bcrypt.check_password_hash(user.password, password):

            # add additional info, such as aud
            additional_claims = {"aud": request.host}

            # create refresh token
            refresh_token = create_refresh_token(
                identity=user.public_id, additional_claims=additional_claims
            )

            # add refresh token to additional_claims,
            # so we can use it later in the revoke part in logout action
            additional_claims['refresh_token'] = refresh_token

            access_token = create_access_token(
                identity=user.public_id, additional_claims=additional_claims
            )
            return jsonify({
                'user': {
                    'refresh_token': refresh_token,
                    'access_token': access_token,
                    'username': user.username,
                    'email': user.email
                }
            }), HTTP_200_OK
    return jsonify({
        'error': 'Wrong credentials! Please check email and password'
    }), HTTP_401_UNAUTHORIZED


@auth.get("/protected")
@jwt_required()
def protected():
    claims = get_jwt()
    allowed_auds = []

    if "aud" in claims and claims["aud"] in allowed_auds:
        # we can use current_user whenever we want,
        # just need to add jwt_required and import current_user
        # and add function user_lookup_callback,
        # and then, it's free to use
        # docs: https://flask-jwt-extended.readthedocs.io/en/stable/automatic_user_loading/
        return jsonify({
            "id": current_user.id,
            'email': current_user.email,
            'username': current_user.username,
            "aud": claims['aud']
        }), HTTP_200_OK
    return jsonify({
        "message": f"This url '{claims['aud']}' is not allowed",
    }), HTTP_401_UNAUTHORIZED


@auth.get('/refresh')
@jwt_required(refresh=True)
def refresh_users_token():
    identity = get_jwt_identity()

    # get claims
    claims = get_jwt()

    # extract from claims to additional_claims
    additional_claims = {x: claims[x] for x in {'aud'}}

    # add refresh token from header to additional_claims
    refresh_token = request.headers.get('HTTP_AUTHORIZATION').replace('Bearer ', '')
    additional_claims['refresh_token'] = refresh_token

    access = create_access_token(identity=identity, additional_claims=additional_claims)
    return jsonify({
        'access_token': access
    }), HTTP_200_OK


@auth.delete('/logout')
@jwt_required(verify_type=False)
def logout():
    now = datetime.now(pytz.utc)

    # Revoke access token by adding the token information to table TokenBlockList
    # then we will use token_block_list to check revoked token in function check_if_token_revoked
    token = get_jwt()
    jti = token["jti"]
    ttype = token["type"]
    token_block_list = TokenBlockList(jti=jti, type=ttype, create_at=now)
    db.session.add(token_block_list)

    # Revoke refresh token by adding the token information to table TokenBlockList
    # then we will use token_block_list to check revoked token in function check_if_token_revoked
    refresh_token = token['refresh_token']
    jti_refresh_token = get_jti(refresh_token)
    ttype = "refresh"
    token_block_list = TokenBlockList(jti=jti_refresh_token, type=ttype, create_at=now)
    db.session.add(token_block_list)
    db.session.commit()

    # Returns "Access token revoked" or "Refresh token revoked"
    return jsonify({
        "message": f"{ttype.capitalize()} and refresh token successfully revoked"
    }), HTTP_200_OK
