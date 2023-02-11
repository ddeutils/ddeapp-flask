# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

from ....extensions import jwt_manager
from ..users.models import User
from .models import TokenBlockList


# this is a trigger called whenever you call function which have jwt_required()
# it will check if the token is revoked or not
# if true, it will throw err, else, it will allow user to access to the api
# docs: https://flask-jwt-extended.readthedocs.io/en/stable/blocklist_and_token_revoking/
@jwt_manager.token_in_blocklist_loader
def check_if_token_revoked(jwt_header, jwt_payload: dict) -> bool:
    jti = jwt_payload["jti"]
    token = TokenBlockList.query(TokenBlockList.id).filter_by(jti=jti).scalar()
    return token is not None


# Register a callback function that takes whatever object is passed in as the
# identity when creating JWTs and converts it to a JSON serializable format.
@jwt_manager.user_identity_loader
def user_identity_lookup(user):
    return user.id


# Register a callback function that loads a user from your database whenever
# a protected route is accessed. This should return any python object on a
# successful lookup, or None if the lookup failed for any reason (for example
# if the user has been deleted from the database).
@jwt_manager.user_lookup_loader
def user_lookup_callback(_jwt_header, jwt_data):
    identity = jwt_data["sub"]
    return User.query.filter_by(id=identity).one_or_none()
