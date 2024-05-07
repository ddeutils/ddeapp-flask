# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------

from flask import Blueprint, jsonify, render_template, request

from app.core.utils.logging_ import logging

from ....core.constants import (
    HTTP_403_FORBIDDEN,
    HTTP_404_NOT_FOUND,
    HTTP_405_METHOD_NOT_ALLOWED,
    HTTP_429_TOO_MANY_REQUESTS,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

logger = logging.getLogger(__name__)
errors = Blueprint("errors", __name__, template_folder="templates")


@errors.app_errorhandler(HTTP_403_FORBIDDEN)
@errors.app_errorhandler(HTTP_404_NOT_FOUND)
@errors.app_errorhandler(HTTP_405_METHOD_NOT_ALLOWED)
@errors.app_errorhandler(HTTP_429_TOO_MANY_REQUESTS)
@errors.app_errorhandler(HTTP_500_INTERNAL_SERVER_ERROR)
def error_handler(error):
    """Error Handler route."""
    if request.path.startswith("/api"):
        logger.error(str(error))
        return jsonify(error=str(error)), error.code
    return (
        render_template("errors/base.html", error_code=error.code),
        error.code,
    )
