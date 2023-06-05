# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import math
from markupsafe import escape
from flask import (
    Blueprint,
    jsonify,
)
from application.core.models import (
    Status,
    Result,
)
from application.core.constants import (
    HTTP_200_OK,
    HTTP_401_UNAUTHORIZED
)
from application.core.utils.logging_ import logging
from application.components.api.validations import validate_table_short
from ....securities import apikey_required
from ..analytic.tasks import (
    get_operation_process,
    get_dependency_data,
)


analytics = Blueprint('analytics', __name__)
logger = logging.getLogger(__name__)


@analytics.route('/opt/<int:process_id>', methods=['GET'])
@apikey_required
def get_operation(process_id):
    """
    arguments:
        process_id: int: The process_id that returning after request to
                run data or ingestion.
    returns:
        message: str
        logging: str
        status: Status
        percent: float range from 0.00 to 1.00
    """
    log_length = 50
    result = get_operation_process(escape(process_id))
    logger.info(
        f"[{'=' * math.floor(result.percent * log_length)}"
        f"{' ' * (log_length - math.floor(result.percent * log_length))}] "
        f"({result.percent:.2%}) > {process_id}"
    )
    return jsonify({
        'message': result.message,
        'logging': result.logging,
        'status': result.status,
        'percent': result.percent
    }), HTTP_200_OK


@analytics.route('/dpc/', methods=['GET'])
@analytics.route('/dpc/<path:tbl_name_short>', methods=['GET'])
@apikey_required
def get_dependency(tbl_name_short: str = None):
    """
    arguments:
        tbl_name_short: path
    returns:
        message: str
        status: Status
        dependency: Dict[<process>, Dict[]]
    """
    if not tbl_name_short:
        resp = jsonify({
            'message': "Error: Get dependency does not support get all yet"
        })
        return resp, HTTP_401_UNAUTHORIZED
    elif validate_table_short(tbl_name_short):
        resp = jsonify({
            'message': (
                "Error: Please specific `table_short_name` "
                "that want to get data dependency"
            )
        })
        return resp, HTTP_401_UNAUTHORIZED

    result: Result = get_dependency_data(tbl_name_short)
    return jsonify({
        'message': result.message,
        'status': result.status,
        'dependency': result.mapping
    }), (
        HTTP_200_OK
        if result.status == Status.SUCCESS
        else HTTP_401_UNAUTHORIZED
    )
