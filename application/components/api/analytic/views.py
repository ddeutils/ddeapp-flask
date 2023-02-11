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
from ....utils.logging_ import logging
from ....utils.validations import validate_table_short
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
        process_id: int: The process_id that returning after request to run data or ingestion

    returns:
        message:
        logging:
        status:
        percent: 0.00 to 1.00
    """
    log_length = 50
    process, status = get_operation_process(escape(process_id))
    logger.info(f"[{'=' * math.floor(status.percent * log_length)}"
                f"{' ' * (log_length - math.floor(status.percent * log_length))}] ({status.percent:.2%}) "
                f"> {process.id}")
    resp = jsonify({
        'message': status.message,
        'logging': process.messages,
        'status': process.status,
        'percent': status.percent
    })
    resp.status_code = 200
    return resp


@analytics.route('/dpc/', methods=['GET'])
@analytics.route('/dpc/<path:tbl_name_short>', methods=['GET'])
@apikey_required
def get_dependency(tbl_name_short: str = None):
    """
    arguments:
        tbl_name_short: path

    returns:
        message:
        status:
        dependency: Dict[<process>, Dict[]]
    """
    if not tbl_name_short:
        resp = jsonify({'message': "Error: Get dependency does not support get all yet"})
        resp.status_code = 401
        return resp
    elif validate_table_short(tbl_name_short):
        resp = jsonify({'message': "Error: Please specific `table_short_name` that want to get data dependency"})
        resp.status_code = 401
        return resp

    process, status = get_dependency_data(tbl_name_short)
    resp = jsonify({
        'message': process.messages,
        'status': process.status,
        'dependency': status.mapping
    })
    resp.status_code = 401 if process.status == 1 else 200
    return resp
