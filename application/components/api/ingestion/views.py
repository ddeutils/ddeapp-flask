# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import logging
import queue
from typing import Optional
from flask import (
    Blueprint,
    jsonify,
    request,
)
from ....utils.thread_ import ThreadWithControl
from ....utils.objects import Process
from ....securities import apikey_required
from ..ingestion.forms import FormIngest
from ..ingestion.tasks import (
    ingestion_background,
    ingestion_foreground,
)
from ....errors import ValidateFormsError

ingestion = Blueprint('ingestion', __name__)

logger = logging.getLogger(__name__)


@ingestion.route('put/', methods=['PUT'])
@ingestion.route('del/', methods=['DELETE'])
@ingestion.route('put/<path:tbl_name_short>', methods=['PUT'])
@ingestion.route('del/<path:tbl_name_short>', methods=['DELETE'])
@apikey_required
def ingestion_json(tbl_name_short: Optional[str] = None):
    """Receive json data and insert into table with identify by short name

    :argument:
        tbl_name_short: path: "The short name of target table that want to ingest data"

    :parameter:
        run_date: yyyy-mm-dd : default `today()` : "The date want to run"
        update_date: yyyy-mm-dd hh:MM:SS : default `now()` : "The datetime update this data"
        mode: ['update', insert'] : default 'insert': Mode want to action in target table
            'update' : update
            'insert' : insert
        ingest_mode: ['common', 'merge'] : default 'merge': Mode want to ingest
            'common' : common mode
            'merge' : merge mode
        background: ['Y', 'N'] : default 'Y' : "Run with background task"
        data: Union[list, dict] : "data for ingest to target table. `not null` property
            should exists in data and `default` or `serial` property should not exists"
    """
    try:
        if not tbl_name_short:
            raise ValidateFormsError(
                'tbl_name_short', message="specific target in `/api/ai/<table_short_name>` does not exists"
            )
        parameters: dict = FormIngest.add(value={'tbl_name_short': tbl_name_short}).as_dict()
        status: int = 0
        optional: dict = {}
    except ValidateFormsError as error:
        logger.error(str(error))
        resp = jsonify({'message': str(error)})
        resp.status_code = 401
        return resp

    bg_queue = queue.Queue()
    if request.method == 'DELETE':
        raise NotImplementedError("`DELETE` does not implement yet")

    if parameters['background'] == 'Y':
        thread = ThreadWithControl(
            target=ingestion_background, args=('payload', bg_queue, parameters), daemon=True
        )
        thread.start()
        process_id = bg_queue.get()
        table_name = bg_queue.get()
        messages = f"Start running ingest data to {table_name!r} in background. Monitoring task should " \
                   f"select table 'ctr_task_process' where process_id = '{process_id}'."
        optional['process_id'] = process_id
    else:
        process: Process = ingestion_foreground('payload', parameters)
        messages: str = process.messages
        status: int = process.status

    resp = jsonify({'message': messages, **optional})
    resp.status_code = 401 if status > 0 else 200
    return resp
