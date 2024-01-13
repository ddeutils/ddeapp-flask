# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import queue
import logging

from flask import (
    Blueprint,
    jsonify
)

from ....core.errors import ValidateFormsError
from ....core.base import get_run_date
from ....core.utils.reusables import (
    random_sting,
    hash_string,
)
from ....core.utils.threads import ThreadWithControl
from ....core.models import (
    Result, Status,
)
from ....core.constants import (
    HTTP_200_OK,
    HTTP_401_UNAUTHORIZED
)
from ..framework.forms import (
    FormSetup,
    FormData,
    FormRetention,
)
from ..framework.tasks import (
    background_tasks,
    foreground_tasks,
)
from ....securities import apikey_required


logger = logging.getLogger(__name__)
frameworks = Blueprint('frameworks', __name__)


@frameworks.get('/')
def start_framework():
    """Health-Check Response route of framework component
    """
    run_id = get_run_date(fmt='%Y%m%d%H%M%S%f')[:-2]
    output_id = random_sting()
    bg_queue = queue.Queue()
    input_kwargs = {
        "output": output_id,
        "process_id": hash_string(run_id + output_id),
    }

    def background_tasks_demo(
            wait: int,
            _run_id: str,
            _bg_queue: queue.Queue,
            output=None,
            *args,
            **kwargs,
    ):
        """
        check background task can run with simple function
        """
        import time
        logger.info(f"Start: Thread was running with id: {_run_id!r} ...")
        _ = args
        _process_id = kwargs.get('process_id')
        _bg_queue.put(_process_id)
        time.sleep(wait)
        _bg_queue.put(True)
        for i in range(5):
            time.sleep(wait)
            logger.info(
                f"run_id: {_run_id!r} send log {i} from worker"
            )
        time.sleep(0.5)
        return background_tasks_demo_return(output, _process_id)

    def background_tasks_demo_return(output, token):
        logger.info(
            f"End: Thread run successful with output: {output!r} "
            f"and token: {token!r}"
        )
        return output

    # -------------------------- run with thread
    thread = ThreadWithControl(
        target=background_tasks_demo,
        args=(1, run_id, bg_queue, ),
        kwargs=input_kwargs,
        name='pipeline',
        daemon=True
    )
    thread.start()
    # threader.threadList.append(thread)
    # -------------------------- run with process
    # process = multiprocessing.Process(
    #     target=background_tasks_demo, args=input_args, kwargs=input_kwargs
    # )
    # process.start()

    resp = jsonify({
        'message': "Start: Background task was running ...",
        'process_id': f"{bg_queue.get()}",
        'process_name': f"{thread.name}"
    })
    resp.status_code = (
        HTTP_200_OK
        if bg_queue.get()
        else HTTP_401_UNAUTHORIZED
    )
    return resp


@frameworks.post('/setup')
@apikey_required
def run_setup():
    """Run setup module route
    :parameters:
        run_date: 'yyyy-mm-dd' : set run_date in ctr_data_pipeline after create
        pipeline_name: (optional) : Specific pipeline name want to run
        table_name: (optional) : specific table want to setup
        initial_data: ['Y', 'N', 'A', 'S', 'I'] : default 'N' : Execute the
            initial statement after create.
            'Y' : all table in default pipeline
            'A' : only 'ai_' table prefix in default pipeline
            'S' : only 'src_' table prefix in default pipeline
            'I' : only 'imp_' table prefix in default pipeline
        drop_before_create: ['Y', 'N', 'C', 'A', 'S', 'I'] : default 'N' : Drop
            table before create
            'Y' : all table in default pipeline
            'C' : only 'ctr_' table prefix in default pipeline
            'A' : only 'ai_' table prefix in default pipeline
            'S' : only 'src_' table prefix in default pipeline
            'I' : only 'imp_' table prefix in default pipeline
        drop_table: ['Y', 'N'] : default 'N' : Force drop table in AI schema
        drop_scheme: ['Y', 'N'] : default 'N' : Force drop AI schema in database
        cascade: ['Y', 'N'] : default 'N' : Force cascade
        background: ['Y', 'N'] : default 'Y': Run with background task

    :warning:
        - If 'drop_schema' == 'Y', then this module will ignore all input
        parameters.
        - 'pipeline_name' and 'table_name' must exists only one in data forms.
    """
    try:
        parameters: dict = FormSetup().as_dict()
        status: int = Status.SUCCESS
        optional: dict = {}
    except ValidateFormsError as error:
        logger.error(str(error))
        return jsonify({'message': str(error)}), HTTP_401_UNAUTHORIZED

    bg_queue = queue.Queue()
    if parameters['background'] == 'Y':
        thread = ThreadWithControl(
            target=background_tasks,
            args=('setup', bg_queue, parameters),
            daemon=True,
        )
        thread.start()
        process_id = bg_queue.get()
        process_name = bg_queue.get()
        message: str = (
            f"Start running process_name: {process_name!r} in background. "
            f"Monitoring task should select table 'ctr_task_process' "
            f"where process_id = '{process_id}'."
        )
        optional['process_id'] = process_id
    else:
        result: Result = foreground_tasks('setup', parameters)
        message: str = result.message
        status: Status = result.status

    return jsonify({
        "message": message,
        **optional
    }), (
        HTTP_401_UNAUTHORIZED
        if status != Status.SUCCESS
        else HTTP_200_OK
    )


@frameworks.post('/data')
@apikey_required
def run_data():
    """
    :parameters:
        run_date: 'yyyy-mm-dd' : "The date want to run"
        pipeline_name: (optional) : "Specific pipeline name want to run"
        table_name: (optional) : "Specific table want to run"
        run_mode: ['common', 'rerun'] : default 'common' : "Mode want to run"
            'common' : "run data in common mode that reference data_date in
                    ctr_data_pipeline table"
            'rerun' : "run data in rerun mode that ignore data_date in
                    ctr_data_pipeline table"
        background: ['Y', 'N'] : default 'Y': "Run with background task"

    :warning:
        'pipeline_name' and 'table_name' must exist only one in data forms
    """
    try:
        parameters: dict = FormData().as_dict()
        status: int = 0
        optional: dict = {}
    except ValidateFormsError as error:
        logger.error(str(error))
        resp = jsonify({'message': str(error)})
        resp.status_code = HTTP_401_UNAUTHORIZED
        return resp

    bg_queue = queue.Queue()
    if parameters['background'] == 'Y':
        thread = ThreadWithControl(
            target=background_tasks,
            args=('data', bg_queue, parameters),
            daemon=True
        )
        thread.start()
        process_id = bg_queue.get()
        process_name = bg_queue.get()
        message: str = (
            f"Start running process_name: {process_name!r} in background. "
            f"Monitoring task should select table 'ctr_task_process' "
            f"where process_id = '{process_id}'."
        )
        optional['process_id'] = process_id
    else:
        result: Result = foreground_tasks('data', parameters)
        message: str = result.message
        status: Status = result.status

    return jsonify({
        "message": message,
        **optional
    }), (
        HTTP_401_UNAUTHORIZED
        if status != Status.SUCCESS
        else HTTP_200_OK
    )


@frameworks.post('/retention')
@apikey_required
def run_rtt():
    """
    :parameter:
        run_date: 'yyyy-mm-dd' : "The date want to run data retention mode"
        pipeline_name: (optional) : "Specific pipeline name want to run"
        table_name: (optional) : Specific table name want to run"
        backup_table: (optional) : Backup data in current schema to the new
        backup_schema: (optional) : Backup the table in current schema to the
                                    new
        background: ['Y', 'N'] : default 'Y': "Run with background task"

    :warning:
        'pipeline_name' and 'table_name' must exist only one in data forms
    """
    try:
        parameters: dict = FormRetention().as_dict()
        status: int = 0
        optional: dict = {}
    except ValidateFormsError as error:
        logger.error(str(error))
        resp = jsonify({'message': str(error)})
        resp.status_code = HTTP_401_UNAUTHORIZED
        return resp

    bg_queue = queue.Queue()
    if parameters['background'] == 'Y':
        thread = ThreadWithControl(
            target=background_tasks,
            args=('retention', bg_queue, parameters),
            daemon=True,
        )
        thread.start()
        process_id = bg_queue.get()
        process_name = bg_queue.get()
        message = (
            f"Start running process_name: {process_name!r} in background. "
            f"Monitoring task should select table 'ctr_task_process' "
            f"where process_id = '{process_id}'."
        )
        optional['process_id'] = process_id
    else:
        result: Result = foreground_tasks('retention', parameters)
        message: str = result.message
        status: Status = result.status

    return jsonify({
        "message": message,
        **optional
    }), (
        HTTP_401_UNAUTHORIZED
        if status != Status.SUCCESS
        else HTTP_200_OK
    )
