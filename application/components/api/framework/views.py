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
from ....errors import ValidateFormsError
from ....securities import apikey_required
from ....utils.base import get_run_date
from ....utils.reusables import (
    random_sting,
    hash_string,
)
from ....utils.thread_ import ThreadWithControl
from ....utils.objects import Process
from ..framework.forms import (
    FormSetup,
    FormData,
    FormRetention,
)
from ..framework.tasks import (
    background_tasks,
    foreground_tasks,
)

logger = logging.getLogger(__name__)

frameworks = Blueprint('frameworks', __name__)


@frameworks.get('/')
def start_framework():
    """
    check response
    """
    run_id = get_run_date(fmt='%Y%m%d%H%M%S%f')[:-2]
    output_id = random_sting()
    process_id = hash_string(run_id + output_id)
    bg_queue = queue.Queue()
    input_args = (1, run_id, bg_queue,)
    input_kwargs = {"output": output_id, "process_id": process_id}

    def background_tasks_demo(wait: int, _run_id: str, _bg_queue: queue.Queue, output=None, *args, **kwargs):
        """
        check background task can run with simple function
        """
        import time
        logger.info(f"Start: Thread was running with id: {_run_id!r} ...")
        _process_id = kwargs.get('process_id')
        _bg_queue.put(_process_id)
        time.sleep(wait)
        _bg_queue.put(True)
        for i in range(5):
            time.sleep(wait)
            logger.info(f"run_id: {_run_id!r} send log {i} from worker")
        time.sleep(0.5)
        return background_tasks_demo_return(output, _process_id)

    def background_tasks_demo_return(output, token):
        logger.info(f"End: Thread run successful with output: {output!r} and token: {token!r}")
        return output

    # -------------------------- run with thread
    thread = ThreadWithControl(target=background_tasks_demo, args=input_args, kwargs=input_kwargs,
                               name='pipeline', daemon=True)
    thread.start()
    # threader.threadList.append(thread)
    # -------------------------- run with process
    # process = multiprocessing.Process(target=background_tasks_demo, args=input_args, kwargs=input_kwargs)
    # process.start()

    resp = jsonify({
        'message': "Start: Background task was running ...",
        'process_id': f"{bg_queue.get()}",
        'process_name': f"{thread.name}"
    })
    resp.status_code = 200 if bg_queue.get() else 401
    return resp


@frameworks.post('/setup')
@apikey_required
def run_setup():
    """
    :parameters:
        run_date: 'yyyy-mm-dd' : "set run_date in ctr_data_pipeline after create"
        pipeline_name: (optional) : "Specific pipeline name want to run"
        table_name: (optional) : "specific table want to setup"
        initial_data: ['Y', 'N', 'A', 'S', 'I'] : default 'N' : "Excecute the initial statement after create"
            'Y' : "all table in default pipeline"
            'A' : "only 'ai_' table prefix in default pipeline"
            'S' : "only 'src_' table prefix in default pipeline"
            'I' : "only 'imp_' table prefix in default pipeline"
        drop_before_create: ['Y', 'N', 'C', 'A', 'S', 'I'] : default 'N' : "Drop table before create"
            'Y' : "all table in default pipeline"
            'C' : "only 'ctr_' table prefix in default pipeline"
            'A' : "only 'ai_' table prefix in default pipeline"
            'S' : "only 'src_' table prefix in default pipeline"
            'I' : "only 'imp_' table prefix in default pipeline"
        drop_table: ['Y', 'N'] : default 'N' : "Force drop table in AI schema"
        drop_scheme: ['Y', 'N'] : default 'N' : "Force drop AI schema in database"
        cascade: ['Y', 'N'] : default 'N' : "Force cascade"
        background: ['Y', 'N'] : default 'Y': "Run with background task"

    :warning:
        - If 'drop_schema' == 'Y', then this module will ignore all input parameters
        - 'pipeline_name' and 'table_name' must exists only one in data forms
    """
    try:
        parameters: dict = FormSetup().as_dict()
        status: int = 0
        optional: dict = {}
    except ValidateFormsError as error:
        logger.error(str(error))
        resp = jsonify({'message': str(error)})
        resp.status_code = 401
        return resp

    bg_queue = queue.Queue()
    if parameters['background'] == 'Y':
        thread = ThreadWithControl(target=background_tasks, args=('setup', bg_queue, parameters), daemon=True)
        thread.start()
        process_id = bg_queue.get()
        process_name = bg_queue.get()
        messages = f"Start running process_name: {process_name!r} in background. Monitoring task should " \
                   f"select table 'ctr_task_process' where process_id = '{process_id}'."
        optional['process_id'] = process_id
    else:
        process: Process = foreground_tasks('setup', parameters)
        messages: str = process.messages
        status: int = process.status

    resp = jsonify({"message": messages, **optional})
    resp.status_code = 401 if status > 0 else 200
    return resp


@frameworks.post('/data')
@apikey_required
def run_data():
    """
    :parameters:
        run_date: 'yyyy-mm-dd' : "The date want to run"
        pipeline_name: (optional) : "Specific pipeline name want to run"
        table_name: (optional) : "Specific table want to run"
        run_mode: ['common', 'rerun'] : default 'common' : "Mode want to run"
            'common' : "run data in common mode that reference data_date in ctr_data_pipeline table"
            'rerun' : "run data in rerun mode that ignore data_date in ctr_data_pipeline table"
        background: ['Y', 'N'] : default 'Y': "Run with background task"

    :warning:
        'pipeline_name' and 'table_name' must exists only one in data forms
    """
    try:
        parameters: dict = FormData().as_dict()
        status: int = 0
        optional: dict = {}
    except ValidateFormsError as error:
        logger.error(str(error))
        resp = jsonify({'message': str(error)})
        resp.status_code = 401
        return resp

    bg_queue = queue.Queue()
    if parameters['background'] == 'Y':
        thread = ThreadWithControl(target=background_tasks, args=('data', bg_queue, parameters), daemon=True)
        thread.start()
        process_id = bg_queue.get()
        process_name = bg_queue.get()
        messages = f"Start running process_name: {process_name!r} in background. Monitoring task should " \
                   f"select table 'ctr_task_process' where process_id = '{process_id}'."
        optional['process_id'] = process_id
    else:
        process: Process = foreground_tasks('data', parameters)
        messages: str = process.messages
        status: int = process.status

    resp = jsonify({"message": messages, **optional})
    resp.status_code = 401 if status > 0 else 200
    return resp


@frameworks.post('/retention')
@apikey_required
def run_rtt():
    """
    :parameter:
        run_date: 'yyyy-mm-dd' : "The date want to run data retention mode"
        pipeline_name: (optional) : "Specific pipeline name want to run"
        table_name: (optional) : "Specific table name want to run"
        backup_table: (optional) : "Backup data in current AI schema to new schema"
        backup_schema: (optional) : "Backup the table in current AI schema to new schema"
        background: ['Y', 'N'] : default 'Y': "Run with background task"

    :warning:
        'pipeline_name' and 'table_name' must exists only one in data forms
    """
    try:
        parameters: dict = FormRetention().as_dict()
        status: int = 0
        optional: dict = {}
    except ValidateFormsError as error:
        logger.error(str(error))
        resp = jsonify({'message': str(error)})
        resp.status_code = 401
        return resp

    bg_queue = queue.Queue()
    if parameters['background'] == 'Y':
        thread = ThreadWithControl(target=background_tasks, args=('retention', bg_queue, parameters), daemon=True)
        thread.start()
        process_id = bg_queue.get()
        process_name = bg_queue.get()
        messages = f"Start running process_name: {process_name!r} in background. Monitoring task should " \
                   f"select table 'ctr_task_process' where process_id = '{process_id}'."
        optional['process_id'] = process_id
    else:
        process: Process = foreground_tasks('retention', parameters)
        messages: str = process.messages
        status: int = process.status

    resp = jsonify({"message": messages, **optional})
    resp.status_code = 401 if status > 0 else 200
    return resp
