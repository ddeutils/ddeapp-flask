# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import queue
from ....errors import ObjectBaseError
from ....utils.base import get_plural
from ....utils.config import Params
from ....utils.logging_ import logging
from ....utils.objects import (
    Node,
    Process,
    Status,
    ObjectType,
)

logger = logging.getLogger(__name__)

registers = Params(param_name='registers.yaml')


def ingest_payload(
        node: Node,
        process: Process
) -> Status:
    """Run Ingest process node together with process object
    """
    logger.info(f'START {process.order:02d}: {node.name} {"~" * (30 - len(node.name) + 31)}')
    sts: Status = Status(0, "")
    try:
        ps_row_success, ps_row_failed = node.ingest_start()
        sts.message = f"Success: Load data to {node.name!r} with logging value " \
                      f"(success {ps_row_success} row{get_plural(ps_row_success)}, " \
                      f"failed {ps_row_failed} row{get_plural(ps_row_failed)}, {process.duration} sec)"
    except ObjectBaseError as err:
        sts.update(1, f"Error: {err.__class__.__name__}: {str(err)}")
        logger.error(sts.message)
    return sts


MAP_MODULE_FUNC: dict = {
    name: eval(func_name)
    for name, func_name in registers.modules.ingestion.items()
}


def ingestion_foreground(
        module: str,
        external_parameters: dict
):
    """Foreground ingestion function for running data pipeline with module argument.
    This function will control write process log to Control Task Process table.
    If process input is pipeline, this function will log to Control Task Schedule.
    """
    process: Process = Process(module, parameters=external_parameters, task='foreground', component='ingestion')
    logger.info(f'Start run foreground ingestion: {process.id!r} at time: {process.start_time:%Y-%m-%d %H:%M:%S}')
    process.messages = f"[ run_date: {process.run_date} ]"
    logger.info(f"[ run_date: {process.run_date} ]{'=' * 48}")
    ps_obj: ObjectType = process.obj(
        name=process.name,
        process_id=process.id,
        run_mode=external_parameters.get('run_mode', 'ingestion'),
        run_date=process.run_date,
        auto_init=external_parameters.get('initial_data', 'N'),
        auto_drop=external_parameters.get('drop_before_create', 'N'),
        external_parameters=external_parameters
    )
    process.start_task(1)
    process.receive(MAP_MODULE_FUNC[module](node=ps_obj, process=process.next))
    process.end_task()
    logger.info(f'End foreground ingestion: {process.id!r} with duration: {process.duration:.2f} sec')
    return process


def ingestion_background(
        module: str,
        bg_queue: queue.Queue,
        external_parameters: dict,
):
    """Background ingestion function for running data pipeline with module argument.
    This function will control write process log to Control Task Process table.
    If process input is pipeline, this function will log to Control Task Schedule.
    """
    process: Process = Process(module, parameters=external_parameters, task='background', component='ingestion')
    logger.info(f'Start run background ingestion: {process.id!r} at time: {process.start_time:%Y-%m-%d %H:%M:%S}')
    bg_queue.put(process.id)
    process.messages = f"[ run_date: {process.run_date} ]"
    logger.info(f"[ run_date: {process.run_date} ]{'=' * 48}")
    ps_obj: ObjectType = process.obj(
        name=process.name,
        process_id=process.id,
        run_mode=external_parameters.get('run_mode', 'ingestion'),
        run_date=process.run_date,
        auto_init=external_parameters.get('initial_data', 'N'),
        auto_drop=external_parameters.get('drop_before_create', 'N'),
        external_parameters=external_parameters
    )
    process.start_task(1)
    bg_queue.put(process.name)
    process.receive(MAP_MODULE_FUNC[module](node=ps_obj, process=process.next))
    process.end_task()
    logger.info(f'End background ingestion: {process.id!r} with duration: {process.duration:.2f} sec')
