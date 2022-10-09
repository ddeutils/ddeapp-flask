# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import queue
from ...utils.config import Params
from ...utils.logging_ import logging
from ...utils.objects import (
    Node,
    Process,
    Status,
    Schema,
    ObjectType,
)
from ...errors import (
    ObjectBaseError,
    ProcessStatusError,
)

logger = logging.getLogger(__name__)

registers = Params(param_name='registers.yaml')


def run_tbl_setup(
        node: Node,
        process: Process
) -> Status:
    """Run Setup process node together with process object
    This function will control write process log to Control Data Pipeline table.
    """
    logger.info(f'START {process.order:02d}: {node.name} {"~" * (30 - len(node.name) + 31)}')
    sts: Status = Status(0, "")

    try:
        record_stm: str = ""
        if process.drop_table:
            node.push_tbl_drop(cascade=process.cascade)
            record_stm = f" with drop table in {node.process_time} sec"
        else:
            if node.name in {'ctr_data_pipeline', 'ctr_data_parameter'}:
                node.auto_init = True
            row_record: int = node.push_tbl_create(force_drop=node.auto_drop, cascade=process.cascade)
            if node.auto_init:
                record_stm: str = f" with logging value ({row_record} rows, {node.process_time} sec)"
        sts.message = f"Success: Setup {node.name!r}{record_stm}"
    except ObjectBaseError as err:
        sts.update(1, f"Error: {err.__class__.__name__}: {str(err)}")
        logger.error(sts.message)
    return sts


def run_tbl_data(
        node: Node,
        process: Process
) -> Status:
    """Run Data process node together with process object
    This function will control write process log to Control Data Pipeline table.
    """
    logger.info(f'START {process.order:02d}: {node.name} {"~" * (30 - len(node.name) + 31)}')
    sts: Status = Status(0, "")
    try:
        if node.quota:
            logger.warning(f"the running quota of {node.name!r} has been reached")
            sts.message = f"Warning: the running quota of {node.name!r} has been reached"
        else:
            ps_row: dict = node.process_start()
            sts.message = f"Success: Running {node.name!r} in {process.mode} mode " \
                          f"with logging value ({ps_row} rows, {node.process_time} sec)"
    except ObjectBaseError as err:
        sts.update(1, f"Error: {err.__class__.__name__}: {str(err)}")
        logger.error(sts.message)
    return sts


def run_tbl_retention(
        node: Node,
        process: Process
) -> Status:
    """Run Retention process table
    """
    logger.info(f'START {process.order:02d}: {node.name} {"~" * (30 - len(node.name) + 31)}')
    sts: Status = Status(0, "")
    try:
        msg: list = []
        msg_row: list = []
        if node.backup_name:
            ps_bk_row: int = node.backup_start()
            if ps_bk_row > 0:
                msg.append(f"Backup {node.name!r} to '{node.backup_schema}.{node.backup_name}' ")
                msg_row.append(f"{ps_bk_row} row")
        elif node.backup_schema:
            logger.warning("Backup process does not support for put backup scheme only")
        ps_rtt_row: int = node.retention_start()
        ps_rtt_msg: str = ''
        if ps_rtt_row > 0:
            ps_rtt_msg = f"less than '{node.retention_date:%Y-%m-%d}' "
        msg.insert(0, ps_rtt_msg)
        msg_row.append(f"{ps_rtt_row} row")
        sts.message = f"Success: Retention {node.name!r} {'and '.join(msg)}with logging value " \
                      f"({', '.join(msg_row)}, {node.process_time} sec)"
    except ObjectBaseError as err:
        sts.update(1, f"Error: {err.__class__.__name__}: {str(err)}")
        logger.error(sts.message)
    return sts


def run_schema_drop(
        schema: Schema,
) -> Status:
    """Run drop process schema
    """
    logger.info(f'START 01: {schema.name} {"~" * (30 - len(schema.name) + 31)}')
    sts: Status = Status(0, "")
    try:
        schema: Schema = Schema()
        if schema.exists:
            logger.warning(f"Schema {schema.name!r} was exists in database")
        sts.message = f"Success: Drop schema {schema.name!r}"
    except ObjectBaseError as err:
        sts.update(1, f"Error: {err.__class__.__name__}: {str(err)}")
        logger.error(sts.message)
    return sts


MAP_MODULE_FUNC: dict = {
    name: eval(func_name)
    for name, func_name in registers.modules.framework.items()
}


def _task_gateway(
        process: Process,
        obj: ObjectType
) -> Process:
    """Task Gateway for running task with difference `ps_obj` type"""
    if process.is_tbl:
        process.update_task({
            'process_name_get': process.name,
            'run_date_get': obj.run_date
        })
        process.receive(MAP_MODULE_FUNC[process.module](node=obj, process=process.next))
        if process.status == 1:
            raise ProcessStatusError
    else:
        obj.update_to_ctr_schedule({'tracking': 'PROCESSING'})
        for order, node in obj.nodes():
            process.update_task({
                'process_name_get': node.name,
                'process_number_get': order,
                'run_date_get': obj.run_date
            })
            process.receive(MAP_MODULE_FUNC[process.module](node=node, process=process.next))
            if process.status == 1:
                obj.update_to_ctr_schedule({'tracking': 'FAILED'})
                raise ProcessStatusError
    return process


def foreground_tasks(
        module: str,
        external_parameters: dict,
) -> Process:
    """Foreground task function for running data pipeline with module argument.
    This function will control write process log to Control Task Process table.
    If process input is pipeline, this function will log to Control Task Schedule.
    """
    process: Process = Process(module, parameters=external_parameters, task='foreground', component='framework')
    logger.info(f'Start run foreground task: {process.id!r} at time: {process.start_time:%Y-%m-%d %H:%M:%S}')

    if process.drop_schema:
        schema: Schema = Schema(cascade=process.cascade)
        process.receive(MAP_MODULE_FUNC['drop_schema'](schema=schema))
        logger.info(f'End foreground task: {process.id!r} with duration: {process.duration:.2f} sec')
        return process

    for index, run_date in enumerate(process.run_dates, start=1):
        logger.info(f"[ run_date: {run_date} ]{'=' * 48}")
        ps_obj: ObjectType = process.obj(
            name=process.name,
            process_id=process.id,
            run_mode=external_parameters.get('run_mode', 'common'),
            run_date=run_date,
            auto_init=external_parameters.get('initial_data', 'N'),
            auto_drop=external_parameters.get('drop_before_create', 'N'),
            external_parameters=external_parameters
        )
        # TODO: add waiting process by queue
        process.start_task(index, ps_obj.process_count)
        try:
            process: Process = _task_gateway(process, ps_obj)
        except ProcessStatusError:
            logger.warning("Process was break because raise by `ProcessStatusError` ...")
            break
    process.end_task()
    logger.info(f'End foreground task: {process.id!r} with duration: {process.duration:.2f} sec')
    return process


def background_tasks(
        module: str,
        bg_queue: queue.Queue,
        external_parameters: dict,
) -> Process:
    """Background task function for running data pipeline with module argument.
    This function will control write process log to Control Task Process table.
    If process input is pipeline, this function will log to Control Task Schedule.
    """
    process = Process(module, parameters=external_parameters, task='background', component='framework')
    logger.info(f'Start run background task: {process.id!r} at time: {process.start_time:%Y-%m-%d %H:%M:%S}')
    bg_queue.put(process.id)

    if process.drop_schema:
        schema: Schema = Schema(cascade=process.cascade)
        bg_queue.put(process.name)
        process.receive(MAP_MODULE_FUNC['drop_schema'](schema=schema))
        logger.info(f'End foreground task: {process.id!r} with duration: {process.duration:.2f} sec')
        return process

    for index, run_date in enumerate(process.run_dates, start=1):
        logger.info(f"[ run_date: {run_date} ]{'=' * 48}")
        ps_obj: ObjectType = process.obj(
            name=process.name,
            process_id=process.id,
            run_mode=external_parameters.get('run_mode', 'common'),
            run_date=run_date,
            auto_init=external_parameters.get('initial_data', 'N'),
            auto_drop=external_parameters.get('drop_before_create', 'N'),
            external_parameters=external_parameters
        )
        # TODO: add waiting process by queue
        process.start_task(index, ps_obj.process_count)
        bg_queue.put(process.name)
        try:
            process: Process = _task_gateway(process, ps_obj)
        except ProcessStatusError:
            logger.warning("Process was break because raise by `ProcessStatusError` ...")
            break
    process.end_task()
    logger.info(f'End background task: {process.id!r} with duration: {process.duration:.2f} sec')
