# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import queue
from typing import Callable

from ....core.__legacy.objects import (
    Node,
    ObjectType,
    Pipeline,
)
from ....core.errors import (
    ObjectBaseError,
    ProcessStatusError,
)
from ....core.models import (
    CommonResult,
    ParameterType,
    Result,
    Status,
    TaskComponent,
    TaskMode,
)
from ....core.services import (
    Schema,
    Task,
)
from ....core.utils.config import Params
from ....core.utils.logging_ import logging

logger = logging.getLogger(__name__)
registers = Params(param_name="registers.yaml")
ObjectMap = {
    "table": Node,
    "pipeline": Pipeline,
}


def run_tbl_setup(node: Node, task: Task) -> Result:
    """Run Setup process node together with task model This function will
    control write process log to Control Data Pipeline table."""
    result: Result = CommonResult()
    try:
        record_stm: str = ""
        if task.parameters.drop_table:
            node.push_tbl_drop(cascade=task.parameters.cascade)
            record_stm = f" with drop table in {node.process_time} sec"
        else:
            if node.name in {"ctr_data_pipeline", "ctr_data_parameter"}:
                node.auto_init = True
            row_record: int = node.push_tbl_create(
                force_drop=node.auto_drop,
                cascade=task.parameters.cascade,
            )
            if node.auto_init:
                record_stm: str = (
                    f" with logging value "
                    f"({row_record} rows, {node.process_time} sec)"
                )
        result.message = f"Success: Setup {node.name!r}{record_stm}"
    except ObjectBaseError as err:
        result.update(
            f"Error: {err.__class__.__name__}: {str(err)}", Status.FAILED
        )
        logger.error(result.message)
    return result


def run_tbl_data(node: Node, task: Task) -> Result:
    """Run Data process node together with task model This function will
    control write process log to Control Data Pipeline table."""
    result: Result = CommonResult()
    try:
        if node.quota:
            logger.warning(
                f"the running quota of {node.name!r} has been reached"
            )
            result.message = (
                f"Warning: the running quota of {node.name!r} has been reached"
            )
        else:
            ps_row: dict = node.process_start()
            result.message = (
                f"Success: Running {node.name!r} in {task.parameters.mode} "
                f"mode with logging value "
                f"({ps_row} rows, {node.process_time} sec)"
            )
    except ObjectBaseError as err:
        result.update(
            f"Error: {err.__class__.__name__}: {str(err)}", Status.FAILED
        )
        logger.error(result.message)
    return result


def run_tbl_retention(node: Node, task: Task) -> Result:
    """Run Retention process node together with task model This function will
    control write process log to Control Data Pipeline table."""
    _ = task
    result: Result = CommonResult()
    try:
        msg: list = []
        msg_row: list = []
        if node.backup_name:
            ps_bk_row: int = node.backup_start()
            if ps_bk_row > 0:
                msg.append(
                    f"Backup {node.name!r} to "
                    f"'{node.backup_schema}.{node.backup_name}' "
                )
                msg_row.append(f"{ps_bk_row} row")
        elif node.backup_schema:
            logger.warning(
                "Backup process does not support for put backup scheme only"
            )
        ps_rtt_row: int = node.retention_start()
        ps_rtt_msg: str = ""
        if ps_rtt_row > 0:
            ps_rtt_msg = f"less than '{node.retention_date:%Y-%m-%d}' "
        msg.insert(0, ps_rtt_msg)
        msg_row.append(f"{ps_rtt_row} row")
        result.message = (
            f"Success: Retention {node.name!r} {'and '.join(msg)}"
            f"with logging value "
            f"({', '.join(msg_row)}, {node.process_time} sec)"
        )
    except ObjectBaseError as err:
        result.update(
            f"Error: {err.__class__.__name__}: {str(err)}", Status.FAILED
        )
        logger.error(result.message)
    return result


def run_schema_drop(
    schema: Schema,
    cascade: bool = False,
) -> Result:
    """Run drop process schema."""
    result: Result = CommonResult()
    try:
        if schema.exists:
            logger.warning(f"Schema {schema.name!r} was exists in database")
        result.message = f"Success: Drop schema {schema.name!r} with {cascade}"
    except ObjectBaseError as err:
        result.update(
            f"Error: {err.__class__.__name__}: {str(err)}", Status.FAILED
        )
        logger.error(result.message)
    return result


MAP_MODULE_FUNC: dict[str, Callable] = {
    name: eval(func_name)
    for name, func_name in registers.modules.framework.items()
}


def _task_gateway(task: Task, obj: ObjectType) -> Task:
    """Task Gateway for running task with difference `ps_obj` type (Node nor
    Pipeline)."""
    logger.info(
        f"START {task.release.index:02d}: {obj.name} "
        f'{"~" * (30 - len(obj.name) + 31)}'
    )
    if task.parameters.type == ParameterType.TABLE:
        task.fetch(
            values={"process_name_get": obj.name, "run_date_get": obj.run_date}
        )
        task.receive(MAP_MODULE_FUNC[task.module](node=obj, task=task))
        if task.status == Status.FAILED:
            raise ProcessStatusError
    else:
        obj.update_to_ctr_schedule({"tracking": "PROCESSING"})
        for order, node in obj.nodes():
            task.fetch(
                values={
                    "process_name_get": node.name,
                    "process_number_get": order,
                    "run_date_get": obj.run_date,
                }
            )
            task.receive(MAP_MODULE_FUNC[task.module](node=node, task=task))
            if task.status == Status.FAILED:
                obj.update_to_ctr_schedule({"tracking": "FAILED"})
                raise ProcessStatusError
    return task


def foreground_tasks(
    module: str,
    external_parameters: dict,
) -> Result:
    """Foreground task function for running data pipeline with module argument.

    This function will control write process log to Control Task Process
    table. If process input is pipeline, this function will log to
    Control Task Schedule.
    """
    result: Result = CommonResult()
    task: Task = Task.parse_obj(
        {
            "module": module,
            "parameters": external_parameters,
            "mode": TaskMode.FOREGROUND,
            "component": TaskComponent.FRAMEWORK,
        }
    )
    logger.info(
        f"Start run foreground task: {task.id!r} "
        f"at time: {task.start_time:%Y-%m-%d %H:%M:%S}"
    )

    if task.parameters.drop_schema:
        for _, run_date in task.runner():
            logger.info(f"[ run_date: {run_date} ]{'=' * 48}")
            task.receive(
                MAP_MODULE_FUNC["drop_schema"](
                    schema=Schema(),
                    cascade=task.parameters.cascade,
                )
            )
            logger.info(
                f"End foreground task: {task.id!r} "
                f"with duration: {task.duration():.2f} sec"
            )

            # Ingestion only first date
            break

        return result.update(task.message, task.status)

    for _, run_date in task.runner():
        logger.info(f"[ run_date: {run_date} ]{'=' * 48}")
        ps_obj: ObjectType = ObjectMap[task.parameters.type](
            name=task.parameters.name,
            process_id=task.id,
            run_mode=task.parameters.others.get("run_mode", "common"),
            run_date=run_date,
            auto_init=task.parameters.others.get("initial_data", "N"),
            auto_drop=task.parameters.others.get("drop_before_create", "N"),
            external_parameters=external_parameters,
        )
        # TODO: add waiting process by queue
        task.start(ps_obj.process_count)
        try:
            # Start push the task to target execute function.
            task: Task = _task_gateway(task, ps_obj)
        except ProcessStatusError:
            logger.warning(
                "Process was break because raise by `ProcessStatusError` ..."
            )
            break
    task.finish()
    logger.info(
        f"End foreground task: {task.id!r} "
        f"with duration: {task.duration():.2f} sec"
    )
    return result.update(task.message, task.status)


def background_tasks(
    module: str,
    bg_queue: queue.Queue,
    external_parameters: dict,
) -> Result:
    """Background task function for running data pipeline with module argument.

    This function will control write process log to Control Task Process
    table. If process input is pipeline, this function will log to
    Control Task Schedule.
    """
    result: Result = CommonResult()
    task = Task.parse_obj(
        {
            "module": module,
            "parameters": external_parameters,
            "mode": TaskMode.BACKGROUND,
            "component": TaskComponent.FRAMEWORK,
        }
    )
    logger.info(
        f"Start run background task: {task.id!r} "
        f"at time: {task.start_time:%Y-%m-%d %H:%M:%S}"
    )
    bg_queue.put(task.id)
    if task.parameters.drop_schema:
        bg_queue.put(task.parameters.name)
        task.receive(
            MAP_MODULE_FUNC["drop_schema"](
                schema=Schema(),
                cascade=task.parameters.cascade,
            )
        )
        logger.info(
            f"End background task: {task.id!r} "
            f"with duration: {task.duration():.2f} sec"
        )
        return result.update(task.message, task.status)

    for _, run_date in task.runner():
        logger.info(f"[ run_date: {run_date} ]{'=' * 48}")
        ps_obj: ObjectType = ObjectMap[task.parameters.type](
            name=task.parameters.name,
            process_id=task.id,
            run_mode=task.parameters.others.get("run_mode", "common"),
            run_date=run_date,
            auto_init=task.parameters.others.get("initial_data", "N"),
            auto_drop=task.parameters.others.get("drop_before_create", "N"),
            external_parameters=external_parameters,
        )
        # TODO: add waiting process by queue
        task.start(ps_obj.process_count)
        bg_queue.put(task.parameters.name)
        try:
            task: Task = _task_gateway(task, ps_obj)
        except ProcessStatusError:
            logger.warning(
                "Process was break because raise by `ProcessStatusError` ..."
            )
            break
    task.finish()
    logger.info(
        f"End background task: {task.id!r} "
        f"with duration: {task.duration():.2f} sec"
    )
    return result.update(task.message, task.status)
