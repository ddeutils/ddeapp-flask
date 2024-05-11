# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import queue
from typing import Callable, Union

from app.core.errors import (
    ObjectBaseError,
    ProcessStatusError,
)
from app.core.models import (
    FAILED,
    CommonResult,
    Result,
    TaskComponent,
    TaskMode,
)
from app.core.services import (
    NodeManage,
    Pipeline,
    Schema,
    Task,
)
from app.core.utils import logging
from app.core.utils.config import Params

logger = logging.getLogger(__name__)
registers = Params(param_name="registers.yaml")
ObjectType = Union[NodeManage, Pipeline]
ObjectMap = {
    "table": NodeManage,
    "pipeline": Pipeline,
}


def run_tbl_setup(node: NodeManage, task: Task) -> Result:
    """Run Setup process node together with task model This function will
    control write process log to Control Data Pipeline table."""
    result: Result = CommonResult()
    try:
        record_stm: str = ""
        if task.parameters.drop_table:
            node.drop(cascade=task.parameters.cascade)
            record_stm = f" with drop table in {node.fwk_params.duration()} sec"
        else:
            if node.exists() and node.validate_name_flag(
                task.parameters.others.get("initial_data", "N")
            ):
                node.create(
                    force_drop=True,
                    cascade=task.parameters.cascade,
                )
                if node.initial:
                    row_record: int = node.count()
                    record_stm: str = (
                        f" with logging value "
                        f"({row_record} rows, {node.fwk_params.duration()} sec)"
                    )
        result.message = f"Success: Setup {node.name!r}{record_stm}"
    except ObjectBaseError as err:
        result.update(f"Error: {err.__class__.__name__}: {str(err)}", FAILED)
        logger.error(result.message)
    return result


def run_tbl_data(node: NodeManage, task: Task) -> Result:
    """Run Data process node together with task model This function will
    control write process log to Control Data Pipeline table."""
    result: Result = CommonResult()
    try:
        if node.has_quota:
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
                f"({ps_row} rows, {node.fwk_params.duration()} sec)"
            )
    except ObjectBaseError as err:
        result.update(f"Error: {err.__class__.__name__}: {str(err)}", FAILED)
        logger.error(result.message)
    return result


def run_tbl_retention(node: NodeManage, task: Task) -> Result:
    """Run Retention process node together with task model This function will
    control write process log to Control Data Pipeline table."""
    _ = task
    result: Result = CommonResult()
    try:
        msg: list = []
        msg_row: list = []
        if (bk := node.name_backup)[0]:
            ps_bk_row: int = node.backup()
            if ps_bk_row > 0:
                msg.append(f"Backup {node.name!r} to '{bk[0]}.{bk[1]}' ")
                msg_row.append(f"{ps_bk_row} row")
        ps_rtt_row: int = node.retention()
        ps_rtt_msg: str = ""
        if ps_rtt_row > 0:
            ps_rtt_msg = f"less than '{node.retention_date:%Y-%m-%d}' "
        msg.insert(0, ps_rtt_msg)
        msg_row.append(f"{ps_rtt_row} row")
        result.message = (
            f"Success: Retention {node.name!r} {'and '.join(msg)}"
            f"with logging value "
            f"({', '.join(msg_row)}, {node.fwk_params.duration()} sec)"
        )
    except ObjectBaseError as err:
        result.update(f"Error: {err.__class__.__name__}: {str(err)}", FAILED)
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
            schema.drop(cascade=cascade)
        result.message = (
            f"Success: Drop schema {schema.name!r} with "
            f"{'cascade' if cascade else ''}"
        )
    except ObjectBaseError as err:
        result.update(f"Error: {err.__class__.__name__}: {str(err)}", FAILED)
        logger.error(result.message)
    return result


MAP_MODULE_FUNC: dict[str, Callable] = {
    name: eval(func_name)
    for name, func_name in registers.modules.framework.items()
}


def _task_gateway(task: Task, obj: ObjectType) -> Task:
    """Task Gateway for running task with difference `ps_obj` type (NodeManage
    nor Pipeline)."""
    logger.info(f"START {task.release.index:02d}: {f'{obj.name} ':~<50}'")
    if task.parameters.is_table():
        obj: NodeManage
        task.push(
            values={
                "process_name_get": obj.name,
                "run_date_get": obj.fwk_params.run_date,
            }
        )
        task.receive(MAP_MODULE_FUNC[task.module](node=obj, task=task))
        if task.is_failed():
            raise ProcessStatusError
    else:
        obj: Pipeline
        obj.push({"tracking": "PROCESSING"})
        for order, node in obj.process_nodes():
            task.push(
                values={
                    "process_name_get": node.name,
                    "process_number_get": order,
                    "run_date_get": obj.fwk_params.run_date,
                }
            )
            task.receive(MAP_MODULE_FUNC[task.module](node=node, task=task))
            if task.is_failed():
                obj.push({"tracking": "FAILED"})
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
    with Task.parse_obj(
        {
            "module": module,
            "parameters": external_parameters,
            "mode": TaskMode.FOREGROUND,
            "component": TaskComponent.FRAMEWORK,
        }
    ) as task:
        logger.info(
            f"Start run foreground task: {task.id!r} "
            f"at time: {task.start_time:%Y-%m-%d %H:%M:%S}"
        )
        if task.parameters.drop_schema:
            for _, run_date in task.runner():
                logger.info(f"{f'[ run_date: {run_date} ]':=<60}")
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
                break
            return result.update(task.message, task.status)
        for _, run_date in task.runner():
            logger.info(f"{f'[ run_date: {run_date} ]':=<60}")
            ps_obj: ObjectType = ObjectMap[task.parameters.type].parse_task(
                task.parameters.name,
                fwk_params={
                    "run_id": task.id,
                    "run_date": run_date,
                    "run_mode": task.component,
                    "task_params": task.parameters,
                },
                ext_params=external_parameters,
            )
            # TODO: add waiting process by queue
            task.start(ps_obj.process_count)
            try:
                # Start push the task to target execute function.
                task: Task = _task_gateway(task, ps_obj)
            except ProcessStatusError as err:
                logger.warning(f"Process was break because raise from {err}")
                break
        logger.info(
            f"End foreground task: {task.id!r} with duration: "
            f"{task.duration():.2f} sec"
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
