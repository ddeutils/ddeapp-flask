# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import queue
from typing import Callable

from app.core.__legacy.objects import (
    Node,
    ObjectType,
)
from app.core.base import (
    get_plural,
    registers,
)
from app.core.errors import ObjectBaseError
from app.core.models import (
    FAILED,
    CommonResult,
    Result,
    TaskComponent,
    TaskMode,
)
from app.core.services import Task
from app.core.utils.logging_ import logging

logger = logging.getLogger(__name__)


def ingest_payload(node: Node, task: Task) -> Result:
    """Run Ingest process node together with process object."""
    result: CommonResult = CommonResult()
    try:
        ps_row_success, ps_row_failed = node.ingest_start()
        result.message = (
            f"Success: Load data to {node.name!r} with logging value "
            f"(success {ps_row_success} row{get_plural(ps_row_success)}, "
            f"failed {ps_row_failed} row{get_plural(ps_row_failed)}, "
            f"{task.duration()} sec)"
        )
    except ObjectBaseError as err:
        result.update(f"Error: {err.__class__.__name__}: {str(err)}", FAILED)
        logger.error(result.message)
    return result


MAP_MODULE_FUNC: dict[str, Callable] = {
    name: eval(func_name)
    for name, func_name in registers.modules.ingestion.items()
}


def ingestion_foreground(module: str, external_parameters: dict) -> Result:
    """Foreground ingestion function for running data pipeline with module
    argument.

    This function will control write process log to Control Task Process
    table. If process input is pipeline, this function will log to
    Control Task Schedule.
    """
    with Task.parse_obj(
        {
            "module": module,
            "parameters": external_parameters,
            "mode": TaskMode.FOREGROUND,
            "component": TaskComponent.INGESTION,
        }
    ) as task:
        logger.info(
            f"Start run foreground ingestion: {task.id!r} at time: "
            f"{task.start_time:%Y-%m-%d %H:%M:%S}"
        )
        for idx, run_date in task.runner():
            logger.info(f"{f'[ run_date: {run_date} ]':=<60}")
            node: ObjectType = Node(
                name=task.parameters.name,
                process_id=task.id,
                run_mode=task.parameters.others.get("run_mode", "ingestion"),
                run_date=run_date,
                auto_init=task.parameters.others.get("initial_data", "N"),
                auto_drop=task.parameters.others.get("drop_before_create", "N"),
                external_parameters=external_parameters,
            )
            logger.info(f"START {idx:02d}: {f'{node.name} ':~<30}'")
            task.receive(MAP_MODULE_FUNC[module](node=node, task=task))
            # NOTE: Ingestion only first date
            break
        logger.info(
            f"End foreground ingestion: {task.id!r} "
            f"with duration: {task.duration():.2f} sec"
        )
    return CommonResult.make(task.message, task.status)


def ingestion_background(
    module: str,
    bg_queue: queue.Queue,
    external_parameters: dict,
) -> Result:
    """Background ingestion function for running data pipeline with module
    argument.

    This function will control write process log to Control Task Process
    table. If process input is pipeline, this function will log to
    Control Task Schedule.
    """
    with Task.parse_obj(
        {
            "module": module,
            "parameters": external_parameters,
            "mode": TaskMode.BACKGROUND,
            "component": TaskComponent.INGESTION,
        }
    ) as task:
        logger.info(
            f"Start run background ingestion: {task.id!r} "
            f"at time: {task.start_time:%Y-%m-%d %H:%M:%S}"
        )
        bg_queue.put(task.id)
        for idx, run_date in task.runner():
            logger.info(f"{f'[ run_date: {run_date} ]':=<60}")
            node: ObjectType = Node(
                name=task.parameters.name,
                process_id=task.id,
                run_mode=task.component.value,
                run_date=run_date,
                auto_init=task.parameters.others.get("initial_data", "N"),
                auto_drop=task.parameters.others.get("drop_before_create", "N"),
                external_parameters=external_parameters,
            )
            bg_queue.put(task.parameters.name)
            logger.info(f"START {idx:02d}: {f'{node.name} ':~<30}'")
            task.receive(MAP_MODULE_FUNC[module](node=node, task=task))
            # NOTE: Ingestion only first date
            break
        logger.info(
            f"End background ingestion: {task.id!r} "
            f"with duration: {task.duration():.2f} sec"
        )
    return CommonResult.make(task.message, task.status)
