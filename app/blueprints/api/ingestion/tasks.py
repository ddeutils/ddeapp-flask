# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import queue
from typing import Callable

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
from app.core.services import NodeIngest, Task
from app.core.utils import logging

logger = logging.getLogger(__name__)


def ingest_payload(node: NodeIngest, task: Task) -> Result:
    """Run Ingest process node together with process object."""
    result: CommonResult = CommonResult()
    try:
        if not node.exists():
            ...
        ps_row_success, ps_row_failed = node.ingest()
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
            node: NodeIngest = NodeIngest.start(
                task.parameters.name,
                fwk_params={
                    "run_id": task.id,
                    "run_date": run_date,
                    "run_mode": task.component,
                    "task_params": task.parameters,
                },
                ext_params=external_parameters,
            )
            logger.info(f"START {idx:02d}: {f'{node.name} ':~<50}")
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
            node: NodeIngest = NodeIngest.start(
                task.parameters.name,
                fwk_params={
                    "run_id": task.id,
                    "run_date": run_date,
                    "run_mode": task.component,
                    "task_params": task.parameters,
                },
                ext_params=external_parameters,
            )
            bg_queue.put(task.parameters.name)
            logger.info(f"START {idx:02d}: {f'{node.name} ':~<50}")
            task.receive(MAP_MODULE_FUNC[module](node=node, task=task))
            # NOTE: Ingestion only first date
            break
        logger.info(
            f"End background ingestion: {task.id!r} "
            f"with duration: {task.duration():.2f} sec"
        )
    return CommonResult.make(task.message, task.status)
