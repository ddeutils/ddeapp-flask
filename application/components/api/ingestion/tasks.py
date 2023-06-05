# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import queue
from typing import Callable, Dict
from application.core.errors import ObjectBaseError
from application.core.utils.config import Params
from application.core.utils.logging_ import logging
from application.core.base import get_plural
from application.core.models import (
    Result,
    CommonResult,
    Status,
    TaskMode,
    TaskComponent,
)
from application.core.legacy.objects import (
    Node,
    ObjectType,
)
from application.core.services import Task


logger = logging.getLogger(__name__)
registers = Params(param_name='registers.yaml')


def ingest_payload(
        node: Node,
        task: Task
) -> Result:
    """Run Ingest process node together with process object
    """
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
        result.update(
            f"Error: {err.__class__.__name__}: {str(err)}",
            Status.FAILED,
        )
        logger.error(result.message)
    return result


MAP_MODULE_FUNC: Dict[str, Callable] = {
    name: eval(func_name)
    for name, func_name in registers.modules.ingestion.items()
}


def ingestion_foreground(
        module: str,
        external_parameters: dict
) -> Result:
    """Foreground ingestion function for running data pipeline with module
    argument. This function will control write process log to Control Task
    Process table. If process input is pipeline, this function will log to
    Control Task Schedule.
    """
    result: Result = CommonResult()
    task: Task = Task.parse_obj({
        "module": module,
        "parameters": external_parameters,
        "mode": TaskMode.FOREGROUND,
        "component": TaskComponent.INGESTION
    })
    logger.info(
        f'Start run foreground ingestion: {task.id!r} at time: '
        f'{task.start_time:%Y-%m-%d %H:%M:%S}'
    )
    for idx, run_date in task.runner():
        logger.info(f"[ run_date: {run_date} ]{'=' * 48}")
        node: ObjectType = Node(
            name=task.parameters.name,
            process_id=task.id,
            run_mode=task.parameters.others.get('run_mode', 'ingestion'),
            run_date=run_date,
            auto_init=task.parameters.others.get('initial_data', 'N'),
            auto_drop=task.parameters.others.get('drop_before_create', 'N'),
            external_parameters=external_parameters
        )
        task.start()
        logger.info(
            f'START {idx:02d}: {node.name} '
            f'{"~" * (30 - len(node.name) + 31)}'
        )
        task.receive(
            MAP_MODULE_FUNC[module](node=node, task=task)
        )

        # Ingestion only first date
        break

    task.finish()
    logger.info(
        f'End foreground ingestion: {task.id!r} '
        f'with duration: {task.duration():.2f} sec'
    )
    return result.update(
        task.message,
        task.status
    )


def ingestion_background(
        module: str,
        bg_queue: queue.Queue,
        external_parameters: dict,
) -> Result:
    """Background ingestion function for running data pipeline with module
    argument. This function will control write process log to Control Task
    Process table. If process input is pipeline, this function will log to
    Control Task Schedule.
    """
    result: Result = CommonResult()
    task: Task = Task.parse_obj({
        "module": module,
        "parameters": external_parameters,
        "mode": TaskMode.BACKGROUND,
        "component": TaskComponent.INGESTION
    })
    logger.info(
        f'Start run background ingestion: {task.id!r} '
        f'at time: {task.start_time:%Y-%m-%d %H:%M:%S}'
    )
    bg_queue.put(task.id)
    for idx, run_date in task.runner():
        logger.info(f"[ run_date: {run_date} ]{'=' * 48}")
        node: ObjectType = Node(
            name=task.parameters.name,
            process_id=task.id,
            run_mode=task.component.value,
            run_date=run_date,
            auto_init=task.parameters.others.get('initial_data', 'N'),
            auto_drop=task.parameters.others.get('drop_before_create', 'N'),
            external_parameters=external_parameters
        )
        task.start()
        bg_queue.put(task.parameters.name)
        logger.info(
            f'START {idx:02d}: {node.name} '
            f'{"~" * (30 - len(node.name) + 31)}'
        )
        task.receive(
            MAP_MODULE_FUNC[module](node=node, task=task)
        )

        # Ingestion only first date
        break
    task.finish()
    logger.info(
        f'End background ingestion: {task.id!r} '
        f'with duration: {task.duration():.2f} sec'
    )
    return result.update(
        task.message,
        task.status
    )
