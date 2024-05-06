# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------

import os
from typing import (
    Optional,
)

from psycopg2 import OperationalError as PsycopgOperationalError
from sqlalchemy.exc import OperationalError

from app.blueprints.api.framework.tasks import foreground_tasks
from app.core.__legacy.objects import (
    Action,
    Control,
    Node,
    Pipeline,
)
from app.core.base import get_catalogs
from app.core.errors import (
    CatalogBaseError,
    ObjectBaseError,
)
from app.core.models import (
    Result,
    Status,
)
from app.core.services import Task
from app.core.utils.config import (
    Environs,
    Params,
)
from app.core.utils.logging_ import logging

logger = logging.getLogger(__name__)
registers = Params(param_name="registers.yaml")
params = Params(param_name="parameters.yaml")
env = Environs(env_name=".env")


def push_close_ssh(force: bool = False):
    """Run close SSH function"""
    from app.core.connections.postgresql import ssh_connect

    server = ssh_connect()
    if server.is_alive or force:
        server.close()


def push_schema_setup() -> None:
    from app.core.__legacy.objects import Schema

    _schema: Schema = Schema()
    if not _schema.exists:
        _schema.create()
        logger.info("Success: Create Schema to target database.")


def push_func_setup(task: Optional[Task] = None) -> None:
    """Run Setup function in `register.yaml`"""
    task: Task = task or Task.make(module="function_setup")
    functions = registers.functions
    for idx, _func_prop in enumerate(functions, start=1):
        try:
            _func: Action = Action(
                _func_prop["name"], process_id=task.id, auto_create=False
            )
        except ObjectBaseError:
            _func: Action = Action(
                _func_prop["name"], process_id=task.id, auto_create=True
            )
        logger.info(
            f"START {idx:02d}: {_func.name} "
            f'{"~" * (30 - len(_func.name) + 31)}'
        )
        logger.info(
            f"Success: Setup {_func.name} "
            f"with logging value {_func.process_time} sec"
        )


def push_ctr_setup(
    task: Optional[Task] = None,
) -> None:
    """Run Setup Control Framework table in `register.yaml`"""
    task: Task = task or Task.make(module="control_setup")
    for idx, _ctr_prop in enumerate(
        registers.control_frameworks,
        start=1,
    ):
        status: Status = Status.SUCCESS
        try:
            _node = Node(
                name=_ctr_prop["name"],
                process_id=task.id,
                run_mode="setup",
                auto_init="Y",
                auto_drop="Y",
            )
            logger.info(
                f"START {idx:02d}: {_node.name} "
                f'{"~" * (30 - len(_node.name) + 31)}'
            )
        except ObjectBaseError as err:
            # TODO: Bug of create control table
            #   sqlalchemy.exc:ProgrammingError: relation "ai.ctr_data_pipeline"
            #   does not exist
            #   LINE 1: ...count_max, rtt_value, rtt_column, active_flg from
            #   postgres.a...
            logger.error(f"Error ObjectBaseError: {err}")
            status: Status = Status.FAILED
        logger.info(
            f"Success run {_ctr_prop['name']!r} "
            f"after app start with status {status.value}"
        )


MAP_BG_PROCESS: dict = {}


def pull_ctr_check_process() -> tuple[int, ...]:
    """Check process_id in `ctr_task_process` table"""
    # search process with status does not success in control table
    if not (
        ctr_pull := {
            data["process_id"]: data["status"]
            for data in Control("ctr_task_process").pull(
                pm_filter={"process_id": "*"},
                included_cols=["process_id", "status"],
                condition="status <> '0'",
                all_flag=True,
            )
        }
    ):
        return 0, 0, len(MAP_BG_PROCESS)

    ps_filter_false: dict = dict(
        filter(lambda x: x[1] == "1", ctr_pull.items())
    )
    ps_filter_wait: dict = dict(filter(lambda x: x[1] == "2", ctr_pull.items()))
    _del_key: list = []
    append = _del_key.append
    for k, v in MAP_BG_PROCESS.items():
        if k not in ps_filter_wait:
            if v.check_count < 2:
                v.check_count += 1
            else:
                append(k)
    for _ in _del_key:
        MAP_BG_PROCESS.pop(_)
    del ctr_pull
    return len(ps_filter_false), len(ps_filter_wait), len(MAP_BG_PROCESS)


def pull_migrate_tables():
    """Check migrate table process"""
    pipe_cnt = Pipeline(
        name="control_search",
        auto_create=False,
        verbose=False,
    )
    for _order, node in pipe_cnt.nodes():
        node.push_tbl_diff()


def push_ctr_stop_running() -> None:
    """
    Do something before server shutdown
    """
    if eval(os.environ.get("DEBUG", "True")):
        return

    try:
        logger.info("Start update message and status to in-progress tasks.")
        Action(
            name="query:query_shutdown",
            external_parameters={
                "status": 1,
                "process_message": (
                    "Error: RuntimeError: Server shutdown while "
                    "process was running in background"
                ),
            },
        ).push_query()
    except (OperationalError, PsycopgOperationalError):
        logger.warning("... Target database does not connectable.")
    logger.critical("Success server has been shut down :'(")


def push_trigger_schedule() -> int:
    """Push run data with trigger schedule"""
    ps_time_all: int = 0
    for pipe_name, _pipe_props in get_catalogs(
        config_form="pipeline",
        key_exists=params.map_pipe.trigger,
        key_exists_all_mode=False,
        priority_sorted=True,
    ).items():
        try:
            pipeline: Pipeline = Pipeline(pipe_name)
            if pipeline.check_pipe_trigger():
                logger.info(
                    f"Start trigger schedule "
                    f"for data pipeline: {pipeline.name!r}"
                )
                result: Result = foreground_tasks(
                    module="data",
                    external_parameters={
                        "pipeline_name": pipeline.name,
                        "run_mode": "common",
                    },
                )
                logger.info(
                    f"End trigger {pipeline.name!r} "
                    f"with status: {result.status.name} "
                    f"with time {result.duration()} sec."
                )
                ps_time_all += result.duration()
        except (ObjectBaseError, CatalogBaseError) as err:
            logger.error(f"{err.__class__.__name__}: {str(err)}")
            continue
    return ps_time_all


def push_cron_schedule(group_name: str, waiting_process: int = 300) -> int:
    """Push run data with cron schedule"""
    ps_time_all: int = 0
    for pipe_name, _ in get_catalogs(
        config_form="pipeline",
        key_exists=params.map_pipe.schedule,
        key_exists_all_mode=False,
        priority_sorted=True,
    ):
        try:
            pipeline: Pipeline = Pipeline(pipe_name)
            if pipeline.check_pipe_schedule(
                group=group_name,
                waiting_process=waiting_process,
            ):
                logger.info(
                    f"Start cron jon schedule "
                    f"for data pipeline: {pipeline.name!r}"
                )
                result: Result = foreground_tasks(
                    module="data",
                    external_parameters={
                        "pipeline_name": pipeline.name,
                        "run_mode": "common",
                    },
                )
                logger.info(
                    f"End trigger {pipeline.name!r} "
                    f"with status: {result.status.name} "
                    f"with time {result.duration()} sec"
                )
                ps_time_all += result.duration()
        except (ObjectBaseError, CatalogBaseError) as err:
            logger.error(f"{err.__class__.__name__}: {str(err)}")
            continue
    return ps_time_all


def push_retention() -> int:
    """Push run retention with `retention_search` pipeline which auto generate
    by framework engine
    """
    logger.info(
        "Start run retention process with `retention_search` pipeline ..."
    )
    try:
        result: Result = foreground_tasks(
            module="retention",
            external_parameters={"pipeline_name": "retention_search"},
        )
        return result.duration()
    except (ObjectBaseError, CatalogBaseError) as err:
        logger.error(f"{err.__class__.__name__}: {str(err)}")
        return 0


def push_load_file_to_db(
    filename: str,
    target: str,
    truncate: bool = False,
    compress: Optional[str] = None,
):
    """Push load csv file to target table with short name
    :usage:
        >> push_load_file_to_db('initial/ilticd/ilticd_20220821.csv', 'ilticd')
    """
    task: Task = Task.make(module="load_data_from_file")
    node: Node = Node(name=Node.convert_short(target), process_id=task.id)
    node.load_file(filename, truncate=truncate, compress=compress)


def push_initialize_frontend(): ...
