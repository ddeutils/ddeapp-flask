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
    Control,
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
from app.core.services import (
    Action,
    ActionQuery,
    Node,
    NodeLocal,
    Schema,
    Task,
)
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
    """Run close SSH function."""
    from app.core.connections.postgresql import ssh_connect

    server = ssh_connect()
    if server.is_alive or force:
        server.close()


def push_schema_setup() -> None:
    """Run Set up the main schema."""
    _schema: Schema = Schema()
    if not _schema.exists():
        _schema.create()
        logger.info("Success: Create Schema to target database.")


def push_func_setup(task: Optional[Task] = None) -> None:
    """Run Setup function in `register.yaml`"""
    task: Task = task or Task.make(module="function_setup")
    for idx, _func_prop in enumerate(registers.functions, start=1):
        _func: Action = Action.parse_name(fullname=_func_prop["name"])
        logger.info(
            f"START {idx:02d}: {_func.name} "
            f'{"~" * (30 - len(_func.name) + 31)}'
        )

        if not _func.exists():
            _func.create()
            logger.info(
                f"Success: Setup {_func.name} "
                f"with logging value {task.duration()} sec"
            )


def push_ctr_setup(
    task: Optional[Task] = None,
) -> None:
    """Run Setup Control Framework table in `register.yaml`"""
    from app.core.__legacy.objects import Node as LegacyNode

    task: Task = task or Task.make(module="control_setup")
    for idx, _ctr_prop in enumerate(
        registers.control_frameworks,
        start=1,
    ):
        status: Status = Status.SUCCESS
        _node = Node.parse_name(fullname=_ctr_prop["name"])
        logger.info(
            f"START {idx:02d}: {_node.name} {'~' * (30 - len(_node.name) + 31)}"
        )

        if not _node.exists():
            if _node.name in (
                "ctr_data_logging",
                "ctr_task_process",
            ):
                # NOTE: Create without logging.
                _node.create()
            else:
                try:
                    _node_legacy: LegacyNode = LegacyNode(
                        name=_ctr_prop["name"],
                        process_id=task.id,
                        run_mode="setup",
                        auto_init="Y",
                        auto_drop="Y",
                    )
                except ObjectBaseError as err:
                    logger.error(f"Error ObjectBaseError: {err}")
                    status: Status = Status.FAILED
        logger.info(
            f"Success create {_ctr_prop['name']!r} "
            f"after app start with status {status.value}"
        )


MAP_BG_PROCESS: dict = {}


def pull_ctr_check_process() -> tuple[int, ...]:
    """Check process_id in `ctr_task_process` table."""
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
    """Check migrate table process."""
    pipe_cnt = Pipeline(
        name="control_search",
        auto_create=False,
        verbose=False,
    )
    for _order, node in pipe_cnt.nodes():
        node.push_tbl_diff()


def push_ctr_stop_running() -> None:
    """Do something before server shutdown."""
    if eval(os.environ.get("DEBUG", "True")):
        return

    try:
        logger.info("Start update message and status to in-progress tasks.")
        (
            ActionQuery.parse_name(fullname="query:query_shutdown").push(
                params={
                    "status": 1,
                    "process_message": (
                        "Error: RuntimeError: Server shutdown while "
                        "process was running in background"
                    ),
                }
            )
        )
    except (OperationalError, PsycopgOperationalError):
        logger.warning("... Target database does not connectable.")
    logger.critical("Success server has been shut down :'(")


def push_trigger_schedule() -> int:
    """Push run data with trigger schedule."""
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
    """Push run data with cron schedule."""
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
    by framework engine."""
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
    """Push load csv file to target table with short name.

    Examples:
        >> push_load_file_to_db('initial/ilticd/ilticd_20220821.csv', 'ilticd')
        ...
    """
    _: Task = Task.make(module="load_data_from_file")
    rs: int = NodeLocal.parse_shortname(target).load(
        filename, truncate=truncate, compress=compress
    )
    return rs


def push_initialize_frontend(): ...


def push_testing() -> None:
    Schema().create()

    logger.info("Start Testing ...")

    # for _, _ctr_prop in enumerate(
    #     registers.control_frameworks,
    #     start=1,
    # ):
    #     node = Node.parse_name(fullname=_ctr_prop["name"])
    #     if not node.exists():
    #         node.create()

    from app.core.services import Control

    print(Control.params())
