# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import ast
import builtins
from typing import (
    Any,
    Optional,
)

from pydantic import Field, validator
from typing_extensions import Self

from .__legacy.objects import Control as LegacyControl
from .base import (
    get_plural,
    get_run_date,
    registers,
)
from .connections import (
    query_execute,
    query_insert_from_csv,
    query_select,
    query_select_check,
)
from .errors import ControlProcessNotExists
from .models import (
    ParameterMode,
    ParameterType,
    Status,
    TaskComponent,
    TaskMode,
    reduce_text,
)
from .statements import (
    ControlStatement,
    FunctionStatement,
    QueryStatement,
    SchemaStatement,
    TableStatement,
    reduce_value_pairs,
)
from .utils.config import (
    AI_APP_PATH,
    Environs,
)
from .utils.logging_ import logging
from .utils.reusables import (
    merge_dicts,
    must_list,
)
from .validators import (
    Pipeline as PipelineCatalog,
)
from .validators import (
    ReleaseDate,
)
from .validators import (
    Task as BaseTask,
)

env = Environs(env_name=".env")
logger = logging.getLogger(__name__)

__all__ = (
    "Schema",
    "Action",
    "ActionQuery",
    "Node",
    "NodeLocal",
    "Pipeline",
    "Task",
    "Control",
)


def null_or_str(value: str) -> Optional[str]:
    return None if value == "None" else value


class Schema(SchemaStatement):
    """Schema Service Model."""

    name: str = Field(default=env.AI_SCHEMA, description="Schema name")

    def exists(self) -> bool:
        """Push exists statement to target database."""
        return query_select_check(self.statement_check(), parameters=True)

    def create(self) -> Schema:
        """Push create statement to target database."""
        query_execute(self.statement_create())
        return self

    def drop(self, cascade: bool = False) -> Schema:
        """Push drop statement to target database."""
        query_execute(self.statement_drop(cascade=cascade))
        return self


class Action(FunctionStatement):

    def exists(self) -> bool:
        """Push exists statement to target database."""
        return query_select_check(self.statement_check())

    def create(self) -> None:
        """Push create statement to target database."""
        query_execute(self.statement_create(), parameters=True)


class ActionQuery(QueryStatement):

    def push(self, params: dict[str, Any]):
        query_execute(
            self.statement(),
            parameters={
                "update_date": self.tag.ts.strftime("%Y-%m-%d %H:%M:%S"),
                **params,
            },
        )


class BaseNode(TableStatement):
    """Base Node Service Model."""

    ext_parameters: dict = Field(
        default_factory=dict,
        description="Node parameters from the application framework",
    )

    @validator("ext_parameters", always=True)
    def prepare_ext_params(cls, value: dict[str, Any]):
        return merge_dicts(Control.params(), value)

    def exists(self) -> bool:
        """Push exists statement to target database."""
        return query_select_check(self.statement_check(), parameters=True)

    def create(self) -> Self:
        """Execute create statement to target database."""
        query_execute(self.statement_create(), parameters=True)
        return self

    def drop(self): ...

    def log_push(self): ...

    def log_fetch(self): ...

    def task_push(self): ...

    def task_fetch(self): ...


class Node(BaseNode):
    """"""

    def backup(self): ...

    def retention(self): ...

    def init(self): ...


class NodeLocal(BaseNode):
    """Node for Local File loading."""

    def load(
        self,
        filename: str,
        chuck: int = 10_000,
        truncate: bool = False,
        compress: Optional[str] = None,
    ) -> int:
        file_props: dict[str, Any] = {
            "filepath": AI_APP_PATH / f"{registers.path.data}/{filename}",
            "table": self.name,
            "props": {
                "delimiter": "|",
                "encoding": "utf-8",
                "engine": "python",
            },
        }
        rows: int = 0
        for chunk, row in query_insert_from_csv(
            file_props,
            chunk_size=chuck,
            truncate=truncate,
            compress=compress,
        ):
            logger.info(
                f"Success with first chuck size {chunk} "
                f"with {row} row{get_plural(row)}"
            )
            rows += row
        return rows


class NodeIngest(BaseNode):
    """Node for Ingestion."""

    def delete(self): ...

    def fetch(self): ...

    def push(self):
        """"""
        ...


class Pipeline(PipelineCatalog):
    """Pipeline Service Model."""

    def nodes(self): ...

    def log_push(self): ...

    def log_fetch(self): ...

    def check_triggered(self): ...

    def check_scheduled(self): ...

    def schedule_push(self): ...


class Task(BaseTask):
    """Task Service Model.

    Notes:
        -   For starting any api tasks, I will create this Task model with the 4
            keys (module, parameters, mode, and component).
        -   Task will generate the task ID from a module value, ``param.type``,
            and ``param.name``.
        -   Task has the runner method that return generator of datetime from
            the ``param.dates``.
        -   ``Task.start`` and ``Task.finish`` is the main of create and update
            the task status log to logging table at target database.

    Examples:

        >>> task: Task = Task.make(module="demo_docstring")
        ... task.start()
        ... print("Do Something")
        ... task.finish()
    """

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.finish()

    @classmethod
    def pull(cls, task_id: str):
        """Pull Task from target database with Task ID."""
        if ctr_process := (
            LegacyControl("ctr_task_process").pull(pm_filter=[task_id])
        ):
            mode, _type = ctr_process.get(
                "process_type", "foreground|undefined"
            ).split("|", maxsplit=1)
            component, module = ctr_process.get(
                "process_module", "undefined|undefined"
            ).split("|", maxsplit=1)
            run_mode: str = ctr_process.get("run_mode", "common")
            date: Optional[str] = null_or_str(ctr_process.get("run_date_get"))
            return cls(
                module=module,
                parameters={
                    "name": module,
                    "type": ParameterType(_type),
                    "mode": ParameterMode(run_mode),
                    "dates": must_list(ctr_process["run_date_put"]),
                    **ctr_process,
                },
                mode=TaskMode(mode),
                component=TaskComponent(component),
                status=Status(int(ctr_process["status"])),
                id=task_id,
                message=ctr_process["process_message"],
                release=ReleaseDate(date=date),
            )
        raise ControlProcessNotExists(
            f"Process ID: {task_id} does not exists in Control Task Process "
            f"table."
        )

    def start(self, task_total: Optional[int] = 1) -> int:
        """Start Task.

        :rtype: int
        :return: Return 0 if the release was pushed (runner index !=
            start index). But push the log to target database when
            release was not pushed.
        """
        return (
            0
            if self.release.pushed
            else self.push(values={"process_number_put": task_total})
        )

    def finish(self) -> int:
        """Update task log to target database."""
        return self.fetch(
            values=(
                {
                    "process_name_get": "null",
                    "run_date_get": "null",
                }
                if self.status == Status.SUCCESS
                else {}
            )
        )

    def push(self, values: Optional[dict] = None) -> int:
        """Push information to the Control Data Logging."""
        return LegacyControl("ctr_task_process").push(
            push_values=merge_dicts(
                {
                    "process_id": self.id,
                    "process_name_put": self.parameters.name,
                    "process_name_get": "null",
                    "run_date_put": reduce_text(str(self.parameters.dates)),
                    "run_date_get": self.release.date,
                    "process_message": (
                        f"Start {self.mode} process {self.parameters.type} "
                        f"`{self.parameters.name}` ..."
                    ),
                    "process_number_put": 1,
                    "process_number_get": 1,
                    "process_module": f"{self.component}|{self.module}",
                    "process_type": f"{self.mode}|{self.parameters.type}",
                },
                (values or {}),
            )
        )

    def fetch(self, values: Optional[dict] = None) -> int:
        """Fetch information to the Control Data Logging."""
        return LegacyControl("ctr_task_process").update(
            update_values=merge_dicts(
                {
                    "process_id": self.id,
                    "process_message": reduce_text(self.message),
                    "process_time": self.duration(),
                    "status": self.status,
                },
                (values or {}),
            )
        )


class Control(ControlStatement):

    def __init__(self, name: str) -> None:
        self.node: Node = Node.parse_name(fullname=name)
        self.name: str = self.node.name
        self.defaults: dict[str, Any] = {
            "update_date": get_run_date(fmt="%Y-%m-%d %H:%M:%S"),
            "process_time": 0,
            "status": 2,
        }

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def __str__(self) -> str:
        return self.name

    @classmethod
    def params(cls, module: Optional[str] = None) -> dict[str, Any]:
        logger.debug("Loading params from `ctr_data_parameter` by Control ...")
        _results: dict = {
            value["param_name"]: (
                ast.literal_eval(value["param_value"])
                if value["param_type"] in {"list", "dict"}
                else getattr(builtins, value["param_type"])(
                    value["param_value"]
                )
            )
            for value in query_select(
                cls.statement_params(),
                parameters=reduce_value_pairs({"module_type": (module or "*")}),
            )
        }
        # Calculate special parameters that logic was provided by vendor.
        proportion_value: int = _results.get("proportion_value", 3)
        proportion_inc_curr_m: str = _results.get(
            "proportion_inc_current_month_flag", "N"
        )
        window_end: int = 1 if proportion_inc_curr_m == "N" else 0
        window_start: int = (
            proportion_value
            if proportion_inc_curr_m == "N"
            else (proportion_value - 1)
        )
        return {
            "window_start": window_start,
            "window_end": window_end,
            **_results,
        }

    @classmethod
    def tables(cls): ...

    def push(self): ...

    def pull(self):
        """Pull data from the control table."""
