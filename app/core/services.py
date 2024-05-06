# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from typing import Optional

from pydantic import Field

from .__legacy.objects import Control as LegacyControl
from .connections import query_execute, query_select_check
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
    FunctionStatement,
    SchemaStatement,
    TableStatement,
)
from .utils.config import (
    Environs,
)
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

env = Environs(env_name='.env')

__all__ = (
    'Schema',
    'Action',
    'Node',
    'Pipeline',
    'Task',
)


def null_or_str(value: str) -> Optional[str]:
    return None if value == 'None' else value


class Schema(SchemaStatement):
    """Schema Service Model"""
    name: str = Field(default=env.AI_SCHEMA, description="Schema name")

    def exists(self) -> bool:
        """Push exists statement to target database"""
        return query_select_check(self.statement_check())

    def create(self):
        """Push create statement to target database"""
        query_execute(self.statement_create())

    def drop(self, cascade: bool = False):
        """Push drop statement to target database"""
        query_execute(self.statement_drop(cascade=cascade))


class Action(FunctionStatement):
    ...

    def exists(self):
        ...


class BaseNode(TableStatement):
    """Node Service Model"""
    ext_parameters: dict = Field(
        default_factory=dict,
        description="Node parameters from the application framework"
    )

    def exists(self) -> bool:
        """Push exists statement to target database"""
        return query_select_check(self.statement_check())

    def log_push(self):
        ...

    def log_fetch(self):
        ...

    def task_push(self):
        ...

    def task_fetch(self):
        ...


class Node(BaseNode):
    """"""

    def create(self):
        ...

    def drop(self):
        ...

    def backup(self):
        ...

    def retention(self):
        ...


class NodeIngest(BaseNode):
    """Node for Ingestion"""

    def delete(self):
        ...

    def fetch(self):
        ...

    def push(self):
        """"""
        ...


class Pipeline(PipelineCatalog):
    """Pipeline Service Model"""
    ...


class Task(BaseTask):
    """Task Service Model"""

    @classmethod
    def pull(cls, task_id: str):
        """Pull Task from target database with Task ID"""
        if ctr_process := (
                LegacyControl('ctr_task_process')
                    .pull(pm_filter=[task_id])
        ):
            mode, _type = (
                ctr_process
                    .get('process_type', 'foreground|undefined')
                    .split('|', maxsplit=1)
            )
            component, module = (
                ctr_process
                    .get('process_module', 'undefined|undefined')
                    .split('|', maxsplit=1)
            )
            run_mode: str = ctr_process.get("run_mode", "common")
            date: Optional[str] = null_or_str(ctr_process.get("run_date_get"))
            return cls(
                module=module,
                parameters={
                    "name": module,
                    "type": ParameterType(_type),
                    "mode": ParameterMode(run_mode),
                    "dates": must_list(ctr_process["run_date_put"]),
                    **ctr_process
                },
                mode=TaskMode(mode),
                component=TaskComponent(component),
                status=Status(int(ctr_process["status"])),
                id=task_id,
                message=ctr_process["process_message"],
                release=ReleaseDate(
                    date=date,
                ),
            )
        raise ControlProcessNotExists(
            f"Process ID: {task_id} does not exists in Control Task Process "
            f"table."
        )

    def start(self, task_total: Optional[int] = 1) -> int:
        """Start Task"""
        return (
            0 if self.release.pushed
            else self.push(values={'process_number_put': task_total})
        )

    def finish(self) -> int:
        """Finish Task"""
        return self.fetch(values=(
            {
                'process_name_get': 'null',
                'run_date_get': 'null',
            }
            if self.status == Status.SUCCESS
            else {}
        ))

    def push(self, values: Optional[dict] = None) -> int:
        """Push information to the Control Data Logging"""
        return LegacyControl('ctr_task_process').push(
            push_values=merge_dicts({
                'process_id': self.id,
                'process_name_put': self.parameters.name,
                'process_name_get': 'null',
                'run_date_put': reduce_text(str(self.parameters.dates)),
                'run_date_get': self.release.date,
                'process_message': (
                    f'Start {self.mode} process {self.parameters.type} '
                    f'`{self.parameters.name}` ...'
                ),
                'process_number_put': 1,
                'process_number_get': 1,
                'process_module': f'{self.component}|{self.module}',
                'process_type': f'{self.mode}|{self.parameters.type}',
            }, (values or {}))
        )

    def fetch(self, values: Optional[dict] = None) -> int:
        """Fetch information to the Control Data Logging"""
        return LegacyControl('ctr_task_process').update(
            update_values=merge_dicts({
                'process_id': self.id,
                'process_message': reduce_text(self.message),
                'process_time': self.duration(),
                'status': self.status
            }, (values or {}))
        )
