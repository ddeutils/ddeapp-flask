# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
import datetime as dt

from ....core.errors import ControlProcessNotExists
from ....core.models import (
    UNDEFINED,
    AnalyticResult,
    DependencyResult,
    Result,
    Status,
    TaskComponent,
)
from ....core.services import Task
from ....core.utils.logging_ import logging
from ....core.validators import Table

logger = logging.getLogger(__name__)


def get_dependency_data(
        tbl_name_sht: str
) -> Result:
    """Get dependency tables of target table
    """
    result: Result = DependencyResult()
    tbl: Table = Table.parse_shortname(tbl_name_sht)
    result.mapping = tbl.dependency()
    return result.update(
        f'Success: get dependency from catalog {tbl.name} '
        f'but result does not develop yet'
    )

def get_operation_process(
        process_id: str
) -> Result:
    """Get data in control process table
    """
    result: Result = AnalyticResult()
    try:
        process: Task = Task.pull(task_id=process_id)
    except ControlProcessNotExists as err:
        result.update(
            f"Error: {err.__class__.__name__}: {str(err)}",
            Status.FAILED,
        )
        return result

    _update_date: str = process.parameters.others['update_date']
    if process.component == TaskComponent.FRAMEWORK:
        return _extracted_get_operation_framework(
            process=process,
            update_date=_update_date,
        )
    result.percent = (
        float(process.status.value)
        if process.status != Status.WAITING
        else 0.0
    )
    result.logging = process.message
    return result.update(
        f"Process was {Status(process.status).name.lower()} "
        f"at {_update_date}"
    )


def _extracted_get_operation_framework(
        process: Task,
        update_date,
) -> Result:
    """Extracted result from Framework component"""
    result: Result = AnalyticResult(
        logging=process.message
    )
    if process.status == Status.SUCCESS:
        result.percent = 1.00
        return result.update(
            f"Process run successful at {update_date}"
        )

    if not (
            _num_put := (
                process.parameters
                    .others
                    .get('process_number_put')
            )
    ):
        return result

    run_date_put: list = [
        dt.date.fromisoformat(i) for i in process.parameters.dates
    ]
    run_date_num_put: int = len(run_date_put)

    if (_get := process.release.date) is not None:
        _run_date_get: dt.date = dt.date.fromisoformat(_get)
        _run_date_get_filter: list = list(
            filter(lambda x: _run_date_get >= x, run_date_put)
        )
        run_date_num_get: int = len(_run_date_get_filter) - 1
    else:
        run_date_num_get: int = 0
    ps_num_put: int = int(_num_put)
    ps_num_get: int = int(
        process.parameters
            .others
            .get('process_number_put', 0)
    )
    current_percent: float = (
            (
                    (run_date_num_get * ps_num_put)
                    + (ps_num_get - 1)
            )
            / (run_date_num_put * ps_num_put)
    )
    process_name_get: str = (
        process.parameters
            .others
            .get('process_name_get', UNDEFINED)
    )
    result.percent = current_percent
    return result.update(
        f"Process was {Status(process.status).name.lower()} status "
        f"in process {process_name_get} "
        f"with run date: {_get}"
    )
