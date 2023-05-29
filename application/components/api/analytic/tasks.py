# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import datetime as dt
from typing import Tuple
from ....core.legacy.base import TblCatalog
from ....utils.logging_ import logging
from ....utils.objects import Process
from ....utils.models import (
    AnalyticStatus,
    DependencyStatus,
)
from ....errors import ControlProcessNotExists

logger = logging.getLogger(__name__)


def get_dependency_data(tbl_name_sht: str):
    """Get dependency tables of target table
    """
    process: Process = Process.make('dependency')
    sts: DependencyStatus = DependencyStatus()
    tbl: TblCatalog = TblCatalog.short(tbl_name_sht)
    sts.mapping = tbl.get_tbl_dependency()
    process.messages = f'Success: get dependency from catalog {tbl.tbl_name} but result does not develop yet'
    return process, sts


def get_msg_status(status: int) -> str:
    mapping: dict = {
        0: 'successful',
        1: 'failed'
    }
    return mapping.get(status, 'running')


def get_operation_process(
        process_id: str
) -> Tuple[Process, AnalyticStatus]:
    """Get data in control process table
    """
    sts: AnalyticStatus = AnalyticStatus()
    try:
        process: Process = Process.load(process_id)
    except ControlProcessNotExists as err:
        process: Process = Process.make('operation')
        sts.message = f"Error: {err.__class__.__name__}: {str(err)}"
        process.status = 1
        return process, sts

    _update_date: str = process.parameters['update_date']
    if process.component == 'framework':
        if process.status == 0:
            sts.message = f"Process run successful at {_update_date}"
            sts.percent = 1.00
            return process, sts

        if not (_num_put := process.parameters.get('process_number_put')):
            sts.message = ''
            return process, sts

        run_date_put: list = list(map(lambda x: dt.date.fromisoformat(x), process.ps_dates))
        run_date_num_put: int = len(run_date_put)

        if _get := process.ps_date:
            _run_date_get: dt.date = dt.date.fromisoformat(_get)
            _run_date_get_filter: list = list(filter(lambda x: _run_date_get >= x, run_date_put))
            run_date_num_get: int = len(_run_date_get_filter) - 1
        else:
            run_date_num_get: int = 0
        ps_num_put: int = int(_num_put)
        ps_num_get: int = int(_num_get) if (_num_get := process.parameters.get('process_number_put')) else 0

        current_percent: float = (
                ((run_date_num_get * ps_num_put) + (ps_num_get - 1)) /
                (run_date_num_put * ps_num_put)
        )
        process_name_get: str = process.parameters.get('process_name_get')
        sts.message = f"Process was {get_msg_status(process.status)} " \
                      f"in {process_name_get} with run date: {_get}"
        sts.percent = current_percent
        return process, sts

    sts.message = f"Process was {get_msg_status(process.status)} at {_update_date}"
    sts.percent = float(process.status) if process.status < 2 else 0.0
    return process, sts
