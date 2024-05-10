# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import ast
from datetime import date, datetime
from typing import Any, Union

from pydantic import validator

from .models import UNDEFINED
from .validators import BaseUpdatableModel


class ControlWatermark(BaseUpdatableModel):
    system_type: str
    table_name: str
    table_type: str
    data_date: date
    update_date: datetime
    run_date: date
    run_type: str
    run_count_now: int
    run_count_max: int
    rtt_value: int
    rtt_column: list[str]
    active_flg: str

    @validator("update_date", pre=True)
    def prepare_update_date(cls, value: Union[str, date, datetime]) -> datetime:
        if isinstance(value, datetime):
            return value
        elif isinstance(value, date):
            return datetime.fromisoformat(value.isoformat())
        return datetime.fromisoformat(value)

    @validator("rtt_column", pre=True)
    def prepare_rtt_column(cls, value):
        try:
            return (
                ast.literal_eval(value)
                if (value.startswith("[") and value.endswith("]"))
                else [value]
            )
        except AttributeError:
            # This error will raise when tbl_ctr_data does not define
            # before call this property.
            return [UNDEFINED]


WTM_DEFAULT: dict[str, Any] = {
    "system_type": UNDEFINED,
    "table_name": UNDEFINED,
    "table_type": UNDEFINED,
    "data_date": "1990-01-01",
    "update_date": "1990-01-01 00:00:00",
    "run_date": "1990-01-01",
    "run_type": UNDEFINED,
    "run_count_now": "1",
    "run_count_max": "1",
    "rtt_value": "0",
    "rtt_column": UNDEFINED,
    "active_flg": "N",
}
