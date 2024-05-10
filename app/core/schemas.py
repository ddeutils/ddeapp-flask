# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

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
    run_count_now: float
    run_count_max: float
    rtt_value: str
    rtt_column: str
    active_flg: str

    @validator("update_date", pre=True)
    def prepare_update_date(cls, value: Union[str, date, datetime]) -> datetime:
        if isinstance(value, datetime):
            return value
        elif isinstance(value, date):
            return datetime.fromisoformat(value.isoformat())
        return datetime.fromisoformat(value)


WTM_DEFAULT: dict[str, Any] = {
    "system_type": UNDEFINED,
    "table_name": UNDEFINED,
    "table_type": UNDEFINED,
    "data_date": "1990-01-01",
    "update_date": "1990-01-01 00:00:00",
    "run_date": "1990-01-01",
    "run_type": UNDEFINED,
    "run_count_now": "1.0",
    "run_count_max": "1.0",
    "rtt_value": UNDEFINED,
    "rtt_column": UNDEFINED,
    "active_flg": "N",
}
