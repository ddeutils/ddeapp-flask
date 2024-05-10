# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

from datetime import date, datetime
from typing import Union

from pydantic import validator

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
