# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

from dataclasses import (
    dataclass,
    field,
)
from datetime import datetime
from enum import (
    IntEnum,
)
from functools import partial, total_ordering
from typing import Optional, Union

from strenum import StrEnum

from .base import get_run_date

UNDEFINED: str = "undefined"


def reduce_text(text: str, newline: Optional[str] = None) -> str:
    """Reduce text before insert to Database."""
    return text.replace("'", "''").replace("\n", (newline or "|"))


def enum_ordering(cls):
    """Add order property to Enum object."""

    def __lt__(self, other):
        if isinstance(other, type(self)):
            return self.value < other.value
        raise ValueError("Cannot compare different Enums")

    cls.__lt__ = __lt__
    return total_ordering(cls)


@enum_ordering
class Status(IntEnum):
    SUCCESS = 0
    FAILED = 1
    WAITING = 2
    PROCESSING = 2


class PartitionType(StrEnum):
    RANGE = "range"
    LIST = "list"
    HASH = "hash"


class ParameterType(StrEnum):
    TABLE = "table"
    PIPELINE = "pipeline"
    UNDEFINED = UNDEFINED


class ParameterMode(StrEnum):
    """Parameter Mode Enum."""

    COMMON = "common"
    RERUN = "rerun"


class ParameterIngestMode(StrEnum):
    """Parameter Ingestion Mode Enum."""

    COMMON = "common"
    MERGE = "merge"


class TaskMode(StrEnum):
    FOREGROUND = "foreground"
    BACKGROUND = "background"


class TaskComponent(StrEnum):
    FRAMEWORK = "framework"
    INGESTION = "ingestion"
    ANALYTIC = "analytic"
    UNDEFINED = UNDEFINED


class TaskStatus(StrEnum):
    SUCCESSFUL = "successful"
    FAILED = "failed"
    PROCESSING = "processing"


@dataclass
class IngestionRow:
    success: int
    failed: int


@dataclass
class BaseResult:
    status: Status
    _message: str
    _duration: datetime = field(
        default_factory=partial(get_run_date, date_type="datetime")
    )

    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, msg: str):
        self._message += f"\n{msg}" if self._message else f"{msg}"

    def update(
        self,
        msg: str,
        status: Optional[Union[Status, int]] = None,
    ):
        if status is not None:
            self.status: Status = (
                status if isinstance(status, Status) else Status(status)
            )
        self.message: str = reduce_text(msg)
        return self

    def duration(self) -> int:
        return round(
            (
                get_run_date(date_type="date_time") - self._duration
            ).total_seconds()
        )


@dataclass
class CommonResult(BaseResult):
    status: Status = Status.SUCCESS
    _message: str = ""


@dataclass
class AnalyticResult(BaseResult):
    status: Status = Status.SUCCESS
    _message: str = ""
    percent: float = 0.0
    logging: str = ""


@dataclass
class DependencyResult(BaseResult):
    status: Status = Status.SUCCESS
    _message: str = ""
    mapping: dict = field(default_factory=dict)


@dataclass
class IngestionResult(BaseResult):
    status: Status = Status.SUCCESS
    _message: str = ""


Result = Union[
    CommonResult,
    AnalyticResult,
    DependencyResult,
]
