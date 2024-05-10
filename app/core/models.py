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
    Enum,
    IntEnum,
)
from functools import partial, total_ordering
from typing import Optional, Union

from typing_extensions import Self

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


class PartitionType(str, Enum):
    RANGE = "range"
    LIST = "list"
    HASH = "hash"


class ParameterType(str, Enum):
    TABLE = "table"
    PIPELINE = "pipeline"
    UNDEFINED = UNDEFINED


class ParameterMode(str, Enum):
    """Parameter Mode Enum."""

    COMMON = "common"
    RERUN = "rerun"


class ParameterIngestMode(str, Enum):
    """Parameter Ingestion Mode Enum."""

    COMMON = "common"
    MERGE = "merge"


class TaskMode(str, Enum):
    FOREGROUND = "foreground"
    BACKGROUND = "background"


class TaskComponent(str, Enum):
    FRAMEWORK = "framework"
    INGESTION = "ingestion"
    ANALYTIC = "analytic"
    RECREATED = "recreated"
    UNDEFINED = UNDEFINED


class TaskStatus(str, Enum):
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

    @classmethod
    def make(cls, msg: str, status: Optional[Status] = None) -> Self:
        return cls(
            _message=msg,
            status=(Status.WAITING if status is None else status),
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

SUCCESS = Status.SUCCESS
FAILED = Status.FAILED
WAITING = Status.WAITING
PROCESSING = Status.PROCESSING
