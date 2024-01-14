# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import os
from dataclasses import dataclass, field
from typing import Protocol

from app.core.legacy.convertor import reduce_text
from app.core.utils.reusables import must_bool


@dataclass
class BaseStatus:
    id: int
    _message: str

    @property
    def message(self):
        return self._message

    @message.setter
    def message(self, msg: str):
        self._message += (f'\n{msg}' if self._message else f'{msg}')

    def update(self, _id: int, msg: str):
        self.id: int = _id
        self.message: str = reduce_text(msg)


@dataclass
class Status(BaseStatus):
    id: int = 0
    _message: str = ""


@dataclass
class AnalyticStatus(BaseStatus):
    id: int = 0
    _message: str = ""
    percent: float = 0.0


@dataclass
class DependencyStatus(BaseStatus):
    id: int = 0
    _message: str = ""
    mapping: dict = field(default_factory=dict)


class VerboseObject(Protocol):
    """Verbose Object protocol should have verbose property
    """
    verbose: bool


class VerboseDummy:
    """Verbose Object dummy
    """
    verbose: bool = must_bool(os.getenv('DEBUG', 'False'))
