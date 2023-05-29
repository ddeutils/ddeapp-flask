from functools import partial
import random
from dataclasses import dataclass, field
from datetime import date
from enum import Enum, auto
from typing import List, Optional
from functools import total_ordering


def enum_ordering(cls):
    """Add order property to Enum object."""
    def __lt__(self, other):
        if type(other) == type(self):
            return self.value < other.value
        raise ValueError("Cannot compare different Enums")

    setattr(cls, '__lt__', __lt__)
    return total_ordering(cls)


@enum_ordering
class Status(Enum):
    SUCCESS = auto()
    WAITING = auto()
    FAILED = auto()


@dataclass(frozen=True)
class Profile:
    features: dict = field(default_factory=dict, compare=True)


@dataclass
class Process:
    parameter: list = field(default_factory=list, compare=True)
    step: dict = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        if self.parameter:
            self.parameter = list(set(self.parameter))


@dataclass
class Initial:
    parameter: list
    statement: str


@dataclass
class Catalog:
    """Data class for Catalog"""
    name: str = field(compare=True)
    type: str = field(default='sql', compare=True)
    id: int = field(default_factory=partial(random.randint, 0, 100), compare=False, repr=False)
    # version: str = field(default="", compare=False)
    description: str = field(default="", compare=False)
    profile: Profile = field(default_factory=Profile, compare=True, repr=False)
    process: Process = field(default_factory=Process, compare=True, repr=False)
    initial: dict = field(default_factory=dict, compare=True, repr=False)

    @property
    def name_short(self) -> str:
        """Return the short name of this node with `_` separator"""
        return "".join([word[0] for word in self.name.split("_")])

    @property
    def prefix(self) -> str:
        """Return the prefix value of this node name with `_` separator"""
        return self.name.split('_')[0]

    def __post_init__(self) -> None:
        """Post initialization for value validation"""
        if self.name in {'test_catalog'}:
            raise ValueError(
                "Catalog name does not support for included test value."
            )


@dataclass(order=True)
class Process:
    id: str = field(compare=True)
    status: Status


def main() -> None:
    aam = Catalog(
        name='ai_article_master',
    )
    aad = Catalog(
        name='ai_article_master',
    )
    print(aam)
    print(aad)
    print(aam == aad)


def process():
    print(list(Status))
    process01 = Process(id='asc', status=Status.SUCCESS)
    process02 = Process(id='asc', status=Status.FAILED)
    print(process01 <= process02)


if __name__ == '__main__':
    main()
    process()
