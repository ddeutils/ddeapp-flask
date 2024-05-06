from typing import (
    Optional,
    Union,
)

from pydantic import BaseModel


class Table(BaseModel):
    description: Optional[str]
    id: str
    name: str
    type: str
    prefix: str
    profile: Optional[dict]
    process: Optional[dict]


class Function(BaseModel):
    description: Optional[str]
    id: str
    name: str
    type: str
    prefix: str
    profile: Optional[dict]


class Pipeline(BaseModel):
    description: Optional[str]
    id: str
    name: str
    type: str
    prefix: str
    schedule: Optional[list]
    trigger: Optional[Union[list, set]]
    alert: Optional[list[str]]
    nodes: dict[int, dict]


Catalog = Union[Table, Function, Pipeline]


class Category(BaseModel):
    category: str
    data: list[Catalog]
