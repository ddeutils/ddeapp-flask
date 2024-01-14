from typing import (
    Dict,
    List,
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
    profile: Optional[Dict]
    process: Optional[Dict]


class Function(BaseModel):
    description: Optional[str]
    id: str
    name: str
    type: str
    prefix: str
    profile: Optional[Dict]


class Pipeline(BaseModel):
    description: Optional[str]
    id: str
    name: str
    type: str
    prefix: str
    schedule: Optional[List]
    trigger: Optional[Union[list, set]]
    alert: Optional[List[str]]
    nodes: Dict[int, Dict]


Catalog = Union[Table, Function, Pipeline]


class Category(BaseModel):
    category: str
    data: List[Catalog]
