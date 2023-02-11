from typing import (
    List,
    Set,
    Dict,
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
    profile: Dict
    process: Optional[Dict]


class Function(BaseModel):
    description: Optional[str]
    id: str
    name: str
    type: str
    prefix: str
    profile: Dict


class Pipeline(BaseModel):
    description: Optional[str]
    id: str
    name: str
    type: str
    prefix: str
    schedule: Optional[List]
    trigger: Optional[Union[List[str], Set[str]]]
    alert: Optional[List[str]]
    nodes: Dict[int, Dict]


Catalog = Union[Table, Function, Pipeline]


class Category(BaseModel):
    category: str
    data: List[Union[Table, Function, Pipeline]]
