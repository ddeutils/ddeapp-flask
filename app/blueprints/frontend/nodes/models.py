import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from sqlalchemy import or_
from sqlalchemy.orm import reconstructor

from app.core.errors import CatalogNotFound
from app.core.validators import Pipeline as PipelineCatalog
from app.extensions import db


class Node(db.Model):
    """Node Model represent table, `ctr_data_pipeline`"""

    __tablename__ = "ctr_data_pipeline"
    __table_args__ = {"extend_existing": True, "autoload_with": db.engine}

    sys_type = db.Column("system_type", db.String(64), nullable=False)
    name = db.Column("table_name", db.String(64), primary_key=True)
    type = db.Column("table_type", db.String(64), nullable=False)
    data_date = db.Column("data_date", db.Date, nullable=False)
    update_date = db.Column(
        "update_date", db.DateTime(timezone=True), nullable=True
    )
    run_date = db.Column("run_date", db.Date, nullable=False)
    run_type = db.Column("run_type", db.String(64), nullable=True)
    run_count_now = db.Column("run_count_now", db.Numeric, nullable=False)
    run_count_max = db.Column("run_count_max", db.Numeric, nullable=False)
    rtt_value = db.Column("rtt_value", db.Numeric, nullable=False)
    rtt_column = db.Column("rtt_column", db.String(256), nullable=True)
    active = db.Column("active_flg", db.String(32), nullable=False)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __repr__(self):
        return f"Node('{self.name}', '{self.type}', '{self.sys_type}')"

    def __eq__(self, other):
        """Checks the equality of two `Node` objects using `name`."""
        return (
            self.name == other.name
            if isinstance(other, Node)
            else NotImplemented
        )

    @property
    def id(self):
        return self.shortname

    @property
    def shortname(self) -> str:
        return "".join([word[0] for word in self.name.split("_")])

    def to_dict(self):
        return {
            "sys_type": self.sys_type,
            "name": self.name,
            "type": self.type,
            "data_date": self.data_date.strftime("%Y/%m/%d"),
            "update_date": self.update_date.strftime("%Y/%m/%d"),
            "run_date": self.run_date.strftime("%Y/%m/%d"),
            "run_type": self.run_type,
            "run_count": {
                "now": self.run_count_now,
                "max": self.run_count_max,
            },
            "retention": {"value": self.rtt_value, "column": self.rtt_column},
            "active": self.active,
        }


class Pipeline(db.Model):
    """Pipeline Model represent table, `ctr_task_schedule`"""

    __tablename__ = "ctr_task_schedule"
    __table_args__ = {
        # 'autoload': True,
        "extend_existing": True,
        "autoload_with": db.engine,
    }

    id = db.Column(
        "pipeline_id", db.String(64), primary_key=True, nullable=False
    )
    name = db.Column("pipeline_name", db.String(256), nullable=False)
    type = db.Column("pipeline_type", db.String(64), nullable=True)
    tracking = db.Column("tracking", db.String(64), nullable=False)
    update_date = db.Column(
        "update_date", db.DateTime(timezone=True), nullable=False
    )
    active = db.Column("active_flg", db.Boolean, nullable=False)
    primary_id = db.Column("primary_id", db.BigInteger, nullable=False)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @reconstructor
    def __re_init__(self):
        self.cache_filename = (
            Path(__file__).parent / "cache" / f"catalog_{self.id}.pickle"
        )
        self.cache_flag: bool = self.cache_filename.exists()

    def __repr__(self):
        return f"Pipeline('{self.id}', '{self.name}', '{self.type}')"

    @classmethod
    def search_all(cls, query: str):
        _query: str = query.replace("*", "%")
        return cls.query.filter(
            or_(
                *[
                    col.ilike(f"%{_query}%")
                    for col in [
                        cls.id,
                        cls.type,
                        cls.name,
                        cls.tracking,
                    ]
                ]
            )
        )

    @property
    def catalog(self) -> Optional[PipelineCatalog]:
        """Return the Pipeline catalog data from the .yaml file."""
        try:
            if self.cache_flag:
                with open(self.cache_filename, mode="rb") as f:
                    _catalog = pickle.load(f)
            else:
                self.cache_filename.parent.mkdir(exist_ok=True)
                _catalog = PipelineCatalog.parse_name(self.name)
                with open(self.cache_filename, mode="wb") as f:
                    pickle.dump(_catalog, f)
            return _catalog
        except CatalogNotFound:
            return None

    @property
    def nodes(self) -> list:
        """Return any Node that this pipeline contain."""
        return (
            [v["name"].split(":")[-1] for v in self.catalog.nodes.values()]
            if self.catalog
            else []
        )

    @property
    def alert(self):
        mapping = {
            "PROCESSING": "warning",
            "ALERT-FAILED": "danger",
            "FAILED": "danger",
            "SUCCESS": "success",
        }
        return mapping.get(self.tracking, "info")

    @property
    def icon(self):
        mapping = {
            "PROCESSING": "pause",
            "ALERT-FAILED": "exclamation",
            "FAILED": "exclamation",
            "SUCCESS": "check",
        }
        return mapping.get(self.tracking, "info")


class NodeLog(db.Model):
    """Node Model represent table, `ctr_data_logging`"""

    __tablename__ = "ctr_data_logging"
    __table_args__ = {
        # 'autoload': True,
        "extend_existing": True,
        "autoload_with": db.engine,
    }

    name = db.Column(
        "table_name", db.String(64), primary_key=True, nullable=False
    )
    data_date = db.Column("data_date", db.Date, nullable=True)
    update_date = db.Column(
        "update_date", db.DateTime(timezone=True), nullable=False
    )
    run_date = db.Column("run_date", db.Date, primary_key=True, nullable=False)
    row_record = db.Column("row_record", db.String(256), nullable=True)
    process_time = db.Column("process_time", db.String(128), nullable=True)
    action_type = db.Column(
        "action_type", db.String(64), primary_key=True, nullable=False
    )
    status = db.Column("status", db.String(32), nullable=False)

    node = db.relationship(
        "Node",
        foreign_keys=[name],
        primaryjoin="Node.name == NodeLog.name",
        # backref=db.backref('videoslogs', lazy=True)
    )

    @property
    def alert(self):
        mapping = {0: "success", 1: "danger", 2: "warning"}
        return mapping.get(int(self.status), 2)


@dataclass
class Catalog:
    """Catalog Model."""

    version: str
    description: str
    create: dict
    initial: dict
    update: dict

    @classmethod
    def get(cls, name: str): ...
