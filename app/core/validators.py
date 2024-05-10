# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import importlib
import re
from collections.abc import Generator, Iterator
from datetime import (
    date,
    datetime,
)
from functools import partial, singledispatch
from typing import (
    AbstractSet,
    Any,
    Callable,
    Optional,
    Union,
)

from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    root_validator,
    validator,
)
from typing_extensions import Self

from .base import (
    LoadCatalog,
    get_process_id,
    get_run_date,
)
from .connections.io import load_json_to_values
from .convertor import (
    Statement,
    reduce_stm,
)
from .models import (
    ParameterMode,
    ParameterType,
    Result,
    Status,
    TaskComponent,
    TaskMode,
)
from .utils.config import Params
from .utils.logging_ import get_logger
from .utils.reusables import (
    must_bool,
    must_list,
    only_one,
)

params = Params(param_name="parameters.yaml")
logger = get_logger(__name__)


__all__ = (
    "BaseUpdatableModel",
    "Column",
    "Partition",
    "Profile",
    "Table",
    "TableFrontend",
    "Function",
    "FunctionFrontend",
    "Pipeline",
    "PiplineFrontend",
    "Schema",
    "ReleaseDate",
    "Task",
    "MapParameter",
    "FrameworkParameter",
)


def sorted_set(values):
    """Return Sorted List after parsing Set."""
    return sorted(set(values))


def split_datatype(datatype_full: str) -> tuple[str, str]:
    """Split the datatype value from long string by null string."""
    _nullable: str = "null"
    for null_str in ["not null", "null"]:
        if re.search(null_str, datatype_full):
            _nullable = null_str
            datatype_full = datatype_full.replace(null_str, "")
    return " ".join(datatype_full.strip().split()), _nullable


def split_fk(value: str) -> tuple[str, Optional[str]]:
    """Split the key value from dot or bracket."""
    if "." in value:
        prefix, sub_value = value.rsplit(".", maxsplit=1)
        return prefix, sub_value
    elif "(" in value and ")" in value:
        if m := re.search(r"(?P<prefix>\w+)\s?\((?P<value>[^()]+)\)", value):
            groups = m.groupdict()
            return groups["prefix"], groups["value"].strip()
        else:
            raise ValueError(
                f"foreign key, {value!r}, does not match reference pattern "
                f"with type string"
            )
    return value, None


def catch_from_string(
    value: str, key: str, replace: Optional = None, flag: bool = True
):
    """Catch keyword from string value and return True if exits."""
    if key in value:
        return " ".join(value.replace(key, (replace or "")).split()), (
            True if flag else key
        )
    return value, (False if flag else None)


def get_function(func_string: str) -> callable:
    """Get function from imported string.

    Examples:
        >>> get_function('app.vendor.replenishment.run_prod_cls_criteria')
        app.vendor.replenishment.run_prod_cls_criteria
    """
    module, _function = func_string.rsplit(sep=".", maxsplit=1)
    mod = importlib.import_module(module)
    return getattr(mod, _function)


def convert_str_to_dict(key: str, value: Union[str, dict]) -> dict:
    """Convert value from string or not to dictionary with input key."""
    return {key: value} if isinstance(value, str) else value


def filter_ps_type(
    ps_name_full: str,
    default: str = "sql",
) -> tuple[str, str]:
    if ":" in ps_name_full:
        _name_split: list = ps_name_full.split(":")
        _type: str = _name_split.pop(0)
        return _type, _name_split[-1].split(".")[-1]
    return default, ps_name_full


def filter_not_null(datatype: str) -> bool:
    return all(not re.search(word, datatype) for word in ("default", "serial"))


AbstractSetOrDict = Union[
    AbstractSet[Union[int, str]], dict[Union[int, str], Any]
]


class BaseUpdatableModel(BaseModel):
    """Base Model that was implemented updatable method and properties."""

    @classmethod
    def parse(cls, obj) -> Self:
        return cls.parse_obj(obj)

    @classmethod
    def get_field_names(cls, alias=False):
        return list(cls.schema(alias).get("properties").keys())

    @classmethod
    def get_properties(cls) -> list:
        """Return list of properties of this model."""
        return [
            prop
            for prop in cls.__dict__
            if isinstance(cls.__dict__[prop], property)
        ]

    def dict(
        self,
        *,
        include: AbstractSetOrDict = None,
        exclude: AbstractSetOrDict = None,
        by_alias: bool = False,
        skip_defaults: bool = None,
        exclude_unset: bool = False,
        exclude_defaults: bool = False,
        **kwargs,
    ) -> dict[str, Any]:
        """Override the dict function to include our properties
        docs: https://github.com/pydantic/pydantic/issues/935
        """
        attribs = super().dict(
            include=include,
            exclude=exclude,
            by_alias=by_alias,
            skip_defaults=skip_defaults,
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            **kwargs,
        )
        props: list = self.get_properties()

        # Include and exclude properties
        if include:
            props: list = [prop for prop in props if prop in include]
        if exclude:
            props: list = [prop for prop in props if prop not in exclude]

        # Update the attribute dict with the properties
        if props:
            attribs.update({prop: getattr(self, prop) for prop in props})
        return attribs

    def update(self, data: dict):
        """Updatable method for update data to existing model data.

        docs: https://github.com/pydantic/pydantic/discussions/3139
        """
        update = self.dict()
        update.update(data)
        for k, v in self.validate(update).dict(exclude_defaults=True).items():
            setattr(self, k, v)
        return self

    class Config:
        # This config allow to validate before assign new data to any field
        validate_assignment = True
        allow_population_by_field_name = True
        use_enum_values = True
        underscore_attrs_are_private = True


class Tag(BaseUpdatableModel):
    """"""

    author: str = Field(default=None, description="Author")
    description: Optional[str] = Field(default=None, description="Description")
    labels: list = Field(default_factory=list)
    version: date = Field(default="1990-01-01", alias="TagVersion")
    ts: Optional[datetime] = Field(
        default=None,
        alias="TagTimestamp",
    )

    @validator("author", pre=True, always=True)
    def set_author(cls, value):
        return value or "undefined"

    @validator("ts", pre=True, always=True)
    def set_ts(cls, value):
        """Pre initialize the `ts` value that parsing from default."""
        return value or datetime.now()

    @validator("version", pre=True, always=True)
    def set_version(cls, value):
        """Pre initialize the `version` value that parsing from default."""
        _dt = datetime.strptime(value, "%Y-%m-%d") if value else datetime.now()
        return _dt.date()


class CommonType(BaseUpdatableModel):
    """Data Type model."""

    type: str = Field(
        ..., description="Type of data that implement with target database"
    )

    @root_validator(pre=True)
    def prepare_values(cls, values):
        if not (_type := values.get("type")):
            raise ValueError
        return values


class CharType(CommonType):
    """Character Type model."""

    max_length: Optional[int] = Field(default=None)


class NumericType(CommonType):
    """"""

    precision: Optional[int] = Field(default=None, description="")
    scale: Optional[int] = Field(default=None)


class TimeType(CommonType):
    """"""

    precision: Optional[int] = Field(default=None)
    timezone: bool = Field(default=False, description="Time zone flag")


DataType = Union[CommonType, CharType, NumericType, TimeType]


class Column(BaseUpdatableModel):
    """Column model."""

    # Necessary value
    name: str = Field(
        ...,
        description="Name of Column that match in database",
        alias="ColumnName",
    )
    datatype: Union[dict, str] = Field(
        ..., description="Data type of value of this column", alias="DataType"
    )

    # Default value
    nullable: bool = Field(
        default=True, description="Nullable flag", alias="Nullable"
    )
    unique: bool = Field(
        default=False,
        description="Unique key flag which can contain null value",
        alias="Unique",
    )
    default: Union[int, str] = Field(
        default=None,
        description="Default value of this column",
        alias="Default",
    )
    check: Optional[str] = Field(
        default=None,
        description="Check statement before insert to database",
        alias="Check",
    )

    # Special value that effect to parent model that include this model
    pk: bool = Field(
        default=False,
        description="Primary key flag which can not contain null value",
        alias="PrimaryKey",
    )
    fk: dict = Field(
        default_factory=dict,
        description="Foreign key reference",
        alias="ForeignKey",
    )

    @root_validator(pre=True)
    def prepare_datatype_from_string(cls, values):
        """Prepare datatype value that parsing to this model with different
        types, string or dict type.

        This filter will prepare datatype value from the format,
            {DATATYPE} {UNIQUE} {NULLABLE} {DEFAULT}
            {PRIMARY KEY|FOREIGN KEY} {CHECK}

        Examples:
            - varchar( 100 ) not null default 'O' check( <name> <> 'test' )
            - serial not null primary key
        """
        if not (
            datatype_key := only_one(
                values, params.map_tbl.datatype, default=False
            )
        ):
            raise ValueError("datatype does not contain in values")

        pre_datatype = values.pop(datatype_key)
        if not isinstance(pre_datatype, str):
            raise TypeError(
                f"datatype value does not support for this type, "
                f"{type(pre_datatype)}"
            )

        _datatype, _nullable = split_datatype(pre_datatype)
        values_update: dict = {"nullable": False}

        # Remove unique value from datatype
        _datatype, values_update["unique"] = catch_from_string(
            _datatype,
            "unique",
        )

        # Remove primary key value from datatype
        _datatype, values_update["pk"] = catch_from_string(
            _datatype,
            "primary key",
        )

        # Rename serial value to int from datatype
        _datatype, values_update["default"] = catch_from_string(
            _datatype, "serial", replace="int"
        )

        if "check" in _datatype:
            if m := re.search(
                r"check\s?\((?P<check>[^()]*(?:\(.*\))*[^()]*)\)",
                _datatype,
            ):
                _datatype, values_update["check"] = catch_from_string(
                    _datatype, m.group(), flag=False
                )
            else:
                raise ValueError(
                    "datatype with type string does not support for "
                    "this format of check"
                )
        if (
            re.search("default", _datatype)
            and values.get("datatype") is not None
        ):
            values["datatype"] = _datatype.split("default")[0].strip()
        else:
            values["datatype"] = _datatype
            values_update["nullable"] = not re.search("not null", _nullable)
        return values_update | values

    @validator("name", pre=True)
    def prepare_name(cls, value) -> str:
        """Prepare name."""
        return "".join(value.strip().split())

    @root_validator()
    def validate_and_check_logic_values(cls, values):
        """Validate and check logic of values."""
        pk: bool = values.get("pk", False)
        nullable: bool = values.get("nullable", True)

        # primary key and nullable does not True together
        if pk and nullable:
            raise ValueError("pk and nullable can not be True together")
        return values


class Partition(BaseModel):
    """Partition model."""

    type: Optional[str] = Field(default=None)
    columns: list[str] = Field(default_factory=list)


class Reference(BaseModel):
    """"""

    target: str
    feature: str


class ForeignKey(BaseModel):
    name: str
    reference: Reference = Field(
        ..., description="Reference mapping of foreign key"
    )


ValueListOrDict = Union[
    list[dict], dict[int, dict], dict[str, Union[str, dict]]
]


class Profile(BaseUpdatableModel):
    """Profile Model.

    note: If data parsing with empty value, it will return default of mapping
    """

    features: list[Column] = Field(
        default_factory=list,
        description="Mapping Column features with position order",
    )
    primary_key: list[str] = Field(
        default_factory=list,
        description="List of primary key or composite key",
    )
    foreign_key: list[dict] = Field(
        default_factory=list,
        description="Mapping of foreign keys",
    )
    partition: Partition = Field(
        default_factory=dict,
        description="Partition properties",
    )

    @root_validator(pre=True)
    def prepare_values(cls, values):
        logger.debug("Profile: Start validate pre-root ...")
        return values

    @validator("features", pre=True)
    def prepare_features(cls, value: ValueListOrDict):
        """Prepare features before features type validation occurs."""
        logger.debug("Profile: ... Start pre-validate features")
        # This filter will prepare features value from the format,
        # [{ 'name': NAME, 'datatype': DATATYPE, ...  }, ...]
        if not isinstance(value, (list, dict)):
            raise ValueError(
                f"features does not support for type "
                f"{type(value)} in prepare process"
            )
        elif isinstance(value, dict):
            # This filter will prepare features value from the format,
            # { NAME: DATATYPE, ... }
            # { NAME: { 'datatype': DATATYPE, ... }, ... }
            _features: list = []
            for k, v in value.items():
                if isinstance(k, str):
                    if not isinstance(v, (str, dict)):
                        raise TypeError(
                            f"the value type of mapping features "
                            f"does not support for type {type(v)}"
                        )
                    _features.append(
                        {"name": k, **convert_str_to_dict("datatype", v)}
                    )
                elif not isinstance(k, int) or not isinstance(v, dict):
                    raise TypeError(
                        f"the key type {type(k)} or the value type {type(v)} "
                        f"of features mapping does not support for process"
                    )
                else:
                    _features.append(v)
            del value
            value: list[dict] = _features
        return value

    @validator("primary_key", each_item=True)
    def validate_for_each_primary_key(cls, value, values):
        """Validate for each data in the list of primary key.

        Note: If the list does not contain any data, this process will skip.
        """
        logger.debug(
            "Profile: ... Start validate primary_key and update pk flag "
            "to feature"
        )
        _features: list[Column] = values.get("features")
        _columns_exist: list = [feature.name for feature in _features]
        if value not in _columns_exist:
            raise ValueError(
                f"{value} in primary key does not exists in features"
            )
        _index: int = _columns_exist.index(value)
        _features[_index] = _features[_index].update(
            {"pk": True, "nullable": False}
        )
        return value

    @validator("foreign_key", pre=True)
    def prepare_foreign_key(
        cls, value: Union[list[dict], dict[str, dict[str, str]]]
    ):
        """Prepare foreign key value before foreign key type validation
        occurs."""
        logger.debug("Profile: ... Start pre-validate foreign_key")
        if isinstance(value, list):
            return value
        elif not isinstance(value, dict):
            raise TypeError(
                f"foreign key does not support for type {type(value)}"
            )
        _fk: list = []
        for feature, v in value.items():
            if isinstance(v, str):
                table, ft = split_fk(v)
                _fk.append(
                    {
                        "name": feature,
                        "ref_table": table,
                        "ref_column": (ft or feature),
                    }
                )
            elif isinstance(v, dict):
                _fk.append({"name": feature, **v})
            else:
                raise TypeError(
                    f"foreign key does not support for value of mapping "
                    f"with type {type(v)}"
                )
        return _fk

    @validator("foreign_key", pre=True, each_item=True)
    def prepare_for_each_foreign_key(cls, value):
        """Prepare for each foreign key value in list before type validation
        occurs."""
        logger.debug("Profile: ... ... Start pre-validate for each foreign_key")
        if not isinstance(value, dict):
            raise TypeError(
                f"foreign key does not support for type {type(value)}"
            )
        for v in ["name", "ref_table"]:
            if v not in value:
                raise ValueError(f"foreign key value does not contain {v}")
        if "ref_column" not in value:
            value["ref_column"] = value["name"]
        return value

    @validator("foreign_key", each_item=True)
    def validate_for_each_foreign_key(cls, value, values):
        """Validate for each foreign key in list after type validation.

        This step will check and update fk value to the foreign key
        column in features.
        """
        logger.debug("Profile: ... ... Start validate for each foreign_key")
        features: list[Column] = values.get("features")
        _columns_exist: list = [feature.name for feature in features]
        if value["name"] not in _columns_exist:
            raise ValueError(
                f"{value['name']} in primary key does not exists in features"
            )
        _index: int = _columns_exist.index(value["name"])
        features[_index] = features[_index].update(
            {
                "fk": {
                    "table": value["ref_table"],
                    "column": value["ref_column"],
                }
            }
        )
        return value

    @validator("partition")
    def validate_partition(cls, value, values):
        features: list[Column] = values.get("features")
        _columns_exist: list = [feature.name for feature in features]
        for v in value.columns:
            if v not in _columns_exist:
                raise ValueError(
                    f"partition value {v!r} does not exists in features"
                )
        return value

    @root_validator(skip_on_failure=True)
    def check_values(cls, values):
        """Check."""
        logger.debug("Profile: Start validate root ...")
        return values

    def conflict(self, update: bool = False) -> str:
        """Property return conflict statement which map with primary key."""
        doing_statement: str = "UPDATE" if update else "NOTING"
        return (
            f" ON CONFLICT ( {primary} ) DO {doing_statement}"
            if (primary := ", ".join(self.primary_key))
            else ""
        )

    def columns(self, pk_included: bool = False) -> list[str]:
        """Return list of column name."""
        _columns: list[str] = [feature.name for feature in self.features]
        return (
            _columns
            if pk_included
            else list(filter(lambda x: x not in self.primary_key, _columns))
        )


class BaseProcess(BaseUpdatableModel):
    """Process model."""

    name: str = Field(..., description="Process name")
    parameter: list[str] = Field(
        default_factory=list, description="List of process's parameter"
    )
    priority: int = Field(default=0, description="Process priority")

    @root_validator(pre=True)
    def prepare_base_values(cls, values):
        """Prepare value before."""
        logger.debug("Base Process: Start validate pre-root ...")
        if not (name := values.pop("name", None)):
            raise ValueError("name does not contain")
        return {
            "name": name,
            "parameter": sorted_set(
                values.pop(only_one(list(values), params.map_tbl.param), [])
            ),
            **values,
        }


class SQLProcess(BaseProcess):
    """SQL Process."""

    statement: Union[str, dict]

    @root_validator(pre=True)
    def prepare_sql_values(cls, values):
        logger.debug("SQL Process: Start validate pre-root ...")
        return values

    @validator("statement")
    def validate_statement(cls, value):
        """Validate and convert string statement."""
        return Statement(value).generate()


class PYProcess(BaseProcess):
    """Python process."""

    function: Callable
    load: SQLProcess
    save: SQLProcess

    @root_validator(pre=True)
    def prepare_py_values(cls, values):
        logger.debug("PY Process: Start validate pre-root ...")
        return values

    @validator("function", pre=True)
    def prepare_function(cls, value) -> Callable:
        """Prepare function value."""
        return get_function(value)


Process = Union[SQLProcess, PYProcess]


class Table(BaseUpdatableModel):
    """Table/Catalog Model that receive data from yaml file and validate all
    values to standard format for core engine that processable.

    The Object of this model that why I choose Pydantic,

        >>> import yaml
        ... with open('<filename>.yaml') as f:
        ...     model = Table.parse_obj(yaml.load(f))

    This class include the statement generator methods
    """

    # Metadata of catalog model
    name: str = Field(..., description="Catalog name", alias="CatalogName")
    shortname: str = Field(
        ...,
        description="Catalog shortname which separate by _",
        alias="CatalogShortName",
    )
    prefix: str = Field(..., alias="CatalogPrefix")
    type: str = Field(default="sql", alias="CatalogType")

    # Action metadata of catalog model
    profile: Profile = Field(..., description="Profile data of catalog")
    process: dict[str, Process] = Field(
        default_factory=dict, description="Process data of catalog"
    )
    initial: Any = Field(
        default_factory=dict, description="Initial data of catalog"
    )

    # Tag metadata of catalog model
    tag: Tag = Field(default_factory=dict, description="Tag of catalog")

    @classmethod
    def parse_shortname(cls, shortname: str) -> Self:
        """Parse shortname to Table Model."""
        return cls.parse_obj(
            LoadCatalog.from_shortname(
                name=shortname,
                prefix="",
                folder="catalog",
                prefix_file="catalog",
            ).load()
        )

    @classmethod
    def parse_name(
        cls,
        fullname: str,
        *,
        additional: Optional[dict[str, Any]] = None,
    ) -> Self:
        """Parse name to Table Model."""
        _type, name = filter_ps_type(fullname)
        obj = LoadCatalog(
            name=name,
            prefix=name.split("_")[0],
            folder="catalog",
            prefix_file="catalog",
        ).load()
        obj.update({"type": _type})
        return cls.parse_obj(obj=(obj | {"additional": (additional or {})}))

    @root_validator(pre=True)
    def prepare_values(cls, values):
        logger.debug("Table: Start validate pre-root ...")

        if not (name := values.get("name")):
            raise ValueError("name does not set")
        prefix: str = name.split("_")[0]
        if not (
            profile_key := only_one(
                values,
                params.map_tbl.profile,
                default=False,
            )
        ):
            raise ValueError(
                "Profile key does not found any key represent "
                "`create`/`profile`"
            )
        elif (
            not (
                process_key := only_one(
                    values, params.map_tbl.process, default=False
                )
            )
            and prefix in params.list_tbl_prefix_must_have_process
        ):
            raise ValueError(
                "Catalog does not found any key represent "
                "`update`/`process`/`function`"
            )

        _initial: dict = {}
        if _initial_key := only_one(
            values, params.map_tbl.initial, default=False
        ):
            _initial: Any = values.pop(_initial_key)

        # Make new mapping to model
        return {
            "name": name,
            "shortname": "".join(w[0] for w in name.split("_")),
            "prefix": prefix,
            "type": values.get("type", params.list_tbl_types[0]),
            "profile": values.pop(profile_key),
            "process": (values.pop(process_key) if process_key else {}),
            "initial": _initial,
            "tag": {
                "version": values.pop("version", None),
                "description": values.pop(
                    only_one(values, params.map_tbl.desc),
                    None,
                ),
            },
            **values.get("additional", {}),
        }

    @validator("profile", pre=True)
    def prepare_profile(cls, value):
        logger.debug("Table: ... Start pre-validate profile")
        if not value:
            raise ValueError("Profile value was emptied and did not set")
        elif not isinstance(value, dict):
            raise TypeError(
                f"Profile value was not support for type {type(value)}"
            )
        return value

    @validator("process", pre=True)
    def prepare_process(cls, value, values):
        logger.debug("Table: ... Start pre-validate process")
        _processes: dict = {}
        for _ps_count, (ps_name, ps_details) in enumerate(
            sorted(
                value.items(),
                key=lambda x: x[1].get("priority", 99),
                reverse=False,
            )
        ):
            if (
                ps_type := ps_details.get("type", values["type"])
            ) not in params.list_tbl_types:
                raise ValueError(
                    f"framework does not support for process type {ps_type!r}"
                )
            _processes[ps_name] = {
                "name": ps_name,
                "parameter": sorted_set(
                    ps_details.get(
                        only_one(list(ps_details), params.map_tbl.param),
                        [],
                    )
                ),
                "priority": _ps_count + 1,
            }
            if ps_type == "sql":
                _processes[ps_name]["statement"] = ps_details.get(
                    only_one(list(ps_details), params.map_tbl.stm), ""
                )
            elif ps_type == "py":
                if not (
                    ps_func_key := only_one(
                        list(ps_details), params.map_tbl.func, default=False
                    )
                ):
                    raise ValueError(
                        f"Function does not set while process {ps_name!r} "
                        f"has type {ps_type!r}"
                    )
                _processes[ps_name]["function"] = ps_details[ps_func_key]
                for stage in {"load", "save"}:
                    sub_stage_details: dict = ps_details.get(stage, {})
                    _processes[ps_name][stage]: dict = {
                        "name": f"{stage}_{ps_name}",
                        "parameter": sorted_set(
                            sub_stage_details.get(
                                only_one(
                                    list(sub_stage_details),
                                    params.map_tbl.param,
                                ),
                                [],
                            )
                        ),
                        "statement": sub_stage_details.get(
                            only_one(
                                list(sub_stage_details), params.map_tbl.stm
                            ),
                            "",
                        ),
                    }
            else:
                raise ValueError(
                    f"Process type {ps_type!r} does not support yet"
                )
        return _processes

    @validator("initial", pre=True)
    def prepare_initial(cls, value, values):
        logger.debug("Table: ... Start pre-validate initial")

        # If profile was raise ValidationError, it will return None value
        profile: Optional[Profile] = values.get("profile")
        return value if (profile and profile.features) else {}

    @validator("initial")
    def set_initial(cls, value, values):
        """Post prepare initial value."""
        logger.debug("Table: ... Start validate initial")
        _initial: dict = {
            "parameter": sorted_set(
                value.get(only_one(list(value), params.map_tbl.param), [])
            ),
        }
        if not value:
            return value
        elif not (
            _value_key := only_one(
                list(value),
                ["value", "values", "file", "files"],
                default=False,
            )
        ):
            _initial["statement"]: str = Statement(
                value.get(only_one(list(value), params.map_tbl.stm), "")
            ).generate()
            return _initial

        profile: Profile = values["profile"]
        _columns: list = [
            feature.name
            for feature in profile.features
            if all(_ not in feature.datatype for _ in {"default", "serial"})
        ]

        if _value_key in {"file", "files"}:
            _values = load_json_to_values(value[_value_key], schema=_columns)
        else:
            _values = value[_value_key]

        initial_value: str = (
            ", ".join([f"({_value})" for _value in _values])
            if isinstance(_values, list)
            else f"({_values})"
        )
        _stm: str = (
            f"insert into "
            f"{{database_name}}.{{ai_schema_name}}.{values['name']} "
            f"as {values['shortname']} "
            f"( {', '.join(_columns)} ) "
            f"values {initial_value}{profile.conflict()}"
        )
        _initial["statement"]: str = reduce_stm(_stm, add_row_number=False)
        return _initial

    def validate_name_flag(self, flag: Optional[Union[str, bool]]) -> bool:
        """Validate flag of name."""
        try:
            return must_bool(flag, force_raise=True)
        except ValueError:
            return any(
                self.name.startswith(params.map_tbl_flag.get(_)) for _ in flag
            )

    def validate_columns(
        self, columns: Union[list, dict], raise_error: bool = False
    ) -> list:
        """Validate column of features."""
        _filter: list = list(
            filter(lambda c: c in self.profile.columns(), columns)
        )
        if len(_filter) != len(columns) and raise_error:
            _filter_out: set = set(columns).difference(set(_filter))
            raise ValueError(
                f"Column validate does not exists in {self.name} "
                f"from {list(_filter_out)}"
            )
        return (
            _filter
            if isinstance(columns, list)
            else {_col: columns[_col] for _col in columns if _col in _filter}
        )

    def dependency(self) -> dict[str, dict[int, tuple[str]]]:
        """Return dependencies mapping."""
        _result: dict = {}
        # FIXME: this method does not support for process type "py"
        for ps, attrs in sorted(
            self.process.items(),
            key=lambda x: x[1].priority,
            reverse=False,
        ):
            stm: Statement = Statement(attrs.statement)
            _result[ps] = stm.mapping()
        return _result


class Function(BaseUpdatableModel):
    """Function/Catalog Model that receive data from yaml file and validate all
    values to standard format for core engine that processable."""

    # Metadata of pipeline model
    name: str = Field(..., description="Function name", alias="FunctionName")
    shortname: str = Field(
        ...,
        description="Function shortname which separate by _",
        alias="FunctionShortName",
    )
    prefix: str = Field(..., alias="CatalogPrefix")
    type: str = Field(default="func", alias="CatalogType")

    # Action metadata of catalog model
    profile: SQLProcess = Field(..., description="")

    # Tag metadata of catalog model
    tag: Tag = Field(default_factory=dict, description="Tag of Function")

    @classmethod
    def parse_name(cls, fullname: str):
        """Parse name to Function Model."""
        _type, name = filter_ps_type(
            fullname, default=params.list_func_types[0]
        )
        obj = LoadCatalog(
            name=name,
            prefix="",
            folder=params.map_func_types.get(_type, "function"),
            prefix_file=_type,
        ).load()
        obj.update({"type": _type})
        return cls.parse_obj(obj)

    @root_validator(pre=True)
    def prepare_values(cls, values):
        logger.debug("Function: Start validate pre-root ...")
        if not (name := values.get("name")):
            raise ValueError("name does not set")

        prefix: str = name.split("_")[0]
        if not (
            profile_key := only_one(
                values, params.map_func.create, default=False
            )
        ):
            raise ValueError(
                "Catalog does not found any key represent "
                "`create`/`function`/`statement`"
            )
        return {
            "name": name,
            "shortname": "".join(w[0] for w in name.split("_")),
            "prefix": prefix,
            "type": values.get("type", params.list_func_types[0]),
            "profile": values.pop(profile_key),
            "tag": {
                "version": values.pop("version", None),
                "description": values.pop(
                    only_one(values, params.map_func.desc),
                    None,
                ),
            },
        }

    @validator("profile", pre=True)
    def prepare_profile(cls, value, values):
        logger.debug("Function: ... Start pre-validate profile")
        if not value:
            raise ValueError("Profile value was emptied and did not set")

        name: str = values["name"]
        if isinstance(value, str):
            return {
                "name": name,
                "parameter": [],
                "statement": value,
            }
        elif isinstance(value, dict):
            if not (_stm_key := only_one(list(value), params.map_func.stm)):
                raise ValueError(
                    "Function profile does not set statement while profile key "
                    "was dict type"
                )
            return {
                "name": name,
                "parameter": sorted_set(
                    value.get(only_one(list(value), params.map_func.param), [])
                ),
                "statement": value.get(_stm_key, ""),
            }
        raise ValueError(
            f"Function profile does not support for create type {type(value)!r}"
        )


class Pipeline(BaseUpdatableModel):
    """Pipeline model."""

    # Metadata of pipeline model
    name: str = Field(..., description="Pipeline name", alias="PipelineName")
    shortname: str = Field(
        ...,
        description="Pipeline shortname which separate by _",
        alias="PipelineShortName",
    )

    id: str = Field(..., description="Pipeline ID")
    priority: int = Field(default=0)
    schedule: list = Field(default_factory=list)
    trigger: Union[list, set] = Field(default_factory=list)
    alert: list = Field(default_factory=list)
    nodes: dict[Union[int, float], dict] = Field(default_factory=dict)

    # Tag metadata of catalog model
    tag: Tag = Field(default_factory=dict, description="Tag of Pipeline")

    @classmethod
    def parse_name(cls, name: str):
        obj = LoadCatalog(
            name=name,
            prefix="",
            folder="pipeline",
            prefix_file="pipeline",
        ).load()
        return cls.parse_obj(obj)

    @root_validator(pre=True)
    def prepare_values(cls, values):
        logger.debug("Pipeline: Start validate pre-root ...")

        if not (name := values.get("name")):
            raise ValueError("name does not set")
        if not (pipe_id := only_one(values, params.map_pipe.id, default=False)):
            raise ValueError(
                "Catalog does not found any key represent `id`/`pipeline_id`"
            )
        if not (
            node_key := only_one(values, params.map_pipe.nodes, default=False)
        ):
            raise ValueError(
                "Catalog does not found any key represent `nodes`/`tables`"
            )
        # Make new mapping to Model
        return {
            "name": name,
            "shortname": "".join(w[0] for w in name.split("_")),
            "id": values.pop(pipe_id),
            "priority": values.pop("priority", None),
            "schedule": must_list(
                values.get(only_one(values, params.map_pipe.schedule), [])
            ),
            "trigger": values.get(
                only_one(values, params.map_pipe.trigger), []
            ),
            "alert": must_list(
                values.get(only_one(values, params.map_pipe.alert), [])
            ),
            "nodes": values.pop(node_key),
            "tag": {
                "description": values.get(
                    only_one(values, params.map_pipe.desc), ""
                ),
            },
        }

    @validator("trigger", pre=True)
    def prepare_trigger(cls, value, config):
        """Prepare trigger value :structure: (i)     trigger: [ 'pipe-id-01',
        ... ]

        (ii)    trigger: 'pipe-id-01 & (pipe-id-02 | pipe-id-03)'
        """

        def __prepare_trigger(trigger: str) -> list:
            """Prepare trigger string value to list value."""
            _trigger: str = "".join(trigger.strip().split())
            for _ in re.findall(r"(\([A-Za-z0-9_&|]+\))", _trigger):
                if (
                    config.pipe_cond_and not in _
                    and config.pipe_cond_or not in _
                ):
                    _trigger = _trigger.replace(_, _.strip("()"))
            _trigger: str = _trigger.replace("(", "( ").replace(")", " )")
            return [
                x.strip()
                for x in (
                    _trigger.replace(
                        config.pipe_cond_and, f" {config.pipe_cond_and} "
                    )
                    .replace(config.pipe_cond_or, f" {config.pipe_cond_or} ")
                    .split()
                )
            ]

        if isinstance(value, list):
            return value

        _trigger_split: list = __prepare_trigger(value)
        if any(
            _ not in {config.pipe_cond_and, config.pipe_cond_or}
            for _ in list(
                filter(lambda x: x not in {"(", ")"}, _trigger_split)
            )[1::2]
        ):
            raise ValueError(
                "trigger property does not valid with logical condition"
            )
        elif _trigger_split.count("(") != _trigger_split.count(")"):
            raise ValueError(
                "trigger property does not valid with bracket of "
                "logical condition"
            )
        return cls.__generate_condition(
            _trigger_split, config.pipe_cond_and, config.pipe_cond_or
        )

    @staticmethod
    def __generate_condition(
        trigger_lists: Union[list, Iterator[str]],
        trigger_ane: str,
        trigger_or: str,
    ) -> Union[list, set]:
        logger.debug("Generate trigger condition ...")

        def convert_element_with_type(mapping: dict) -> Union[list, set]:
            return mapping["type"](mapping["element"])

        type_map: dict = {trigger_ane: list, trigger_or: set}
        _default: list = [{"element": [], "type": list}]
        _default_index = 0
        for trigger in trigger_lists:

            if trigger in type_map:
                _default[_default_index]["type"] = type_map[trigger]
                continue

            if trigger.startswith("("):
                _default_index += 1
                _default.append({"element": [], "type": list})
            elif trigger.endswith(")"):
                if _default_index == 0:
                    raise ValueError(
                        "trigger property does not valid with bracket of "
                        "logical condition with ')'"
                    )
                _convert_result = convert_element_with_type(
                    _default.pop(_default_index)
                )
                _default_index -= 1
                _default[_default_index]["element"].append(_convert_result)
            else:
                _default[_default_index]["element"].append(trigger)
        if len(_default) > 1:
            raise ValueError(
                "trigger property does not valid with bracket of "
                "logical condition with '('"
            )
        return convert_element_with_type(_default[0])

    @validator("nodes", pre=True)
    def prepare_nodes(cls, value):
        """Prepare nodes value :structure:

        (i)     <node_name>:
                    priority: 1
                    type: "<node_type>"
                    choose: ['choose_process', ...]

        (i.a)   <node_name_full: `node_type:node_name`>:
                    priority: 1
                    choose: ['choose_process', ...]

        (ii)    <node_name_full: `node_type:node_name`>:
                    - 'choose_process'
                    - ...

        (iii)   - name: <node_name_full: `node_type:node_name`>
                  choose: []
                - name: <node_name_full: `node_type:node_name`>
                ...

        (iv)    - "<node_name_full: `node_type:node_name`>"
                - "<node_name_full: `node_type:node_name`>"
                ...
        """
        _nodes: dict = {}
        if isinstance(value, list):
            for _default_priority, _node_props in enumerate(value, start=1):
                _name, _priority, _choose = cls.__generate_node_props(
                    _default_priority, _node_props
                )
                _nodes[_priority] = {"name": _name, "choose": _choose}
        elif isinstance(value, dict):
            _default_priority: float = 1
            for node_name, node_props in sorted(
                value.items(),
                key=lambda x: x[1].get("priority", 99),
                reverse=False,
            ):
                _name, _priority, _choose = cls.__generate_node_props(
                    _default_priority, node_props, node_name
                )
                _nodes[_priority] = {"name": _name, "choose": _choose}
                _default_priority: float = _priority + 0.1
        return _nodes

    @staticmethod
    def __generate_node_props(
        priority: float,
        node_props: Union[list, dict],
        node_name: Optional[str] = None,
    ):
        """Generate node properties with different type of node input
        argument."""
        if isinstance(node_props, list):
            _priority: float = round(priority, 2)
            _node_choose: list = node_props
            _name: str = node_name
        elif isinstance(node_props, dict):
            _priority: float = round(node_props.get("priority") or priority, 2)
            _choose: str = only_one(
                list(node_props), params.map_pipe.choose, default=True
            )
            _type: str = only_one(
                list(node_props), params.map_pipe.type, default=True
            )
            if not node_name:
                node_name: str = node_props["name"]
            _name: str = (
                f"{_node_type}:{node_name}"
                if (
                    (_node_type := node_props.get(_type))
                    and ":" not in node_name
                )
                else node_name
            )
            _node_choose: Optional[list] = node_props.get(_choose, [])
        elif isinstance(node_props, str):
            _priority = priority
            _node_choose: list = []
            _name: str = node_props
        else:
            raise ValueError(
                f"node properties does not support for '{type(node_props)}'"
            )
        return _name, _priority, _node_choose

    @validator("nodes")
    def validate_nodes(cls, value, values):
        """Validate nodes value."""
        name = values["name"]
        for _priority, v in value.items():
            try:
                Table.parse_name(fullname=v["name"])
            except ValidationError as e:
                raise ValueError(
                    f"From {name}, node name {v['name']} does not exists"
                ) from e
        return value

    class Config:
        pipe_cond_and: str = "&"
        pipe_cond_or: str = "|"


class Schema(BaseUpdatableModel):
    """Schema model."""

    name: str = Field(..., description="Schema name", alias="SchemaName")


class Parameter(BaseUpdatableModel):
    """Parameter Model that receive data from interface of framework."""

    others: dict = Field(
        default_factory=dict,
        description="Other parameters",
    )
    type: ParameterType = Field(
        default=ParameterType.UNDEFINED.value, description="Parameter type"
    )
    name: Optional[str] = Field(default=None, description="Parameter name")
    dates: list[str] = Field(
        default_factory=lambda: [get_run_date()], description="Parameter dates"
    )
    mode: ParameterMode = Field(
        default=ParameterMode.COMMON.value, description="Module mode parameter"
    )
    drop_table: bool = Field(default=False, description="Drop Table parameter")
    drop_schema: bool = Field(
        default=False, description="Drop Schema parameter"
    )
    cascade: bool = Field(
        default=False,
        description="Cascade flag parameter",
    )

    @root_validator(pre=True)
    def prepare_values(cls, values):
        """Prepare values after parsing to validators."""
        logger.debug("Parameter: Start validate pre-root ...")
        if _name := values.pop("table_name", None):
            values["type"] = ParameterType.TABLE
        elif _name := values.pop("pipeline_name", None):
            values["type"] = ParameterType.PIPELINE
        else:
            _name = values.get("name", None)
        values["name"] = _name

        # Filter others parameters
        _exist_others: dict = values.pop("others", {})
        _others: dict = {
            value: values[value]
            for value in values
            if (
                value != "others"
                and value not in cls.get_field_names(alias=True)
                and value not in cls.get_field_names(alias=False)
            )
        }
        return {"others": _others | _exist_others, **values}

    @validator("dates", always=True)
    def prepare_dates(cls, value, values):
        logger.debug("Parameter: Start validate always dates ...")
        if rd := values["others"].pop("run_dates", None):
            return rd
        elif rd := values["others"].pop("run_date", None):
            return [rd]
        return value

    def is_table(self) -> bool:
        return self.type == ParameterType.TABLE


class FrameworkParameter(BaseUpdatableModel):
    run_id: int
    run_date: date
    run_mode: str


class MapParameter(BaseUpdatableModel):
    fwk_params: FrameworkParameter = Field(
        default_factory=dict,
        description="Framework Parameters from the application framework",
    )

    ext_params: dict = Field(
        default_factory=dict,
        description="External Parameters from the application framework",
    )


class ReleaseDate(BaseModel):
    """Release Date Model that use to tracking checkpoint of runner method on
    the Task model."""

    date: Optional[str] = Field(default=None, description="Date that release")
    index: Optional[int] = Field(
        default=None, description="Index of this release"
    )
    pushed: bool = Field(
        default=False,
        description="If true that mean this the process already pushed",
    )


class Task(BaseUpdatableModel):
    """Task Model that generate or receive task from the framework data."""

    module: str = Field(..., description="Task module")
    parameters: Parameter = Field(
        default_factory=Parameter,
        description="Task parameters",
    )
    mode: TaskMode = Field(
        default=TaskMode.FOREGROUND.value,
        description="Task mode with foreground or background",
    )
    component: TaskComponent = Field(
        default=TaskComponent.UNDEFINED.value,
        description="Task component of the data application framework",
    )
    status: Status = Field(
        default=Status.WAITING.value,
        description="Task status",
    )
    id: Optional[str] = Field(
        default=None,
        description="Task ID",
    )
    message: str = Field(default="", description="Task Message")
    start_time: datetime = Field(
        default_factory=partial(get_run_date, "date_time")
    )
    release: ReleaseDate = Field(default_factory=ReleaseDate)

    @classmethod
    def make(cls, module: str) -> Task:
        return cls(module=module)

    @root_validator(pre=True)
    def root_validate(cls, values):
        logger.debug("Task: Start validate pre-root ...")
        return values

    @validator("parameters", always=True)
    def prepare_parameters(cls, value: Parameter, values) -> Parameter:
        """Prepare parameter value."""
        logger.info("Task: Start validate always parameters")
        if value.type == ParameterType.UNDEFINED:
            value = value.copy(update={"name": values["module"]})
        return value

    @validator("id", always=True)
    def generate_id(cls, value, values) -> str:
        """Generate Task ID."""
        logger.info("Task: Start validate always id")
        return value or get_process_id(
            values["module"]
            + values["parameters"].type
            + values["parameters"].name
        )

    def duration(self) -> int:
        """Generate duration since this model start initialize data."""
        return round(
            (
                get_run_date(date_type="date_time") - self.start_time
            ).total_seconds()
        )

    def runner(
        self,
        start: int = 0,
    ) -> Generator[tuple[int, str], None, None]:
        """Yield index and date values from enumerate of dates."""
        for idx, dt in enumerate(self.parameters.dates, start=0):
            if idx < start:
                continue
            self.message += self.__add_newline(
                msg=f"[ run_date: {dt} ]", checker=self.message
            )
            self.release: ReleaseDate = ReleaseDate(
                date=dt, index=idx, pushed=(idx != start)
            )
            yield idx, dt

        # NOTE: Revert the release value to default
        self.release = ReleaseDate()

    def receive(self, result: Result) -> Task:
        """Receive result dataclass and merge status and message to self."""
        logger.debug("Task: Start Receive data from Result")
        self.status = result.status
        self.message += self.__add_newline(
            result.message, checker=result.message
        )
        logger.debug("Task: End Receive data from Result")
        return self

    @staticmethod
    def __add_newline(msg: str, checker: str) -> str:
        """Return added newline message for empty string."""
        return f"\n{msg}" if checker else msg

    def is_successful(self) -> bool:
        return self.status == Status.SUCCESS

    def is_waiting(self) -> bool:
        return self.status == Status.WAITING

    def is_failed(self) -> bool:
        return self.status == Status.FAILED


class TableFrontend(Table):
    """Table Catalog for Frontend component."""

    @property
    def catalog(self):
        return {
            "id": self.shortname,
            **self.dict(
                exclude={"catalog", "tag"},
                by_alias=False,
            ),
        }


class FunctionFrontend(Function):
    """Function Catalog for Frontend component."""

    @property
    def catalog(self):
        return {
            "id": self.shortname,
            **self.dict(exclude={"catalog", "tag"}, by_alias=False),
        }


class PiplineFrontend(Pipeline):
    """Pipeline Catalog for Frontend component."""

    @property
    def catalog(self):
        return {
            "type": "pipe",
            "prefix": "pipe",
            **self.dict(exclude={"catalog", "tag"}, by_alias=False),
        }


@singledispatch
def process(model):
    """Default processing definition."""
    raise NotImplementedError(f"I don't know how to process {type(model)}")


@process.register
def _(model: Pipeline):
    """Handle pipeline model."""
    print(f"Pipeline: {model}")


@process.register
def _(model: Table):
    """Handle table model."""
    print(f"Table: {model}")
