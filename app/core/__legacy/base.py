# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------

"""This is the legacy base file for generate validator object with pure python
code without any validate class package.

The modern we will migrate the validator and statement generator to the Pydantic
package with split to two files, validators.py and statements.py.

Note:
- If any methods were migrated to modern, it will have comment on top of method
  definition, `[x] Migrate to modern style`.
"""

import datetime as dt
import fnmatch
import importlib
import operator
import os
import re
from typing import (
    Optional,
    Union,
)

from collections.abc import Iterator

import yaml
from dateutil import tz
from dateutil.relativedelta import relativedelta

from app.core.connections.io import load_json_to_values
from app.core.errors import (
    CatalogArgumentError,
    CatalogNotFound,
    TableNotImplement,
    TableValidatorError,
)
from app.core.__legacy.convertor import (
    Statement,
    reduce_stm,
)
from app.core.__legacy.models import VerboseObject
from app.core.utils.config import (
    AI_APP_PATH,
    Params,
)
from app.core.utils.logging_ import logging
from app.core.utils.reusables import (
    hash_string,
    merge_dicts,
    must_bool,
    must_list,
    only_one,
)

params = Params(param_name="parameters.yaml")
registers = Params(param_name="registers.yaml")
logger = logging.getLogger(__name__)


def verbose_log(
    obj: Union[VerboseObject, str],
    message: str,
    *,
    lvl: Optional[int] = 0,
    end: Optional[str] = None,
) -> None:
    """Verbose logging function that control the logging message with DEBUG
    level of any object instance processes."""
    if obj.verbose:
        prefix_level: str = (
            f"{gen} " if (gen := " ".join(["..."] * lvl)) else ""
        )
        logger.debug(prefix_level + message)
        if end:
            logger.debug(end * 75)


# [x] Migrate to modern style base
def sort_by_priority(
    values: Union[list, dict], priority_lists: Optional[list] = None
):
    """Sorted list by string prefix priority."""
    _priority_lists: list = priority_lists or params.list_tbl_priority
    priority_dict: dict = {k: i for i, k in enumerate(_priority_lists)}

    def priority_getter(value):
        return next(
            (
                order
                for _, order in priority_dict.items()
                if value.startswith(_)
            ),
            len(values),
        )

    if isinstance(values, list):
        return sorted(values, key=priority_getter)
    else:
        return {
            k: values[k] for k in sorted(values.keys(), key=priority_getter)
        }


# [x] Migrate to modern style base
def get_run_date(
    date_type: str = "str", fmt: str = "%Y-%m-%d"
) -> Union[str, dt.datetime, dt.date]:
    """Get run_date value from now datetime :usage: >>
    get_run_date(date_type='datetime', fmt='%Y%m%d')

    >> get_run_date(fmt='%Y/%m/%d') '2022/01/01'
    """
    run_date: dt.datetime = dt.datetime.now(tz.gettz("Asia/Bangkok"))
    if date_type == "str":
        return run_date.strftime(fmt)
    return run_date.date() if date_type == "date" else run_date


# [x] Migrate to modern style base
def get_plural(
    num: int,
    word_change: Optional[str] = None,
    word_start: Optional[str] = None,
) -> str:
    """Get plural word for dynamic `num` number if more than 1 or not :usage:
    >>> get_plural(100) 's'.

        >>> get_plural(1, word_change='ies', word_start='y')
        'y'

        >>> get_plural(3, 'es')
        'es'
    """
    return (word_change or "s") if num > 1 else (word_start or "")


def get_process_id(process: str, fmt: str = "%Y%m%d%H%M%S%f") -> str:
    """Get process ID from input string that combine timestamp and hashing of
    argument process together."""
    return get_run_date(fmt=fmt)[:-2] + hash_string(process)


def get_process_date(
    run_date: Union[str, dt.date],
    run_type: str,
    *,
    invert: bool = False,
    date_type: str = "str",
    fmt: str = "%Y-%m-%d",
) -> Union[str, dt.date]:
    """Get process_date value that convert by `run_type` value like 'daily',
    'weekly', etc. :usage: >>> get_process_date('2022-01-20', 'monthly')
    '2022-01-01'.

        >>> get_process_date('2022-01-20', 'monthly', invert=True)
        '2022-01-31'

        >>> get_process_date('2022-01-20', 'weekly')
        '2022-01-17'
    """
    run_type: str = (
        run_type if run_type in params.map_tbl_ps_date.keys() else "daily"
    )
    run_date_ts: dt.date = (
        dt.date.fromisoformat(run_date)
        if isinstance(run_date, str)
        else run_date
    )

    if run_type == "weekly":
        run_date_convert_ts = (
            run_date_ts - dt.timedelta(run_date_ts.weekday())
            if invert
            else run_date_ts - dt.timedelta(run_date_ts.isoweekday())
        )
    elif run_type == "monthly":
        run_date_convert_ts = (
            run_date_ts.replace(day=1)
            + relativedelta(months=1)
            - relativedelta(days=1)
            if invert
            else run_date_ts.replace(day=1)
        )
    elif run_type == "yearly":
        run_date_convert_ts = (
            run_date_ts.replace(month=1, day=1)
            + relativedelta(years=1)
            - relativedelta(days=1)
            if invert
            else run_date_ts.replace(month=1, day=1)
        )
    else:
        run_date_convert_ts = run_date_ts
    return (
        run_date_convert_ts.strftime(fmt)
        if date_type == "str"
        else run_date_convert_ts
    )


def get_cal_date(
    data_date: dt.date,
    mode: str,
    run_type: str,
    cal_value: int,
    date_type: str = "str",
    fmt: str = "%Y-%m-%d",
) -> Union[str, dt.date]:
    """Get date with internal calculation logic."""
    if mode not in {
        "add",
        "sub",
    }:
        raise NotImplementedError(
            f"Get calculation datetime does not support for mode: {mode!r}"
        )
    _result: dt.date = getattr(operator, mode)(
        data_date,
        relativedelta(**{params.map_tbl_ps_date[run_type]: cal_value}),
    )
    return _result.strftime(fmt) if date_type == "str" else _result


def get_function(func_string: str) -> callable:
    """Get function from imported string :usage: ..> get_function( ...

    func_string='vendor.replenishment.run_prod_cls_criteria'
    ... )
    """
    module, _function = func_string.rsplit(sep=".", maxsplit=1)
    mod = importlib.import_module(module)
    return getattr(mod, _function)


def _get_config_filter_path(
    path: str,
    config_dir: str,
    config_prefix: Optional[str] = None,
    config_prefix_file: Optional[str] = None,
) -> bool:
    """Path filtering gateway of configuration directory."""
    if config_dir == "catalog":
        _conf_pre: str = config_prefix or ""
        _conf_pre_file: str = config_prefix_file or "catalog"
        return fnmatch.fnmatch(path, f"{_conf_pre_file}_{_conf_pre}*.yaml")
    elif config_dir in {"function", "view", "adhoc"}:
        _conf_pre_file: str = config_prefix_file or "*"
        return fnmatch.fnmatch(path, f"{_conf_pre_file}_*.yaml")
    elif config_dir == "pipeline":
        return fnmatch.fnmatch(path, "pipeline_*.yaml")
    return False


def _get_config_filter_key(keys, conf, all_mode: bool = True) -> bool:
    """Key filtering gateway of configuration."""
    return (
        set(keys).issubset(set(conf))
        if all_mode
        else len(set(keys).intersection(set(conf))) > 0
    )


# [x] Migrate to modern style by class object `LoadConfig`
def get_config_sht(
    config_name_sht: str,
    config_prefix: str,
    folder_config: str,
    config_prefix_file: str,
) -> dict:
    """Get raw configuration from .yaml file with shortname searching
    engine."""
    prefix: str = f"{config_prefix}_" if config_prefix else ""
    conf_path = os.path.join(AI_APP_PATH, registers.path.conf, folder_config)
    _results: list = []
    for file in sorted(os.listdir(conf_path), reverse=False):
        if _get_config_filter_path(
            file, folder_config, prefix, config_prefix_file
        ):
            with open(os.path.join(conf_path, file), encoding="utf8") as f:
                _tbl_data = yaml.load(f, Loader=yaml.Loader)

                for _tbl in _tbl_data:
                    if (
                        "".join(x[0] for x in _tbl.split("_"))
                        == config_name_sht
                    ):
                        _result = _tbl_data[_tbl]
                        _result["config_name"] = _tbl
                        _results.append(_result)

                del _tbl_data
    if _results:
        return sorted(
            _results,
            key=lambda x: dt.datetime.fromisoformat(
                x.get("version", "1990-01-01")
            ),
            reverse=True,
        )[0]
    raise CatalogNotFound(
        f"catalog_name_short: {config_name_sht!r} not found in "
        f"`./conf/{folder_config}/{config_prefix_file}_{prefix}*.yaml`"
    )


# [x] Migrate to modern style by class object `LoadConfig`
def get_config(
    config_name: str,
    config_prefix: str,
    folder_config: str,
    config_prefix_file: str,
) -> dict:
    """Get the latest raw configuration from .yaml file which sorting by
    `version` key in configuration data descending."""
    prefix: str = f"{config_prefix}_" if config_prefix else ""
    conf_path = os.path.join(AI_APP_PATH, registers.path.conf, folder_config)
    _results: list = []
    for file in sorted(os.listdir(conf_path), reverse=False):
        if _get_config_filter_path(
            file, folder_config, prefix, config_prefix_file
        ):
            with open(os.path.join(conf_path, file), encoding="utf8") as f:
                _config_data: dict = yaml.load(f, Loader=yaml.Loader)
                if _result := _config_data.get(config_name, {}):
                    _result["config_name"] = config_name
                    _results.append(_result)
                del _config_data
    if _results:
        return sorted(
            _results,
            key=lambda x: dt.datetime.fromisoformat(
                x.get("version", "1990-01-01")
            ),
            reverse=True,
        )[0]
    raise CatalogNotFound(
        f"catalog_name: {config_name!r} not found in "
        f"`./conf/{folder_config}/{config_prefix_file}_{prefix}*.yaml`"
    )


# [x] Migrate to modern style and rename to `get_catalogs`
def get_catalog_all(
    folder_config: Optional[Union[str, list]] = None,
    key_exists: Optional[Union[str, list]] = None,
    key_exists_all_mode: bool = True,
    priority_sorted: bool = False,
):
    """Get all raw configuration from .yaml file."""
    _key_exists: list = must_list(key_exists)
    _folder_config: list = must_list(
        folder_config or ["catalog", "pipeline", "function"]
    )
    conf_paths = (
        (os.path.join(AI_APP_PATH, registers.path.conf, x), x)
        for x in _folder_config
    )
    _files: dict = {}
    for conf_path, fol_conf in conf_paths:
        for file in sorted(os.listdir(conf_path), reverse=False):
            if _get_config_filter_path(file, config_dir=fol_conf):
                with open(os.path.join(conf_path, file), encoding="utf8") as f:
                    _config_data_raw: dict = yaml.load(f, Loader=yaml.Loader)
                    _config_data: dict = (
                        {
                            k: v
                            for k, v in _config_data_raw.items()
                            if _get_config_filter_key(
                                _key_exists, v, all_mode=key_exists_all_mode
                            )
                        }
                        if _key_exists
                        else _config_data_raw
                    )
                    _files: dict = merge_dicts(_files, _config_data)
                    del _config_data_raw, _config_data
    return sort_by_priority(_files) if priority_sorted else _files


def split_datatype(datatype_full: str) -> tuple[str, str]:
    for null_str in ["not null", "null"]:
        if search := re.search(null_str, datatype_full):
            _nullable: str = search[0].strip()
            return datatype_full.replace(_nullable, "").strip(), _nullable
    return datatype_full.strip(), "null"


def filter_ps_type(ps_name_full: str) -> tuple[str, str]:
    if ":" in ps_name_full:
        _name_split: list = ps_name_full.split(":")
        _type: str = _name_split.pop(0)
        return _type, _name_split[-1].split(".")[-1]
    return "sql", ps_name_full


def filter_sys_auto(datatype: str) -> bool:
    """Return True if datatype is not System auto generate column."""
    return all(not re.search(word, datatype) for word in ["default", "serial"])


class TblCatalog:
    """Table Catalog for generate all configuration from .yaml file."""

    __slots__ = (
        "tbl_name",
        "tbl_type",
        "tbl_name_sht",
        "tbl_prefix",
        "tbl_catalog",
        "tbl_process_count",
        "verbose",
    )

    @classmethod
    def short(cls, short_name: str):
        _tbl_catalog: dict = get_config_sht(
            short_name,
            config_prefix="",
            folder_config="catalog",
            config_prefix_file="catalog",
        )
        return cls(
            tbl_name=_tbl_catalog["config_name"], tbl_catalog=_tbl_catalog
        )

    @classmethod
    def loads(cls, tbl_name_full: str):
        tbl_type, tbl_name = filter_ps_type(tbl_name_full)
        return cls(tbl_name=tbl_name, tbl_type=cls.validate_tbl_type(tbl_type))

    @classmethod
    def validate_tbl_type(cls, tbl_type: str) -> str:
        if tbl_type not in params.list_tbl_types:
            raise TableNotImplement(
                f"AI framework does not support for table type {tbl_type!r}"
            )
        return tbl_type

    def __init__(
        self,
        tbl_name: str,
        tbl_type: Optional[str] = None,
        tbl_catalog: Optional[dict] = None,
        verbose: bool = False,
    ):
        self.tbl_name: str = tbl_name
        self.tbl_type: str = tbl_type or params.list_tbl_types[0]
        self.tbl_name_sht: str = "".join(
            [word[0] for word in self.tbl_name.split("_")]
        )
        self.tbl_prefix: str = self.tbl_name.split("_")[0]
        self.verbose = verbose
        self.tbl_catalog: dict = self.get_tbl_catalog(config=tbl_catalog)
        if self.tbl_prefix not in params.list_tbl_priority:
            logger.warning(
                f"The config {self.tbl_name} have table prefix name, "
                f"`{self.tbl_prefix}`, which different from the agreed list"
            )

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"({self.tbl_name!r}, tbl_type={self.tbl_type!r})"
        )

    def __str__(self):
        return f"{self.tbl_type}:{self.tbl_name}"

    # [x] Migrate to modern style
    @property
    def catalog(self) -> dict:
        return {
            "id": self.tbl_name_sht,
            "name": self.tbl_name,
            "type": self.tbl_type,
            "prefix": self.tbl_prefix,
            **self.tbl_catalog,
        }

    @property
    def tbl_profile(self) -> dict:
        return self.tbl_catalog["profile"]

    @property
    def tbl_process(self) -> dict:
        return self.tbl_catalog["process"]

    @property
    def tbl_initial(self) -> dict:
        return self.tbl_catalog.get("initial", {})

    @property
    def tbl_primary_key(self) -> list:
        if any("primary_key_" in _ for _ in self.tbl_profile.keys()):
            logger.warning(
                f"The config {self.tbl_name!r} does not have real primary key "
                f"that set in database"
            )
            return self.tbl_profile.get("primary_key", [])
        return self.tbl_profile["primary_key"]

    @property
    def tbl_foreign_key(self) -> dict:
        return self.tbl_profile.get("foreign_key", {})

    @property
    def tbl_partition_type(self) -> str:
        return only_one(
            list(self.tbl_profile), params.map_tbl.partition, default=False
        )

    @property
    def tbl_partition(self) -> list:
        return self.tbl_profile.get(self.tbl_partition_type, [])

    @property
    def tbl_features(self):
        return {
            col: feature["feature"]
            for col, feature in self.tbl_profile["features"].items()
        }

    # [x] Migrate to modern style
    def validate_tbl_with_flag(self, flag: Optional[Union[str, bool]]) -> bool:
        try:
            return must_bool(flag, force_raise=True)
        except ValueError:
            return any(
                self.tbl_name.startswith(params.map_tbl_flag.get(_))
                for _ in flag
            )

    # [x] Migrate to modern style
    def validate_tbl_columns(
        self, columns: Union[list, dict], raise_error: bool = False
    ) -> Optional[list]:
        _filter: list = [_col for _col in columns if _col in self.tbl_features]
        if len(_filter) != len(columns) and raise_error:
            _filter_out: set = set(columns).difference(set(_filter))
            raise TableValidatorError(
                f"Column validate does not exists in {self.tbl_name} from {list(_filter_out)}"
            )
        return (
            _filter
            if isinstance(columns, list)
            else {
                _col: _type
                for _col, _type in columns.items()
                if _col in _filter
            }
        )

    # [x] Migrate to modern style ``Profile.columns``
    def get_tbl_columns(
        self,
        pk_included: bool = False,
        datatype_included: bool = False,
    ) -> Union[list, dict]:
        if datatype_included:
            return (
                self.tbl_profile["features"]
                if pk_included
                else {
                    col: details
                    for col, details in self.tbl_profile["features"].items()
                    if col not in self.tbl_primary_key
                }
            )
        return (
            list(self.tbl_profile["features"])
            if pk_included
            else list(
                filter(
                    lambda x: x not in self.tbl_primary_key,
                    self.tbl_profile["features"],
                )
            )
        )

    # [x] Migrate to modern style
    def get_tbl_dependency(self) -> dict[str, dict[int, tuple[str]]]:
        _result: dict = {}
        for ps, attrs in sorted(
            self.tbl_process.items(),
            key=lambda x: x[1]["priority"],
            reverse=False,
        ):
            stm: Statement = Statement(attrs["statement"])
            _result[ps] = stm.mapping()
        return _result

    # [x] Migrate to modern style
    def get_tbl_stm_drop(self, cascade: bool = False) -> str:
        _cascade: str = "cascade" if cascade else ""
        return reduce_stm(
            params.bs_stm.drop.tbl.format(
                table_name=self.tbl_name, cascade=_cascade
            )
        )

    # [x] Migrate to modern style
    def get_tbl_stm_create(self) -> str:

        foreign: dict = self.tbl_foreign_key

        def _get_foreign_stm(column: str) -> str:
            return (
                f" references {{ai_schema_name}}.{foreign[column]}"
                if column in foreign
                else ""
            )

        features: str = ", ".join(
            [
                f"{k} {v} {_get_foreign_stm(k)}"
                for k, v in self.tbl_features.items()
            ]
        )
        primary: str = (
            f', primary key ( {", ".join(prim)} )'
            if (prim := self.tbl_primary_key)
            else ""
        )
        if self.tbl_partition_type:
            partition: str = " {partition_type}( {partition_cols} )".format(
                partition_type=" ".join(self.tbl_partition_type.split("_")),
                partition_cols=", ".join(self.tbl_partition),
            )
        else:
            partition: str = ""
        _stm: str = (
            "create table if not exists "
            "{{database_name}}.{{ai_schema_name}}.{table_name}"
            "( {features} {primary_key} ){partition}"
        )
        return reduce_stm(
            _stm.format(
                table_name=self.tbl_name,
                features=features,
                primary_key=primary,
                partition=partition,
            )
        )

    # [x] Migrate to modern style
    def get_tbl_stm_create_bk(self, tbl_name_bk: Optional[str] = None) -> str:
        _tbl_name_bk: str = tbl_name_bk or f"{self.tbl_name}_bk"
        return re.sub(
            r"{database_name}\.{ai_schema_name}\.\w+",
            f"{{database_name}}.{{ai_schema_name_backup}}.{_tbl_name_bk}",
            self.get_tbl_stm_create(),
        )

    # [x] Migrate to modern style
    def get_tbl_stm_create_partition(self, start_period, end_period) -> str:
        """
        ref: https://www.enterprisedb.com/postgres-tutorials/how-use-table-partitioning-scale-postgresql
        ref: https://www.postgresql.fastware.com/postgresql-insider-prt-ove
        """
        if self.tbl_partition_type == "partition_by_range":
            _tbl_name_partition = f"{{table_name}}_{start_period}_{end_period}"
            _stm: str = (
                """create table if not exists 
                {{database_name}}.{{ai_schema_name}}.{table_name_partition}
                partition of {{table_name}} for values from ('{start_period}') 
                to ('{end_period}')"""
            )
            return reduce_stm(
                _stm.format(
                    table_name_partition=_tbl_name_partition,
                    start_period=start_period,
                    end_period=end_period,
                )
            )
        raise TableNotImplement(
            f"AI framework does not support create partition table "
            f"with type `{self.tbl_partition_type}`"
        )

    # [x] Migrate to modern style
    def get_tbl_stm_ingest(self) -> str:
        """Generate insert statement for receive data from values string
        :statement:

        INSERT INTO DATABASE.SCHEMA.TABLE_NAME AS TN
        (
            COLUMN1, COLUMN2, ...
        )
        VALUES ('01', '02', ... )
        ON CONFLICT ( PRIMARY KEY ) DO UPDATE
            SET COLUMN1 = EXCLUDED.COLUMN1
            ,   COLUMN1 = EXCLUDED.COLUMN1
            ,   ...
        WHERE   TN.UPDATE <= EXCLUDED.UPDATE
        """
        conflict: str = ""
        if primary := ", ".join(self.tbl_primary_key):
            conflict: str = (
                """ on conflict ( {primary_key} ) do update set {set_column_pairs}
                where {table_name_short}.update_date <= excluded.update_date""".format(
                    primary_key=primary,
                    set_column_pairs=self.get_tbl_conflict_set(),
                    table_name_short=self.tbl_name_sht,
                )
            )

        _stm: str = """insert into {{database_name}}.{{ai_schema_name}}.{table_name} as {table_name_short}
            ( {{string_columns}} ) values  {{string_values}}{conflict}"""

        return reduce_stm(
            _stm.format(
                table_name=self.tbl_name,
                table_name_short=self.tbl_name_sht,
                conflict=conflict,
            ),
            add_row_number=False,
        )

    # [x] Migrate to modern style
    def get_tbl_stm_update(self) -> str:
        """Generate update statement for receive data from values string
        :statement:

        UPDATE DATABASE.SCHEMA.TABLE_NAME AS TN
            SET COLUMN1 = TN_UD.COLUMN1
            ,   COLUMN1 = TN_UD.COLUMN1
            ,   ...
        FROM ( VALUES ('01', '02', ... ), ... ) AS TN_UD( COLUMN1, COLUMN2, ... )
        WHERE TN.PRIMARY_KEY = TN_UD.PRIMARY_KEY
        """
        primary_key_columns = self.get_tbl_conflict_set(
            included=self.tbl_primary_key,
            word_self=self.tbl_name_sht,
            word_target=f"{self.tbl_name_sht}_ud",
            sep="and",
            cast_type=True,
        )
        _stm: str = """update {{database_name}}.{{ai_schema_name}}.{table_name}
            as {table_name_short}
            set {{string_columns_pairs}} from ( values {{string_values}} )
            as {table_name_short}_ud( {{string_columns}} )
            where {primary_key_columns}
        """
        return reduce_stm(
            _stm.format(
                table_name=self.tbl_name,
                table_name_short=self.tbl_name_sht,
                primary_key_columns=primary_key_columns,
            ),
            add_row_number=False,
        )

    # [x] Migrate to modern style
    def get_tbl_conflict_set(
        self,
        excluded: Optional[list] = None,
        included: Optional[list] = None,
        word_self: Optional[str] = None,
        word_target: Optional[str] = None,
        sep: Optional[str] = None,
        cast_type: bool = False,
    ):
        """Generate set statement for ingestion component in update and ingest
        mode.

        :ingest:
                SET
                COLUMN = EXCLUDED.COLUMN,
                COLUMN = EXCLUDED.COLUMN,
                ...

        :update:
                WHERE
                TABLE.COLUMN = TABLE_UD.COLUMN AND
                TABLE.COLUMN = TABLE_UD.COLUMN AND
                ...
        """
        _excluded: list = self.validate_tbl_columns(excluded or [])
        _included: list = self.validate_tbl_columns(
            included or list(self.get_tbl_columns(pk_included=False))
        )
        word_self: str = f"{word_self}." if word_self else ""
        word_target: str = word_target or "excluded"
        sep: str = sep or ","
        return f" {sep} ".join(
            [
                (
                    f"{word_self}{col} = {word_target}.{col}"
                    + (f"::{attrs['datatype']}" if cast_type else "")
                )
                for col, attrs in self.get_tbl_columns(
                    pk_included=True, datatype_included=True
                ).items()
                if (
                    (filter_sys_auto(attrs["feature"]) or included)
                    and col not in _excluded
                    and col in _included
                )
            ]
        )

    # [x] Migrate to modern style
    def get_tbl_catalog(self, config: Optional[dict] = None) -> dict:
        """Get merge configuration from any properties of table
        :structure:
            <catalog_name:`{prefix}_{body_...}`>:
                version: <iso-format: [`%Y-%m-%d`, `%Y-%m-%d %H:%M:%S`]>
                description: ""

                <property/?>:
                    system: ""
                    type: ""
                    run_type: ""
                    run_limit: 0
                    retention:
                        value: 0
                        columns: []

                <create/profile>:
                    <features>:
                        <column>: <string>
                        ...
                    <primary_key>: []
                    <partition_by_...>: []

                <initial/mockup>:
                    parameter: []

                    values:
                        - ""
                          ...

                    statement: ""

                <update/process>:

                    <process_name>:
                        type: "sql"
                        priority: 1
                        parameter: []

                        statement: ""

                        statements:
                            action_name: ""
                            with_<table_name>: ""
                            row_table: ""

                    <process_name>:
                        type: "func"
                        priority: 2
                        parameter: []
                        load:
                            parameter: []
                            statement: ""
                        save:
                            parameter: []
                            statement: ""
        """
        _config: dict = config or get_config(
            config_name=self.tbl_name,
            config_prefix=self.tbl_prefix,
            folder_config="catalog",
            config_prefix_file="catalog",
        )
        _config_keys: list = list(_config)
        if not (
            _create_key := only_one(
                _config_keys, params.map_tbl.profile, default=False
            )
        ):
            raise CatalogNotFound(
                f"Catalog does not found any key represent "
                f"`create`/`profile` of {self.tbl_name!r}"
            )
        elif not (
            _process_key := only_one(
                _config_keys, params.map_tbl.process, default=False
            )
        ):
            if self.tbl_prefix in params.list_tbl_prefix_must_have_process:
                raise CatalogNotFound(
                    f"Catalog does not found any key represent "
                    f"`update`/`process`/`function` of {self.tbl_name!r}"
                )

        self.tbl_catalog: dict = {
            "description": _config.get(
                only_one(_config_keys, params.map_tbl.desc), ""
            ),
            "profile": self._generate_profile(_config.pop(_create_key)),
            "process": self._generate_process(_config.pop(_process_key, {})),
        }

        if _initial_key := only_one(
            _config_keys, params.map_tbl.initial, default=False
        ):
            self.tbl_catalog["initial"] = self._generate_initial(
                _config.pop(_initial_key)
            )
        return self.tbl_catalog

    def _generate_property(
        self,
        properties: dict,
    ):
        """Generate property from configuration to standard mapping."""
        pass

    # [x] Migrate to modern style
    def _generate_profile(
        self,
        profiles: dict,
        excluded: Optional[list] = None,
    ) -> dict:
        """Generate profile from configuration to standard mapping."""
        _excluded: list = excluded or []
        _columns: dict = self._loop_profile(profiles, _excluded)
        _prim_key: list = profiles.get(
            only_one(list(profiles), params.map_tbl.pk, default=False),
            [],
        )
        _fore_key: dict = profiles.get(
            only_one(list(profiles), params.map_tbl.fk, default=False),
            {},
        )

        # Validate columns in primary key and foreign key list
        if _catch_cols := [f"{_!r}" for _ in _prim_key if _ not in _columns]:
            raise CatalogArgumentError(
                f"Primary key {', '.join(_catch_cols)} does not exists "
                f"in features of '{self.tbl_name}'"
            )
        elif _catch_cols := [f"{_!r}" for _ in _fore_key if _ not in _columns]:
            raise CatalogArgumentError(
                f"Foreign key {', '.join(_catch_cols)} does not exists "
                f"in features of '{self.tbl_name}'"
            )

        profiles["features"] = _columns
        return profiles

    # [x] Migrate to modern style
    @staticmethod
    def _loop_profile(profiles: dict, excluded: list):
        """Loop Filter for data in profile key."""
        _columns: dict = {}

        for index, _feature in enumerate(
            profiles.pop("features", {}).items(), start=1
        ):
            _column: str = _feature[0]
            _properties: Union[str, dict] = _feature[1]

            # Skip column name which exists in excluded list
            if _column in excluded:
                continue

            if isinstance(_properties, str):
                _datatype, _nullable = split_datatype(_properties)

                _columns[_column]: dict = {
                    "order": index,
                    "nullable": False,
                    "feature": _properties,
                }

                if re.search("unique", _datatype):
                    _datatype: str = " ".join(
                        _datatype.replace("unique", "").split()
                    )

                if re.search("serial", _datatype):
                    _columns[_column]["datatype"] = _datatype.replace(
                        "serial", "int"
                    )

                elif re.search("default", _datatype):
                    _columns[_column]["datatype"] = _datatype.split("default")[
                        0
                    ].strip()

                else:
                    _columns[_column]["datatype"] = _datatype
                    _columns[_column]["nullable"] = not re.search(
                        "not null", _nullable
                    )
            else:
                _columns[_column]: dict = {
                    "order": index,
                    "datatype": _properties["type"],
                    "nullable": _properties.get("nullable", True),
                    "feature": _properties,
                }

        return _columns

    # [x] Migrate to modern style
    def _generate_process(self, processes: dict):
        """Generate processes from configuration to standard mapping."""
        _processes: dict = {}
        _ps_count: int = 0
        for ps_name, ps_details in sorted(
            processes.items(),
            key=lambda x: x[1].get("priority", 99),
            reverse=False,
        ):
            if (
                ps_type := ps_details.get("type", self.tbl_type)
            ) not in params.list_tbl_types:
                raise TableNotImplement(
                    f"AI framework does not support for process type "
                    f"{ps_type!r} that set in {self.tbl_name!r}"
                )
            _processes[ps_name] = {
                "parameter": list(
                    set(
                        ps_details.get(
                            only_one(list(ps_details), params.map_tbl.param),
                            [],
                        )
                    )
                ),
                "priority": _ps_count + 1,
            }
            if ps_type == "sql":
                _ps_stm: Union[str, dict] = ps_details.get(
                    only_one(list(ps_details), params.map_tbl.stm), ""
                )
                _processes[ps_name]["statement"] = Statement(_ps_stm).generate()
            elif ps_type == "py":
                if not (
                    ps_func_key := only_one(
                        list(ps_details), params.map_tbl.func, default=False
                    )
                ):
                    raise CatalogArgumentError(
                        f"Function does not set in {self.tbl_name!r} while process {ps_name!r} is {ps_type!r} type"
                    )
                ps_function: callable = get_function(ps_details[ps_func_key])
                _processes[ps_name]["function"] = ps_function

                for stage in {"load", "save"}:
                    sub_stage_details: dict = ps_details.get(stage, {})
                    _stages: dict = {
                        "parameter": list(
                            set(
                                sub_stage_details.get(
                                    only_one(
                                        list(sub_stage_details),
                                        params.map_tbl.param,
                                    ),
                                    [],
                                )
                            )
                        ),
                    }
                    sub_stage_stm: Union[str, dict] = sub_stage_details.get(
                        only_one(list(sub_stage_details), params.map_tbl.stm),
                        "",
                    )
                    _stages["statement"] = Statement(sub_stage_stm).generate()
                    _processes[ps_name][stage] = _stages
            else:
                raise CatalogArgumentError(
                    f"Process type {ps_type!r} which set in {self.tbl_name!r} "
                    f"does not support yet"
                )
            _ps_count += 1
        self.tbl_process_count: int = _ps_count
        return _processes

    # [x] Migrate to modern style
    def _generate_initial(self, initial: dict) -> dict:
        """Generate initial from configuration to standard mapping
        :structure:
            <initial/mockup>:
                parameter: []

                (i)     value: ""

                (ii)    values:
                            - ""
                            - ""
                            ...

                (iii)   statement: ""

                (iv)    statements:
                            with_<tbl_name>: ""
                            row_table: ""

                (v)     file: ""

                (vi)    files:
                            - ""
                            - ""
                            ...

        :warning:
            - Initial statement generator need primary key values in profile
        """
        _initial: dict = {
            "parameter": list(
                set(
                    initial.get(
                        only_one(list(initial), params.map_tbl.param),
                        [],
                    )
                )
            ),
        }
        if not (
            _value_key := only_one(
                list(initial),
                ["value", "values", "file", "files"],
                default=False,
            )
        ):
            _initial["statement"]: str = Statement(
                initial.get(only_one(list(initial), params.map_tbl.stm), "")
            ).generate()
            return _initial

        conflict: str = (
            f" on conflict ( {primary} ) do nothing"
            if (primary := ", ".join(self.tbl_primary_key))
            else ""
        )
        _columns: list = [
            _col
            for _col, datatype in self.tbl_features.items()
            if all(_ not in datatype for _ in {"default", "serial"})
        ]

        if _value_key in {"file", "files"}:
            _values = load_json_to_values(initial[_value_key], schema=_columns)
        else:
            _values = initial[_value_key]

        initial_value: str = (
            ", ".join([f"({_value})" for _value in _values])
            if isinstance(_values, list)
            else f"({_values})"
        )

        _stm: str = f"""insert into {{database_name}}.{{ai_schema_name}}.{self.tbl_name} as {self.tbl_name_sht}
            ( {','.join(_columns)} ) values {initial_value}{conflict}"""

        _initial["statement"]: str = reduce_stm(_stm, add_row_number=False)
        return _initial


class FuncCatalog:
    """Function Catalog for generate all configuration from .yaml file."""

    __slots__ = (
        "func_name",
        "func_type",
        "func_name_sht",
        "func_prefix",
        "func_catalog",
    )

    def __init__(
        self,
        func_name: str,
        func_type: Optional[str] = None,
        func_catalog: Optional[dict] = None,
    ):
        self.func_name: str = func_name
        self.func_type: str = func_type or params.list_func_types[0]
        self.func_name_sht: str = "".join(
            [word[0] for word in self.func_name.split("_")]
        )
        self.func_prefix: str = self.func_name.split("_")[0]
        self.func_catalog: dict = self.get_func_catalog(config=func_catalog)
        if self.func_prefix not in params.list_func_priority:
            logger.warning(
                f"The config {self.func_name!r} have table prefix name, `{self.func_prefix}`, "
                f"which different from the agreed list"
            )

    def __repr__(self):
        return f"{self.__class__.__name__}({self.func_name!r}, func_type={self.func_type!r})"

    def __str__(self):
        return f"{self.func_type}:{self.func_name}"

    # [x] Migrate to modern style
    @property
    def catalog(self) -> dict:
        return {
            "id": self.func_name_sht,
            "name": self.func_name,
            "type": self.func_type,
            "prefix": self.func_prefix,
            **self.func_catalog,
        }

    @property
    def func_profile(self) -> dict:
        return self.func_catalog["profile"]

    def get_func_stm_drop(self, cascade: bool = False) -> str:
        _cascade: str = "cascade" if cascade else ""
        return reduce_stm(
            params.bs_stm.drop.get(self.func_type, "func").format(
                func_name=self.func_name, cascade=_cascade
            )
        )

    # [x] Migrate to modern style
    def get_func_stm_create(self) -> str:
        return self.func_profile["statement"]

    # [x] Migrate to modern style
    def get_func_catalog(self, config: Optional[dict] = None):
        """Get merge configuration from any properties of function :structure:

        <catalog_name: `{body_...}`>:
            version: <iso-format: [`%Y-%m-%d`, `%Y-%m-%d %H:%M:%S`]>
            description: ""
            <create/statement/function>: ""
        """
        _config: dict = config or get_config(
            config_name=self.func_name,
            config_prefix="",
            folder_config=params.map_func_types.get(self.func_type, "function"),
            config_prefix_file=self.func_type,
        )
        _config_keys: list = list(_config)
        if not (
            _create_key := only_one(
                _config_keys, params.map_func.create, default=False
            )
        ):
            raise CatalogNotFound(
                f"Catalog does not found any key represent `create`/`function`/`statement` of {self.func_name!r}"
            )
        self.func_catalog: dict = {
            "description": _config.get(
                only_one(_config_keys, params.map_func.desc), ""
            ),
            "profile": self._generate_profile(_config.pop(_create_key)),
        }
        return self.func_catalog

    # [x] Migrate to modern style
    def _generate_profile(self, profiles: Union[dict, str]):
        if isinstance(profiles, str):
            return {
                "statement": Statement(profiles).generate(),
                "parameter": [],
            }
        elif isinstance(profiles, dict):
            _profiles: dict = {
                "parameter": list(
                    set(
                        profiles.get(
                            only_one(list(profiles), params.map_func.param), []
                        )
                    )
                ),
            }
            if not (_stm_key := only_one(list(profiles), params.map_func.stm)):
                raise CatalogArgumentError(
                    f"Function profile of {self.func_name!r} does not "
                    f"set statement while profile key was dict type"
                )
            _profiles["statement"] = Statement(
                profiles.get(_stm_key, "")
            ).generate()
            return _profiles
        raise CatalogArgumentError(
            f"AI framework function does not support for create "
            f"type {type(profiles)!r} that set in {self.func_name!r}"
        )


class PipeCatalog:
    """Pipeline Catalog for generate all configuration from .yaml file."""

    __slots__ = (
        "pipe_name",
        "pipe_catalog",
        "pipe_nodes_count",
        "pipe_ta",
        "pipe_tr",
        "verbose",
    )

    def __init__(
        self,
        pipe_name: str,
        *,
        pipe_catalog: Optional[dict] = None,
        pipe_trigger_and: str = "&",
        pipe_trigger_or: str = "|",
        verbose: bool = False,
    ):
        self.pipe_name: str = pipe_name
        self.pipe_ta: str = pipe_trigger_and
        self.pipe_tr: str = pipe_trigger_or
        self.verbose: bool = verbose
        verbose_log(
            self,
            f"[Start] initialize the pipeline catalog object name {self.pipe_name} ...",
        )
        self.pipe_catalog: dict = self.get_pipe_catalog(config=pipe_catalog)
        if any(_ == self.pipe_id for _ in self.pipe_trigger):
            raise CatalogArgumentError(
                f"Pipeline: {self.pipe_id!r} does not support "
                f"for `id` was included in `trigger` lists"
            )
        verbose_log(
            self,
            f"[Success] initialize the pipeline catalog object "
            f"name {self.pipe_name!r}",
            end="=",
        )

    def __repr__(self):
        return f"{self.__class__.__name__}({self.pipe_name!r})"

    def __str__(self):
        return self.pipe_name

    # [x] Migrate to modern style
    @property
    def catalog(self) -> dict:
        return {
            "name": self.pipe_name,
            "type": "pipe",
            "prefix": "pipe",
            **self.pipe_catalog,
        }

    @property
    def pipe_id(self) -> str:
        return self.pipe_catalog["id"]

    @property
    def pipe_run_option(self) -> str:
        return self.pipe_catalog.get("run_option", "slow")

    @property
    def pipe_alert(self) -> list:
        return self.pipe_catalog["alert"]

    @property
    def pipe_alert_inc(self):
        _alert: list = self.pipe_alert.copy()
        _alert.insert(0, self.pipe_id)
        return _alert

    @property
    def pipe_schedule(self) -> list:
        return self.pipe_catalog["schedule"]

    @property
    def pipe_trigger(self) -> list:
        return self.pipe_catalog["trigger"]

    @property
    def pipe_schedule_type(self) -> str:
        _result: str = ""
        if self.pipe_catalog["trigger"]:
            _result += "trigger"
        if self.pipe_catalog["schedule"]:
            _result += "|schedule" if _result else "schedule"
        return _result

    @property
    def pipe_nodes(self) -> dict:
        return self.pipe_catalog["nodes"]

    def get_pipe_catalog(self, config: Optional[dict] = None) -> dict:
        """Get merge configuration from any properties of pipeline :structure:

        <catalog_name: `pipeline_{body}`>:
            version: <iso-format: [`%Y-%m-%d`, `%Y-%m-%d %H:%M:%S`]>
            description: ""
            id: ""

            run_option: ""
            trigger: [""]
            schedule: [""]

            nodes:
                <node_name>: <node_type>
        """
        verbose_log(
            self,
            "[Start] loading configuration data from `./conf/catalog` path ...",
            lvl=1,
        )
        _config: dict = config or get_config(
            config_name=self.pipe_name,
            config_prefix="",
            folder_config="pipeline",
            config_prefix_file="pipeline",
        )
        _config_keys: list = list(_config)
        if not (
            _pipe_id := only_one(
                _config_keys, params.map_pipe.id, default=False
            )
        ):
            raise CatalogNotFound(
                f"Catalog does not found any key represent `id`/`pipeline_id` of {self.pipe_name!r}"
            )
        if not (
            _node_key := only_one(
                _config_keys, params.map_pipe.nodes, default=False
            )
        ):
            raise CatalogNotFound(
                f"Catalog does not found any key represent `nodes`/`tables` of {self.pipe_name!r}"
            )
        self.pipe_catalog: dict = {
            "description": _config.get(
                only_one(_config_keys, params.map_pipe.desc), ""
            ),
            "id": _config.pop(_pipe_id),
            "schedule": must_list(
                _config.get(
                    only_one(_config_keys, params.map_pipe.schedule), []
                )
            ),
            "trigger": self._generate_trigger(
                _config.get(only_one(_config_keys, params.map_pipe.trigger), [])
            ),
            "alert": must_list(
                _config.get(only_one(_config_keys, params.map_pipe.alert), [])
            ),
            "nodes": self._generate_nodes(_config.pop(_node_key)),
        }
        verbose_log(
            self,
            "[Success] Mapping the loading configuration data to `pipe_catalog`",
            lvl=1,
            end="-",
        )
        return self.pipe_catalog

    # [x] Migrate to modern style
    def __generate_condition(
        self, trigger_lists: Union[list, Iterator[str]]
    ) -> Union[list, set]:
        verbose_log(self, "Generate trigger condition ...", lvl=1)

        def convert_element_with_type(mapping: dict) -> Union[list, set]:
            return mapping["type"](mapping["element"])

        _default: list = [{"element": [], "type": list}]
        _default_index = 0
        for trigger in trigger_lists:

            if trigger == "&":
                _default[_default_index]["type"] = list
                continue
            elif trigger == "|":
                _default[_default_index]["type"] = set
                continue

            if trigger.startswith("("):
                _default_index += 1
                _default.append({"element": [], "type": list})
            elif trigger.endswith(")"):
                if _default_index == 0:
                    raise CatalogArgumentError(
                        f"trigger property in pipeline {self.pipe_name!r} "
                        f"does not valid with bracket of logical condition "
                        f"with ')'"
                    )
                _convert_result = convert_element_with_type(
                    _default.pop(_default_index)
                )
                _default_index -= 1
                _default[_default_index]["element"].append(_convert_result)
            else:
                _default[_default_index]["element"].append(trigger)
        if len(_default) > 1:
            raise CatalogArgumentError(
                f"trigger property in pipeline {self.pipe_name!r} "
                f"does not valid with bracket of logical condition with '('"
            )
        return convert_element_with_type(_default[0])

    # [x] Migrate to modern style
    def _generate_trigger(self, triggers: Union[str, list]):
        """:structure: (i)     trigger: [ 'pipeline-id-01', ... ]

        (ii)    trigger: 'pipeline-id-01 & (pipeline-id-02 | pipeline-
        id-03)'
        """
        verbose_log(
            self,
            f"[Start] Generate trigger value for pipeline name {self.pipe_name!r}",
            lvl=1,
        )

        def __prepare_trigger(trigger: str) -> list:
            _trigger: str = "".join(trigger.strip().split())
            for _ in re.findall(r"(\([A-Za-z0-9_&|]+\))", _trigger):
                if self.pipe_ta not in _ and self.pipe_tr not in _:
                    _trigger = _trigger.replace(_, _.strip("()"))
            _trigger: str = _trigger.replace("(", "( ").replace(")", " )")
            return [
                x.strip()
                for x in (
                    _trigger.replace(self.pipe_ta, f" {self.pipe_ta} ")
                    .replace(self.pipe_tr, f" {self.pipe_tr} ")
                    .split()
                )
            ]

        if isinstance(triggers, list):
            return triggers
        _trigger_split: list = __prepare_trigger(triggers)
        if any(
            _ not in {"&", "|"}
            for _ in list(
                filter(lambda x: x not in {"(", ")"}, _trigger_split)
            )[1::2]
        ):
            raise CatalogArgumentError(
                f"trigger property in pipeline {self.pipe_name!r} "
                f"does not valid with logical condition"
            )
        elif _trigger_split.count("(") != _trigger_split.count(")"):
            raise CatalogArgumentError(
                f"trigger property in pipeline {self.pipe_name!r} "
                f"does not valid with bracket of logical condition"
            )
        return self.__generate_condition(_trigger_split)

    # [x] Migrate to modern style
    def _generate_nodes(self, nodes: Union[dict, list]):
        """Generate node from configuration catalog :structure:

        (i)     <node_name>:
                    priority: 1
                    type: "<node_type>"
                    choose: ['choose_process', ...]

        (i.a)   <node_name_full: `node_type:node_name`>:
                    priority: 1
                    choose: ['choose_process', ...]

        (ii)    <node_name_full: `node_type:node_name`>: ['choose_process', ...]

        (iii)   - name: <node_name_full: `node_type:node_name`>
                  choose: []
                - name: <node_name_full: `node_type:node_name`>
                ...

        (iv)    - "<node_name_full: `node_type:node_name`>"
                - "<node_name_full: `node_type:node_name`>"
                ...
        """
        verbose_log(
            self,
            f"[Start] Generate nodes for pipeline name {self.pipe_name!r}",
            lvl=1,
        )
        _nodes: dict = {}
        if isinstance(nodes, list):
            for _default_priority, _node_props in enumerate(nodes, start=1):
                _node_name, _priority, _node_choose = (
                    self.__generate_node_props(_default_priority, _node_props)
                )
                try:
                    verbose_log(
                        self,
                        f"check node name {_node_name!r} exists in config catalog",
                        lvl=2,
                    )
                    TblCatalog.loads(_node_name)
                except CatalogNotFound as error:
                    filter_error: str = str(error).replace('"', "")
                    raise CatalogNotFound(
                        f"From catalog_name: {self.pipe_name}, {filter_error}"
                    ) from error
                _nodes[_priority] = {
                    "name": _node_name,
                    "choose": _node_choose,
                }
        elif isinstance(nodes, dict):
            _default_priority: float = 1
            for node_name, node_props in sorted(
                nodes.items(),
                key=lambda x: x[1].get("priority", 99),
                reverse=False,
            ):
                _node_name, _priority, _node_choose = (
                    self.__generate_node_props(
                        _default_priority, node_props, node_name
                    )
                )
                try:
                    verbose_log(
                        self,
                        f"check node name {_node_name!r} exists in config catalog",
                        lvl=2,
                    )
                    TblCatalog.loads(_node_name)
                except CatalogNotFound as error:
                    filter_error: str = str(error).replace('"', "")
                    raise CatalogNotFound(
                        f"From catalog_name: {self.pipe_name}, {filter_error}"
                    ) from error
                _nodes[_priority] = {
                    "name": _node_name,
                    "choose": _node_choose,
                }
                _default_priority: float = _priority + 0.1
        self.pipe_nodes_count: int = len(_nodes)
        verbose_log(
            self,
            (
                f"[Success] Generate {self.pipe_nodes_count} nodes "
                f"for pipeline name {self.pipe_name!r}"
            ),
            end="-",
            lvl=1,
        )
        return _nodes

    # [x] Migrate to modern style
    def __generate_node_props(
        self,
        priority: float,
        node_props: Union[list, dict],
        node_name: Optional[str] = None,
    ):
        """Generate node properties with different type of node input
        argument."""
        if isinstance(node_props, list):
            _priority: float = round(priority, 2)
            _node_choose: list = node_props
            _node_name: str = node_name
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
            _node_name: str = (
                f"{_node_type}:{node_name}"
                if (
                    _node_type := node_props.get(_type) and ":" not in node_name
                )
                else node_name
            )
            _node_choose: Optional[list] = node_props.get(_choose, [])
        elif isinstance(node_props, str):
            _priority = priority
            _node_choose: list = []
            _node_name: str = node_props
        else:
            raise CatalogArgumentError(
                f"node properties does not support for '{type(node_props)}' type "
                f"in catalog_name: {self.pipe_name}"
            )
        return _node_name, _priority, _node_choose
