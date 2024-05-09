# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import fnmatch
import importlib
import operator
import os
import re
from datetime import (
    date,
    datetime,
    timedelta,
)
from typing import (
    Optional,
    Union,
)

import yaml
from dateutil import tz
from dateutil.relativedelta import relativedelta

from .errors import (
    CatalogNotFound,
)
from .utils.config import (
    AI_APP_PATH,
    Params,
)
from .utils.logging_ import logging
from .utils.reusables import (
    hash_string,
    merge_dicts,
    must_list,
)

params = Params(param_name="parameters.yaml")
registers = Params(param_name="registers.yaml")
logger = logging.getLogger(__name__)
CATALOGS: list = ["catalog", "pipeline", "function"]


def sort_by_priority(
    values: Union[list, dict], priority_lists: Optional[list] = None
) -> Union[list, dict]:
    """Sorted list by string prefix priority."""
    priority_dict: dict = {
        k: i for i, k in enumerate(priority_lists or params.list_tbl_priority)
    }

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


def get_run_date(
    date_type: str = "str", fmt: str = "%Y-%m-%d"
) -> Union[str, datetime, date]:
    """Get run_date value from now datetime.

    Examples:
        >>> get_run_date(date_type='datetime')
        datetime.now(tz.gettz("Asia/Bangkok"))
        >>> get_run_date(fmt='%Y/%m/%d')
        '2022/01/01'
    """
    run_date: datetime = datetime.now(tz.gettz("Asia/Bangkok"))
    if date_type == "str":
        return run_date.strftime(fmt)
    return run_date.date() if date_type == "date" else run_date


def get_plural(
    num: int,
    word_change: Optional[str] = None,
    word_start: Optional[str] = None,
) -> str:
    """Get plural word for dynamic `num` number if more than 1 or not.

    Examples:
        >>> get_plural(100)
        's'
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
    run_date: Union[str, date],
    run_type: str,
    *,
    invert: bool = False,
    date_type: str = "str",
    fmt: str = "%Y-%m-%d",
) -> Union[str, date]:
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
    run_date_ts: date = (
        date.fromisoformat(run_date) if isinstance(run_date, str) else run_date
    )

    if run_type == "weekly":
        run_date_convert_ts = (
            run_date_ts - timedelta(run_date_ts.weekday())
            if invert
            else run_date_ts - timedelta(run_date_ts.isoweekday())
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
    data_date: date,
    mode: str,
    run_type: str,
    cal_value: int,
    date_type: str = "str",
    fmt: str = "%Y-%m-%d",
) -> Union[str, date]:
    """Get date with internal calculation logic."""
    if mode not in {
        "add",
        "sub",
    }:
        raise NotImplementedError(
            f"Get calculation datetime does not support for mode: {mode!r}"
        )
    _result: date = getattr(operator, mode)(
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


class LoadCatalog:
    """Loading catalog data object."""

    @classmethod
    def from_shortname(
        cls,
        name: str,
        prefix: Optional[str],
        folder: str,
        prefix_file: str,
    ) -> LoadCatalog:
        return cls(name, prefix, folder, prefix_file, shortname=True)

    def __init__(
        self,
        name: str,
        prefix: Optional[str],
        folder: str,
        prefix_file: str,
        shortname: bool = False,
    ):
        """Main initialization of loading catalog object."""
        self.name: str = name
        self.prefix: str = f"{prefix}_" if prefix else ""
        self.folder: str = folder
        self.prefix_file: str = prefix_file
        self.shortname: bool = shortname
        self.path = os.path.join(AI_APP_PATH, registers.path.conf, folder)

    def filter_catalog(self, data: dict) -> list:
        if _result := data.get(self.name, {}):
            _result["name"] = self.name
            return [_result]
        return []

    def filter_catalog_shortname(self, data: dict) -> list:
        _results: list = []
        for _tbl, _result in data.items():
            if "".join(x[0] for x in _tbl.split("_")) == self.name:
                _result["name"] = _tbl
                _results.append(_result)
        return _results

    @staticmethod
    def sorted(results):
        """Sorting version value method."""
        return sorted(
            results,
            key=lambda x: datetime.fromisoformat(
                x.get("version", "1990-01-01")
            ),
            reverse=True,
        )

    def load(self):
        _results: list = []
        for file in sorted(os.listdir(self.path), reverse=False):
            if _get_config_filter_path(
                file, self.folder, self.prefix, self.prefix_file
            ):
                with open(os.path.join(self.path, file), encoding="utf8") as f:
                    _config_data: dict = yaml.load(f, Loader=yaml.Loader)
                    _result: list = (
                        self.filter_catalog_shortname(data=_config_data)
                        if self.shortname
                        else self.filter_catalog(data=_config_data)
                    )
                    if _result:
                        _results.extend(_result)
                    del _config_data
        if _results:
            return self.sorted(_results)[0]
        raise CatalogNotFound(
            f"Catalog {'shortname' if self.shortname else 'name'}: "
            f"{self.name!r} not found in "
            f"`./conf/{self.folder}/{self.prefix_file}_{self.prefix}*.yaml`"
        )


def get_catalogs(
    config_form: Optional[Union[str, list]] = None,
    key_exists: Optional[Union[str, list]] = None,
    key_exists_all_mode: bool = True,
    priority_sorted: bool = False,
) -> dict:
    """Get all raw configuration from .yaml file."""
    _key_exists: list = must_list(key_exists)
    _folder_config: list = must_list(config_form or CATALOGS)
    conf_paths = (
        (AI_APP_PATH / registers.path.conf / x, x) for x in _folder_config
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


def filter_not_null(datatype: str) -> bool:
    return all(not re.search(word, datatype) for word in ["default", "serial"])
