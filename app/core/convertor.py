# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import datetime as dt
import re
from itertools import compress
from typing import Any, Optional, Union

from .errors import (
    ColumnsNotEqualError,
    DuplicateColumnError,
    NullableColumnError,
    OuterColumnError,
    PrimaryKeyNotExists,
    SQLInjection,
    TableArgumentError,
)


def reduce_stm(stm: str, add_row_number: bool = False) -> str:
    """Reduce statement and prepare statement if it wants to catch number of
    result."""
    _reduce_stm: str = " ".join(stm.replace("\t", " ").split()).strip()
    if add_row_number:
        _split_stm: list = _reduce_stm.split(";")
        _last_stm: str = _split_stm.pop(-1)
        if "select count(*) as row_number " not in _last_stm:
            _last_stm: str = (
                f"with row_table as ({_last_stm} returning 1 ) "
                f"select count(*) as row_number from row_table"
            )
        _split_stm.append(_last_stm)
        return "; ".join(_split_stm)
    return _reduce_stm


class Statement:
    """Generate statement from value of key `statement` of `statements`
    :structure:

        (i)     statement: "
                    insert into ...
                    "

        (ii)    statements:
                    with_<table-alias-name>: "
                        select ...
                        "
                    with_row_table: "
                        insert into ...
                        "
                    update_flag: "
                        update ...
                        "

        If statement input has string type, it will convert to dictionary type,
    {'common_query': <stm-string>}.

    :warning:

        If you set some statement after with_row_table, the result of row will
    be that set statement.
    """

    target_list: tuple = ("insert into", "update", "delete from")
    source_list: tuple = (
        "from",
        "join",
        "left join",
        "right join",
        "cross join",
        "full join",
    )

    __slots__ = (
        "stm_statement",
        "stm_with_count",
        "stm_add_row_num",
        "stm_result",
        "stm_generate_flg",
    )

    def __init__(
        self, statement: Union[str, dict], add_row_number: bool = False
    ):
        self.stm_statement: dict = (
            {"common_query": statement}
            if isinstance(statement, str)
            else statement
        )
        self.stm_with_count: int = 0
        self.stm_add_row_num: bool = add_row_number
        self.stm_result: str = ""
        self.stm_generate_flg: bool = False

    def __repr__(self):
        return f"{self.__class__.__name__}(statement={self.stm_statement})"

    def __str__(self):
        return self.generate()

    @property
    def stm_with_prefix(self) -> str:
        return "with" if self.stm_with_count == 1 else ","

    @property
    def stm_type(self) -> str:
        if not self.stm_generate_flg:
            self.generate()
        if "select count(*) as row_number from " in self.stm_result:
            return "dql"
        return self._check_type(self.stm_result)

    @property
    def stm_params(self) -> list[str]:
        if not self.stm_generate_flg:
            self.generate()
        _params_all = re.findall(r"{([^{}]+?)}", self.stm_result)
        return [
            param
            for param in _params_all
            if param
            not in {
                "database_name",
                "ai_schema_name",
            }
        ]

    @staticmethod
    def add_row_num(stm: str, include_with: bool = True) -> str:
        return (
            f"{'with ' if include_with else ''}row_table as "
            f"( {stm} returning 1 ) "
            f"select count(*) from row_table;"
        )

    def _add_row_num(self, stm: str) -> str:
        return (
            f"{self.add_row_num(stm, include_with=False)} "
            if self.stm_add_row_num
            else f"{stm}; "
        )

    def _generate(self) -> str:
        """Generate statement from string type."""
        for _stm_name, _stm_sub_stm in self.stm_statement.items():
            if not isinstance(_stm_sub_stm, str):
                raise TableArgumentError(
                    f"process does not support for type {type(_stm_sub_stm)}"
                )

            _reduce_stm: str = reduce_stm(_stm_sub_stm)
            if _stm_name.startswith("with_"):
                self.stm_with_count += 1
                tbl_alias_stm = (
                    self._add_row_num(_reduce_stm)
                    if (_tbl_alias := "_".join(_stm_name.split("_")[1:]))
                    == "row_table"
                    else f"{_tbl_alias} as ( {_reduce_stm} )"
                )
                if _tbl_alias == "row_table" and not self.stm_add_row_num:
                    self.stm_result += f" {tbl_alias_stm}"
                else:
                    self.stm_result += f"{self.stm_with_prefix} {tbl_alias_stm}"
                continue

            self.stm_with_count: int = 0
            self.stm_result += (
                f"{_reduce_stm}{'' if _reduce_stm.endswith(';') else ';'} "
            )
        self.stm_generate_flg: bool = True
        return self.stm_result

    def generate(self) -> str:
        return self.stm_result if self.stm_generate_flg else self._generate()

    def target(self) -> set:
        return set(
            re.findall(
                (
                    r"(insert into|update|delete from) "
                    r"{database_name}\.{ai_schema_name}\.(\w+)"
                ),
                self.generate(),
            )
        )

    def source(self) -> set:
        return set(
            re.findall(
                (
                    r"(from|join|left join|right join|cross join|full join) "
                    r"{database_name}\.{ai_schema_name}\.(\w+)"
                ),
                self.generate(),
            )
        )

    def mapping(self) -> dict[int, tuple[str]]:
        find_list: list = re.findall(
            (
                r"(insert into|update|delete from|from|join|left join"
                r"|right join|cross join|full join) "
                r"{database_name}\.{ai_schema_name}\.(\w+)"
            ),
            self.generate(),
        )
        _mapping: dict = {}
        _target: Optional[str] = None
        _residue: list = []
        for index, match in enumerate(find_list, start=1):
            if match[0] in self.target_list:
                _target = match[1]
                continue

            if not _target:
                _residue.append(match[1])
                continue

            _mapping[index] = (match[1], _target)
            if _residue:
                for i, _source in enumerate(_residue, start=1):
                    _mapping[index + (i * 0.1)] = (_source, _target)
                _residue: list = []
        return dict(enumerate(_mapping.values(), start=1))

    @staticmethod
    def _check_type(statement: str) -> str:
        _statement: str = statement.strip()
        if _statement.startswith("select"):
            # Data Query Language
            return "dql"
        elif _statement.startswith(
            (
                "insert into",
                "update",
                "delete from",
                "merge",
            )
        ):
            # Data Manipulation Language
            return "dml"
        elif _statement.startswith(
            (
                "create",
                "alter",
                "drop",
                "truncate",
                "rename",
            )
        ):
            # Data Definition Language
            return "ddl"
        elif _statement.startswith(
            (
                "grant",
                "revoke",
            )
        ):
            # Data Control Language
            return "dcl"
        return "undefined"


class Value:
    """Generate values from dictionary or value of key `values` :structure:

    (i)     values:
                -   class_a: 0.96
                    class_b: 0.91
                    class_c: 0.86
                -   ...

    (ii)    values:
                dc_code: "8910",
                dc_name: "DC Korat",
                data_merge:
                    -   rdc_code: "8910",
                        rdc_name: "RDC Korat",
                        lead_time_rdc: 7,
                        inventory_cap_value_rdc: 500000
                    -   rdc_code: "8920",
                        rdc_name: "RDC BKK",
                        lead_time_rdc: 10,
                        inventory_cap_value_rdc: 100000
    """

    def __init__(
        self,
        values: Union[dict, list],
        update_date: Union[dt.datetime, str],
        mode: Optional[str] = None,
        action: Optional[str] = None,
        expected_cols: Optional[dict[str, Any]] = None,
        expected_pk: Optional[list] = None,
    ):
        self.vl_values: Union[dict, list] = values
        self.vl_mode: str = mode or "common"  # merge
        self.vl_action: str = action or "insert"  # update
        self.expected_cols: dict[str, Any] = expected_cols or {}
        self.vl_expected_pk: list[str] = expected_pk or []
        self.vl_update_date: str = (
            update_date
            if isinstance(update_date, str)
            else update_date.strftime("%Y-%m-%d %H:%M:%S")
        )

    def generate(self) -> tuple:
        if self.vl_mode == "common":
            return self._generate_common(self.vl_values)
        elif self.vl_mode == "merge":
            return self._generate_merge(self.vl_values)

    @staticmethod
    def validate_col_duplicate(columns: list):
        distinct = set()
        col_duplicates: list = [
            _col for _col in columns if _col in distinct or distinct.add(_col)
        ]
        if len(columns) != len(set(columns)):
            raise DuplicateColumnError(
                f"Data column was duplicated with {col_duplicates}. "
                f"Please check column name in payloads"
            )

    def validate_col_nullable(self, columns: list):
        if self.vl_action != "update" and any(
            not_found := [
                (k not in columns) if v.nullable else False
                for k, v in self.expected_cols.items()
            ]
        ):
            _raise: list = list(
                compress(
                    list(self.expected_cols.keys()),
                    (not x for x in not_found),
                )
            )
            raise NullableColumnError(
                f"column which not null property, "
                f"{str(_raise)}, does not exists in data"
            )

    def validate_col_pk(self, columns: list):
        if self.vl_action != "insert" and any(
            _ not in columns for _ in self.vl_expected_pk
        ):
            raise PrimaryKeyNotExists(
                f"Data column does not contain list of primary key, "
                f"{str(self.vl_expected_pk)}"
            )

    @staticmethod
    def validate_col_sql_inject(values: dict):
        if any(
            (
                ("drop " in value)
                or ("select " in value)
                or ("delete " in value)
                or ("insert " in value)
            )
            for value in values.values()
            if value and isinstance(value, str)
        ):
            raise SQLInjection(
                "data in payloads have sub SQL query like: "
                "`select`, `drop`, `delete`, `insert`"
            )

    def validate_col_outer(self, columns: list):
        if any(col not in self.expected_cols for col in columns):
            outer: list = list(
                set(columns).difference(set(self.expected_cols.keys()))
            )
            raise OuterColumnError(
                f"Data column, {outer}, was outer from configuration. "
                f"Please check column name in payloads"
            )

    def _generate_common(self, values: Union[dict, list]) -> tuple:
        """Ingest with common mode and insert mode.

        CASE I
        ------
        {
            "background": "N",
            "mode": "insert",
            "ingest_mode": "common",
            "update_date": "2021-11-01 02:11:00",
            "data": [
                {
                    "dc_code": "8910",
                    "dc_name": "DC Korat",
                    "rdc_code": "8910",
                    "rdc_name": "RDC Korat",
                    "lead_time_rdc": 7,
                    "inventory_cap_value_rdc": 500000
                },
                {
                    "dc_code": "8910",
                    "dc_name": "DC Korat",
                    "rdc_code": "8920",
                    "rdc_name": "RDC BKK",
                    "lead_time_rdc": 10,
                    "inventory_cap_value_rdc": 100000
                },
                {
                    "dc_code": "8920",
                    "dc_name": "DC BKK",
                    "rdc_code": "8930",
                    "rdc_name": "RDC TARGET",
                    "lead_time_rdc": 5,
                    "inventory_cap_value_rdc": 2000000
                }
            ]
        }
        CASE II
        -------
        {
            "background": "N",
            "mode": "insert",
            "ingest_mode": "merge",
            "update_date": "2021-11-01 02:11:00",
            "data": [
                {
                    "dc_code": "8910",
                    "dc_name": "DC Korat",
                    "rdc_code": "8910",
                    "rdc_name": "RDC Korat",
                    "franchise_code": "8010",
                    "franchise_name": "Shop Korat",
                    "lead_time_fc": 7,
                    "inventory_cap_value_fc": 500000
                },
                {
                    "dc_code": "8910",
                    "dc_name": "DC Korat",
                    "rdc_code": "8920",
                    "rdc_name": "RDC BKK",
                    "franchise_code": "8020",
                    "franchise_name": "Shop BangNa",
                    "lead_time_fc": 3,
                    "inventory_cap_value_fc": 1000000
                },
                {
                    "dc_code": "8910",
                    "dc_name": "DC Korat",
                    "rdc_code": "8920",
                    "rdc_name": "RDC BKK",
                    "franchise_code": "8030",
                    "franchise_name": "Shop Bangkea",
                    "lead_time_fc": 2,
                    "inventory_cap_value_fc": 750000
                },
                {
                    "dc_code": "8920",
                    "dc_name": "DC BKK",
                    "rdc_code": "8930",
                    "rdc_name": "RDC TARGET",
                    "franchise_code": "8041",
                    "franchise_name": "Shop TARGET",
                    "lead_time_fc": 10,
                    "inventory_cap_value_fc": 200000
                },
                {
                    "dc_code": "8920",
                    "dc_name": "DC BKK",
                    "rdc_code": "8930",
                    "rdc_name": "RDC TARGET",
                    "franchise_code": "8042",
                    "franchise_name": "Shop BTV",
                    "lead_time_fc": 12,
                    "inventory_cap_value_fc": 350000
                },
            ]
        }
        """
        _cols_expected: list = [
            k for k, v in self.expected_cols.items() if v.default is None
        ]
        _cols: list = list(values)

        if isinstance(values, list):
            _values_list: list = []
            _col_previous: list = []
            for index, data in enumerate(values, start=1):
                _cols, _values = self._generate_common(values=data)
                if index > 1 and (
                    self.vl_action == "update"
                    and (
                        len(_col_previous) != len(_cols)
                        or any(_ not in _col_previous for _ in _cols)
                    )
                ):
                    raise ColumnsNotEqualError(
                        "Columns in payload does not equal when use 'update' "
                        "mode"
                    )
                _col_previous: list = _cols
                _values_list.append(_values)
            return _cols, ", ".join(_values_list)

        # Check duplicated
        self.validate_col_duplicate(_cols)

        # Check `update_date` exists
        if "update_date" in self.expected_cols and "update_date" not in _cols:
            _cols.append("update_date")
            values["update_date"] = self.vl_update_date

        # Check `not null` exists
        self.validate_col_nullable(_cols)

        # Check Primary Key exists for update
        self.validate_col_pk(_cols)

        # Check SQL injection
        self.validate_col_sql_inject(values)

        result_values: str = (
            self._generate_result_str(_cols, values)
            if self.vl_action == "update"
            else self._generate_result_str(_cols_expected, values)
        )
        result_columns: list = (
            _cols if self.vl_action == "update" else _cols_expected
        )
        return result_columns, result_values

    def _generate_merge(self, values: Union[dict, list]) -> tuple:
        """Ingest with merge mode.

        CASE I
        ------
        {
            "background": "N",
            "ingest_mode": "merge",
            "update_date": "2021-11-01 02:11:00",
            "data": [
                {
                    "dc_code": "8910",
                    "dc_name": "DC Korat",
                    "data_merge": [
                        {
                            "rdc_code": "8910",
                            "rdc_name": "RDC Korat",
                            "lead_time_rdc": 7,
                            "inventory_cap_value_rdc": 500000
                        },
                        {
                            "rdc_code": "8920",
                            "rdc_name": "RDC BKK",
                            "lead_time_rdc": 10,
                            "inventory_cap_value_rdc": 100000
                        }
                    ]
                },
                {
                    "dc_code": "8920",
                    "dc_name": "DC BKK",
                    "data_merge": {
                        "rdc_code": "8930",
                        "rdc_name": "RDC TARGET",
                        "lead_time_rdc": 5,
                        "inventory_cap_value_rdc": 2000000
                    }
                }
            ]
        }
        CASE II
        -------
        {
            "background": "N",
            "ingest_mode": "merge",
            "update_date": "2021-11-01 02:11:00",
            "data": [
                {
                    "dc_code": "8910",
                    "dc_name": "DC Korat",
                    "data_merge": [
                        {
                            "rdc_code": "8910",
                            "rdc_name": "RDC Korat",
                            "data_merge": {
                                "franchise_code": "8010",
                                "franchise_name": "Shop Korat",
                                "lead_time_fc": 7,
                                "inventory_cap_value_fc": 500000
                            }
                        },
                        {
                            "rdc_code": "8920",
                            "rdc_name": "RDC BKK",
                            "data_merge": [
                                {
                                    "franchise_code": "8020",
                                    "franchise_name": "Shop BangNa",
                                    "lead_time_fc": 3,
                                    "inventory_cap_value_fc": 1000000
                                },
                                {
                                    "franchise_code": "8030",
                                    "franchise_name": "Shop Bangkea",
                                    "lead_time_fc": 2,
                                    "inventory_cap_value_fc": 750000
                                },
                            ]
                        }
                    ]
                },
                {
                    "dc_code": "8920",
                    "dc_name": "DC BKK",
                    "data_merge": {
                        "rdc_code": "8930",
                        "rdc_name": "RDC TARGET",
                        "data_merge": [
                            {
                                "franchise_code": "8041",
                                "franchise_name": "Shop TARGET",
                                "lead_time_fc": 10,
                                "inventory_cap_value_fc": 200000
                            },
                            {
                                "franchise_code": "8042",
                                "franchise_name": "Shop BTV",
                                "lead_time_fc": 12,
                                "inventory_cap_value_fc": 350000
                            },
                        ]
                    }
                }
            ]
        }
        """
        _cols_expected: list = [
            k for k, v in self.expected_cols.items() if v.default is None
        ]

        def merge_with_key(
            _data: dict,
            _key: Optional[str] = "data_merge",
        ) -> list[dict]:
            if _key not in _data:
                return [_data]
            _parents: dict = {k: v for k, v in _data.items() if k != _key}
            _children: list = (
                _data_key
                if isinstance((_data_key := _data[_key]), list)
                else [_data_key]
            )
            return [
                _parents | _merge
                for _child in _children
                for _merge in merge_with_key(_child)
            ]

        if isinstance(values, list):
            data_values_list: list = []
            for value in values:
                _data_columns, _data_values = self._generate_merge(value)
                data_values_list.append(_data_values)
            return _cols_expected, ", ".join(data_values_list)

        _data_values_list: list = []
        for _data_insert in merge_with_key(values):
            _columns: list = list(_data_insert)
            # Check duplicated
            self.validate_col_duplicate(_columns)

            # Check outer column does not exist
            self.validate_col_outer(_columns)

            # Check `update_date` exists
            if (
                "update_date" in self.expected_cols
                and "update_date" not in _columns
            ):
                _columns.append("update_date")
                _data_insert["update_date"] = self.vl_update_date

            # Check `not null` exists
            self.validate_col_nullable(_columns)

            # Check SQL injection
            self.validate_col_sql_inject(_data_insert)

            # TODO: Add action_mode == `update` in this ingest mode
            _data_values_list.append(
                self._generate_result_str(_cols_expected, _data_insert)
            )
        return _cols_expected, ", ".join(_data_values_list)

    def _generate_result_str(self, columns, values) -> str:
        if self.vl_action == "update":
            value: str = ", ".join(
                f"'{_data}'" for _ in columns if (_data := values.get(_))
            )
        else:
            value: str = ", ".join(
                [
                    f"'{_data}'" if (_data := values.get(_)) else "null"
                    for _ in columns
                ]
            )
        return f"({value})"
