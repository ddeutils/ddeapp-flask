# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import re
from datetime import (
    date,
    datetime,
)
from flask import request
from typing import (
    Optional,
    Union,
)
from application.core.utils.reusables import (
    convert_str_list,
    merge_dicts,
)
from application.core.validators import (
    Table,
    Pipeline,
)
from application.core.errors import (
    CatalogNotFound,
    ValidateFormsError,
)


def validate_run_date(params: str) -> bool:
    re_pattern = re.compile(r'^20\d{2}-\d{2}-\d{2}$')
    if re_pattern.match(params):
        _ = date.fromisoformat(params)
        return False
    return not re_pattern.match(params)


def validate_update_date(params: str) -> bool:
    re_pattern = re.compile(r'^20\d{2}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
    if re_pattern.match(params):
        _ = datetime.fromisoformat(params)
        return False
    return not re_pattern.match(params)


def validate_parameter(
        params: str, fix_list: list, option: Optional[str] = None
) -> bool:
    if option == 'split':
        return all(i != param for i in fix_list for param in params)
    return params not in fix_list


def validate_table(tbl_name: Optional[str], optional: bool = False) -> bool:
    try:
        if not tbl_name:
            return not optional
        Table.parse_name(tbl_name)
        return False
    except CatalogNotFound:
        return True


def validate_table_short(
        tbl_name_sht: Optional[str], optional: bool = False
) -> bool:
    try:
        if not tbl_name_sht:
            return not optional
        Table.parse_shortname(tbl_name_sht)
        return False
    except CatalogNotFound:
        return True


def validate_pipeline(pipe_name, optional: bool = False) -> bool:
    try:
        if not pipe_name:
            return not optional
        Pipeline.parse_name(pipe_name)
        return False
    except CatalogNotFound:
        return True


class BaseValidate:

    def __init__(self, form, add_values: Optional[dict] = None):
        self.data_form = form
        try:
            self.data_result = {
                k: self.data_form.get(k, self.__class__.__dict__[k])
                if k in self.__class__.__dict__ else self.data_form[k]
                for k, v in self.__annotations__.items()
                if not k.startswith('_')
            }
        except KeyError as key:
            raise ValidateFormsError(
                str(key), message="key does not exists"
            ) from key

        self._methods: dict = {
            name: func for name in dir(self.__class__)
            if (
                    callable(func := getattr(self.__class__, name))
                    and not name.startswith("_")
            )
        }
        if add_values:
            self.data_result = merge_dicts(self.data_result, add_values)
        self._refactors()

    def as_dict(self) -> dict:
        self._validates()
        self._co_validates()
        self._expands()
        return self.data_result

    def _validates(self) -> None:
        for data, value in self.data_result.items():
            if func := self._methods.get(f'validate_{data}'):
                func(value)

    def _refactors(self) -> None:
        _result: dict = {}
        for data, value in self.data_result.items():
            if func := self._methods.get(f'refactor_{data}'):
                _result: dict = merge_dicts(_result, func(value))
            else:
                _result[data] = value
        self.data_result = _result

    def _expands(self) -> None:
        _result: dict = {}
        for name, func in self._methods.items():
            if name.startswith('expand_'):
                values: list = name.replace('expand_', '').split('_and_')
                _result: dict = merge_dicts(
                    _result,
                    func(**{
                        data: value
                        for data, value in self.data_result.items()
                        if data in values
                    }),
                )
        self.data_result = merge_dicts(self.data_result, _result)

    def _co_validates(self) -> None:
        for name, func in self._methods.items():
            if name.startswith('co_validate_'):
                values: list = name.replace('co_validate_', '').split('_and_')
                func(**{
                    data: value
                    for data, value in self.data_result.items()
                    if data in values
                })


class FormValidate(BaseValidate):

    @classmethod
    def add(cls, value: dict):
        return cls(add_values=value)

    def __init__(self, add_values: Optional[dict] = None):
        super(FormValidate, self).__init__(
            form=request.form,
            add_values=add_values
        )

    @staticmethod
    def refactor_run_date(run_date: str) -> dict:
        return {'run_dates': convert_str_list(run_date)}

    @classmethod
    def validate_run_dates(cls, run_dates: list) -> None:
        if any(validate_run_date(run_date) for run_date in run_dates):
            raise ValidateFormsError(
                'run_dates',
                str(run_dates),
                "it does not ISO date format: YYYY-mm-dd'"
            )


class ContentValidate(BaseValidate):

    @classmethod
    def add(cls, value: dict):
        return cls(add_values=value)

    def __init__(self, add_values: Optional[dict] = None):
        super(ContentValidate, self).__init__(
            form=request.get_json(force=False, silent=True),
            add_values=add_values
        )

    @staticmethod
    def refactor_data(data: Union[list, dict]) -> dict:
        return {'payloads': data}

    @staticmethod
    def refactor_mode(mode: str) -> dict:
        return {'ingest_action': mode}

    @staticmethod
    def expand_tbl_name_short(tbl_name_short: str) -> dict:
        return {'table_name': Table.parse_shortname(tbl_name_short).name}
