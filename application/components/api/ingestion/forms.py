# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

from typing import Union
from flask import request
from application.core.errors import ValidateFormsError
from application.core.base import get_run_date
from application.components.api.validations import (
    ContentValidate,
    validate_parameter,
    validate_run_date,
    validate_update_date,
    validate_table_short,
)


class FormIngest(ContentValidate):
    tbl_name_short: str = 'undefined'
    run_date: str = get_run_date()
    update_date: str = get_run_date(fmt='%Y-%m-%d %H:%M:%S')
    mode: str = 'insert'
    ingest_mode: str = 'common'
    background: str = 'Y'
    data: Union[list, dict]

    @classmethod
    def validate_run_date(cls, run_date):
        if validate_run_date(run_date):
            raise ValidateFormsError('run_date', run_date, "it does not ISO date format: YYYY-mm-dd'")

    @classmethod
    def validate_update_date(cls, update_date):
        if validate_update_date(update_date):
            raise ValidateFormsError('update_date', update_date, "it does not ISO date format: YYYY-mm-dd HH:MM:SS'")

    @classmethod
    def validate_tbl_name_short(cls, tbl_name_short):
        if validate_table_short(tbl_name_short, optional=False):
            raise ValidateFormsError(
                'tbl_name_short',
                tbl_name_short,
                (
                    f"{tbl_name_short!r} in '/api/ai/put/{tbl_name_short}' "
                    f"does not exists in catalog"
                ),
            )

    @classmethod
    def validate_ingest_action(cls, ingest_action):
        if validate_parameter(ingest_action, ['insert', 'update']):
            raise ValidateFormsError('ingest_action', ingest_action)

    @classmethod
    def validate_ingest_mode(cls, ingest_mode):
        if validate_parameter(ingest_mode, ['common', 'merge']):
            raise ValidateFormsError('ingest_mode', ingest_mode)

    @classmethod
    def validate_payloads(cls, payloads):
        if not (payloads and isinstance(payloads, (dict, list))) and request.method == 'PUT':
            raise ValidateFormsError(
                'data', payloads, "`data` does not exists in data-raw or type of `data` does not support"
            )

    @classmethod
    def validate_background(cls, background):
        if validate_parameter(background, ['Y', 'N'], option='split'):
            raise ValidateFormsError('background', background)
