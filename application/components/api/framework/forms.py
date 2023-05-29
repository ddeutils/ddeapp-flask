# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import os
from typing import (
    Optional,
    Union,
)
from ....errors import ValidateFormsError
from ....core.legacy.base import get_run_date
from ....utils.validations import (
    FormValidate,
    validate_parameter,
    validate_table,
    validate_pipeline,
)


class FormSetup(FormValidate):
    run_date: Union[str, list] = get_run_date()
    pipeline_name: Optional[str] = None
    table_name: Optional[str] = None
    initial_data: str = 'N'
    drop_before_create: str = 'N'
    drop_table: str = 'N'
    drop_schema: str = 'N'
    background: str = 'Y'
    cascade: str = 'N'

    @classmethod
    def validate_pipeline_name(cls, pipeline_name):
        if validate_pipeline(pipeline_name, optional=True):
            raise ValidateFormsError(
                'table_name', pipeline_name, f"{pipeline_name!r} or any nodes does not exists in catalog"
            )

    @classmethod
    def validate_table_name(cls, table_name):
        if validate_table(table_name, optional=True):
            raise ValidateFormsError('table_name', table_name, f"{table_name!r} does not exists in catalog")

    @classmethod
    def validate_initial_data(cls, initial_data):
        if validate_parameter(initial_data, ['Y', 'N', 'A', 'S', 'I'], option='split'):
            raise ValidateFormsError('initial_data', initial_data)

    @classmethod
    def validate_drop_before_create(cls, drop_before_create):
        if validate_parameter(drop_before_create, ['Y', 'N', 'C', 'A', 'S', 'I'], option='split'):
            raise ValidateFormsError('drop_before_create', drop_before_create)

    @classmethod
    def validate_drop_table(cls, drop_table):
        if validate_parameter(drop_table, ['Y', 'N'], option='split'):
            raise ValidateFormsError('drop_table', drop_table)

    @classmethod
    def validate_drop_schema(cls, drop_schema):
        if validate_parameter(drop_schema, ['Y', 'N'], option='split'):
            raise ValidateFormsError('drop_schema', drop_schema)

    @classmethod
    def validate_cascade(cls, cascade):
        if validate_parameter(cascade, ['Y', 'N'], option='split'):
            raise ValidateFormsError('drop_schema', cascade)

    @classmethod
    def validate_background(cls, background):
        if validate_parameter(background, ['Y', 'N'], option='split'):
            raise ValidateFormsError('background', background)

    @classmethod
    def co_validate_table_name_and_pipeline_name(cls, table_name, pipeline_name):
        if table_name is None and pipeline_name is None:
            raise ValidateFormsError('table_name', table_name, "it does not set simultaneously with `pipeline_name`")


class FormData(FormValidate):
    run_date: Union[str, list] = get_run_date()
    pipeline_name: Optional[str] = None
    table_name: Optional[str] = None
    run_mode: str = 'common'
    background: str = 'Y'

    @classmethod
    def validate_pipeline_name(cls, pipeline_name):
        if validate_pipeline(pipeline_name, optional=True):
            raise ValidateFormsError(
                'pipeline_name', pipeline_name, f"{pipeline_name!r} or any nodes does not exists in catalog"
            )

    @classmethod
    def validate_table_name(cls, table_name):
        if validate_table(table_name, optional=True):
            raise ValidateFormsError('table_name', table_name, f"{table_name!r} does not exists in catalog")

    @classmethod
    def validate_run_mode(cls, run_mode):
        if validate_parameter(run_mode, ['common', 'rerun']):
            raise ValidateFormsError('run_mode', run_mode)

    @classmethod
    def validate_background(cls, background):
        if validate_parameter(background, ['Y', 'N'], option='split'):
            raise ValidateFormsError('background', background)

    @classmethod
    def co_validate_table_name_and_pipeline_name(cls, table_name, pipeline_name):
        if table_name is None and pipeline_name is None:
            raise ValidateFormsError('table_name', table_name, "it does not set simultaneously with `pipeline_name`")


class FormRetention(FormValidate):
    run_date: Union[str, list] = get_run_date()
    pipeline_name: Optional[str] = None
    table_name: Optional[str] = None
    backup_table: Optional[str] = None
    backup_schema: Optional[str] = None
    background: str = 'Y'

    @classmethod
    def validate_pipeline_name(cls, pipeline_name):
        if validate_pipeline(pipeline_name, optional=True):
            raise ValidateFormsError(
                'pipeline_name', pipeline_name, f"{pipeline_name!r} or any nodes does not exists in catalog"
            )

    @classmethod
    def validate_table_name(cls, table_name):
        if validate_table(table_name, optional=True):
            raise ValidateFormsError('table_name', table_name, f"{table_name!r} does not exists in catalog")

    @classmethod
    def validate_backup_table(cls, backup_table):
        if not validate_table(backup_table, optional=False):
            raise ValidateFormsError('backup_table', backup_table, f"{backup_table!r} does not exists in catalog")

    @classmethod
    def validate_backup_schema(cls, backup_schema):
        if not validate_parameter(backup_schema, [os.getenv('AI_SCHEMA', 'ai'), os.getenv('MAIN_SCHEMA', 'public')]):
            raise ValidateFormsError('backup_schema', backup_schema, f"{backup_schema!r} does not exists in database")

    @classmethod
    def validate_background(cls, background):
        if validate_parameter(background, ['Y', 'N'], option='split'):
            raise ValidateFormsError('background', background)

    @classmethod
    def co_validate_table_name_and_backup_table(cls, table_name, backup_table):
        if table_name is None and backup_table is not None:
            raise ValidateFormsError('table_name', table_name, "it does not set while `backup_table` was set")

    @classmethod
    def co_validate_table_name_and_pipeline_name(cls, table_name, pipeline_name):
        if table_name is None and pipeline_name is None:
            raise ValidateFormsError('table_name', table_name, "it does not set simultaneously with `pipeline_name`")
