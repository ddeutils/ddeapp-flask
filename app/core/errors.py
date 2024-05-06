# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from typing import Optional, Union


class BaseError(Exception):
    """Base Exception class"""


class AllExceptions(BaseError):
    """Raise All Exceptions"""


class CatalogBaseError(BaseError):
    """Catalog Base Exception"""


class CatalogNotFound(CatalogBaseError):
    """Exception raised for errors in key of yaml config file"""


class CatalogArgumentError(CatalogBaseError):
    """Exception raised for errors in arguments of yaml config file"""


class ObjectBaseError(BaseError):
    """Object Base Exception"""


class TableNotFound(ObjectBaseError):
    """Exception raised for errors in key of table name."""


class TableNotImplement(ObjectBaseError):
    """Exception raised for errors in any non-implemented functional"""


class TableValidatorError(ObjectBaseError):
    """Exception raised for errors in table validation"""


class TableArgumentError(ObjectBaseError):
    """Exception raised for errors in arguments of method"""


class FuncNotFound(ObjectBaseError):
    """Exception raised for errors in key of function name."""


class FuncRaiseError(ObjectBaseError):
    """Exception raised for errors in function process"""


class FuncArgumentError(ObjectBaseError):
    """Exception raised for errors in arguments of method"""


class PipelineTypeError(ObjectBaseError):
    """Exception raised for errors in type of nodes"""


class ProcessValueError(ObjectBaseError):
    """Exception raised for errors in value of process"""


class DatabaseProcessError(ObjectBaseError):
    """Exception raised for errors in database"""


class DatabaseSchemaNotExists(ObjectBaseError):
    """Exception raised for errors in schema does not exist"""


class ControlTableNotImplement(ObjectBaseError):
    """Exception raised for errors in any non-implemented functional"""


class ControlTableArgumentError(ObjectBaseError):
    """Exception raised for errors in arguments of method"""


class ControlTableValueError(ObjectBaseError):
    """Exception raised for errors in the control framework table value"""


class ControlTableNotExists(ObjectBaseError):
    """Exception raised for errors in the table not exists"""


class ControlPipelineNotExists(ObjectBaseError):
    """Exception raised for errors in the pipeline not exists"""


class ControlProcessNotExists(ObjectBaseError):
    """Exception raised for errors in the process not exists"""


class ValidateFormsError(ObjectBaseError):
    """Exception raised for errors in value in forms"""

    def __init__(
        self,
        name: Union[str, list],
        value: Optional[str] = None,
        message: Optional[str] = None,
    ):
        if message is None:
            message = (
                f"it does not develop for value {value!r}"
                if value
                else "it does exists or not match"
            )
        _name: str = (
            f"`{name}`"
            if isinstance(name, str)
            else ", ".join(f"`{x}`" for x in name)
        )
        _message = f"Please check form key {_name} because "
        super().__init__(_message + message)


class ColumnsNotEqualError(ObjectBaseError):
    """Exception raised for errors in column difference"""


class DuplicateColumnError(ObjectBaseError):
    """Exception raised for errors in column duplication"""


class NullableColumnError(ObjectBaseError):
    """Exception raised for errors in column nullable"""


class PrimaryKeyNotExists(ObjectBaseError):
    """Exception raised for errors in primary column not exists"""


class SQLInjection(ObjectBaseError):
    """Exception raised for errors in sql injection"""


class OuterColumnError(ObjectBaseError):
    """Exception raised for errors in outer column exists"""


class ProcessBaseError(BaseError):
    """Process Base Exception"""


class ProcessStatusError(ProcessBaseError):
    """Exception raised for errors in status process"""


class IOBaseError(BaseError):
    """I/O Base Exception"""


class WriteCSVError(IOBaseError):
    """Exception raised for errors in writing csv file engine"""
