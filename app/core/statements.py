# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import re
from typing import (
    Optional,
    Union,
)

from pydantic import Field

from .convertor import reduce_stm
from .validators import (
    Column,
    Function,
    Partition,
    Profile,
    Schema,
    Table,
)


def reduce_value(value: Union[str, int]) -> str:
    if isinstance(value, list):
        return f"({', '.join([reduce_value(_) for _ in value])})"
    elif value is None:
        return "null"
    return value if value in ("null", "true", "false", "*") else f"'{value}'"


def reduce_value_pairs(value_pairs: dict) -> dict:
    return {
        col: col if value == "*" else reduce_value(value)
        for col, value in value_pairs.items()
    }


def reduce_in_value(value: Union[str, int, list]) -> str:
    if isinstance(value, list):
        return f"({', '.join([reduce_value(_) for _ in value])})"
    return f"({reduce_value(value)})"


def filter_not_null(datatype: str) -> bool:
    return all(not re.search(word, datatype) for word in ["default", "serial"])


class ColumnStatement(Column):
    """Column Model which enhance with generator method for any Postgres
    statement.

    Examples:
        >>> col = Column.parse_obj({})
        ... col_stm = ColumnStatement.parse_obj(col)
        ... col_stm.statement()
    """

    def statement(
        self,
        *,
        name: bool = True,
        pk: bool = True,
        fk: bool = False,
    ) -> str:
        """Return string statement of column value."""
        pk_stm: str = " PRIMARY KEY " if pk and self.pk else " "
        fk_stm: str = (
            f" REFERENCES {{ai_schema_name}}.{self.fk['table']}"
            f"( {self.fk['column']} )"
            if fk and self.fk
            else ""
        )
        return (
            f"{(f'{self.name} ' if name else '')}{self.datatype} "
            f"{'NULL' if self.nullable else 'NOT NULL'} "
            f"{'UNIQUE' if self.unique else ''}"
            f"{pk_stm}"
            f"{self.check or ''}"
            f"{fk_stm}"
        )

    def constraints(self, prefix: Optional[str] = None) -> list:
        """Return list of all constraint that relate with the column."""
        results: list = []
        prefix: str = f"{prefix}_" if prefix else ""
        if self.nullable:
            results.append(f"ALTER COLUMN {self.name} SET NOT NULL")
        else:
            results.append(f"ALTER COLUMN {self.name} DROP NOT NULL")
        if self.check:
            results.append(
                f"ADD CONSTRAINT {prefix}{self.name}_ck {self.check}"
            )
        if self.unique:
            results.append(
                f"ADD CONSTRAINT {prefix}{self.name}_unq UNIQUE({self.name})"
            )
        if self.pk:
            results.append("ADD CONSTRAINT PRIMARY KEY")
        if self.fk:
            results.append(
                f"ADD CONSTRAINT REFERENCES {self.fk['table']} "
                f"( {self.fk['column']} )"
            )
        return results


class PartitionStatement(Partition):
    """Partition Model which enhance with generator method for any Postgres
    statement."""

    def statement(self) -> str:
        """Return."""
        if self.type:
            return f"PARTITION BY {self.type} ( {', '.join(self.columns)} )"
        return ""


class ProfileStatement(Profile):
    """Profile Model which enhance with generator method for any Postgres
    statement."""

    features: list[ColumnStatement] = Field(
        ..., description="Mapping Column features with position order"
    )
    partition: PartitionStatement = Field(
        default_factory=PartitionStatement,
        description="Partition properties",
    )

    def statement_features(self) -> str:
        """Generate combination of features statement."""
        return ", ".join(
            [feature.statement(pk=False, fk=True) for feature in self.features]
        )

    def statement_pk(self) -> str:
        """Generate primary key statement."""
        return (
            f', PRIMARY KEY ( {", ".join(prim)} )'
            if (prim := self.primary_key)
            else ""
        )

    def to_mapping(self, pk: bool = False) -> dict[str, ColumnStatement]:
        """Return list of column name and its datatype."""
        _columns: list[str] = [feature.name for feature in self.features]
        if pk:
            _columns: list[str] = list(
                filter(lambda x: x not in self.primary_key, _columns)
            )
        return {col.name: col for col in self.features if col.name in _columns}


class TableStatement(Table):
    """Table Model which enhance with generator method for any Postgres
    statement."""

    profile: ProfileStatement = Field(
        ..., description="Profile data of catalog"
    )

    def statement_check(self) -> str:
        return reduce_stm(
            f"SELECT CASE WHEN EXISTS("
            f"SELECT FROM {{database_name}}.information_schema.tables "
            f"WHERE table_schema = '{{ai_schema_name}}' "
            f"AND table_name = '{self.name}'"
            f") THEN 'True' ELSE 'False' END AS check_exists"
        )

    def statement_create(self) -> str:
        """Generate create statement.

        :statement:

            CREATE TABLE IF NOT EXISTS DATABASE.SCHEMA.TABLE_NAME
            (
                COLUMN1 DATATYPE01 CONSTRAINTS,
                COLUMN1 DATATYPE02 CONSTRAINTS,
                ...,
                CONSTRAINTS
            )
        """
        return reduce_stm(
            f"CREATE TABLE IF NOT EXISTS "
            f"{{database_name}}.{{ai_schema_name}}.{self.name}"
            f"( {self.profile.statement_features()} "
            f"{self.profile.statement_pk()} )"
            f"{self.profile.partition.statement()}"
        )

    def statement_create_partition(
        self,
        start: str,
        end: Optional[str] = None,
    ) -> str:
        """Generate create partition statement.

        :statement:

            CREATE TABLE IF NOT EXISTS DATABASE.SCHEMA.TABLE_NAME_PARTITION
            PARTITION OF TABLE_NAME
            FOR VALUE FROM ( START ) TO ( END )

        docs:
            - https://www.enterprisedb.com/postgres-tutorials/
                how-use-table-partitioning-scale-postgresql
            - https://www.postgresql.fastware.com/postgresql-insider-prt-ove
        """
        if (
            self.profile.partition.type
            and self.profile.partition.type == "range"
        ):
            if end:
                return reduce_stm(
                    f"CREATE TABLE IF NOT EXISTS "
                    f"{{database_name}}.{{ai_schema_name}}."
                    f"{self.name}_{start}_{end} "
                    f"PARTITION OF {{table_name}} FOR VALUES FROM "
                    f"('{start}') TO ('{end}')"
                )
            raise ValueError("Partition type range should contain end value")
        raise ValueError(
            f"Does not support for partition type "
            f"{self.profile.partition.type!r}"
        )

    def statement_update(self, suffix: Optional[str] = None) -> str:
        """Generate insert statement for receive data from values string.

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
            RETURNING * | OUTPUT_EXPRESSION AS ALIAS_NAME
            ;
        """
        suffix: str = suffix or "UD"
        primary_key_columns: str = self.conflict_set(
            included=self.profile.primary_key,
            word_self=self.shortname,
            word_target=f"{self.shortname}_{suffix}",
            sep="and",
            cast_type=True,
        )
        return reduce_stm(
            f"UPDATE {{database_name}}.{{ai_schema_name}}.{self.name} "
            f"AS {self.shortname} "
            f"SET {{string_columns_pairs}} FROM ( VALUES {{string_values}} ) "
            f"AS {self.shortname}_{suffix}( {{string_columns}} ) "
            + (f"WHERE {primary_key_columns}" if primary_key_columns else "")
        )

    def statement_insert(self) -> str:
        """Generate insert statement for receive data from values string.

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
        conflict: str = (
            (
                f" ON CONFLICT ( {', '.join(pk)} ) DO UPDATE "
                f"SET {self.conflict_set()} "
                f"WHERE {self.shortname}.update_date <= excluded.update_date"
            )
            if (pk := self.profile.primary_key)
            else ""
        )
        return reduce_stm(
            f"INSERT INTO {{database_name}}.{{ai_schema_name}}.{self.name} "
            f"AS {self.shortname} "
            f"( {{string_columns}} ) VALUES  {{string_values}}{conflict}"
        )

    def statement_drop(self, cascade: bool = False) -> str:
        """Generate drop statement.

        :statement:

        DROP TABLE IF EXISTS DATABASE.SCHEMA.TABLE_NAME CASCADE
        """
        _cascade: str = "CASCADE" if cascade else ""
        return reduce_stm(
            f"DROP TABLE IF EXISTS "
            f"{{database_name}}.{{ai_schema_name}}.{self.name} "
            f"{_cascade}"
        )

    def statement_count(self) -> str:
        return reduce_stm(
            f"SELECT {{ai_schema_name}}.func_count_if_exists("
            f"'{{ai_schema_name}}', '{self.name}') AS row_number"
        )

    def statement_count_condition(self) -> str:
        return reduce_stm(
            f"SELECT COUNT(1) AS row_number "
            f"FROM {{database_name}}.{{ai_schema_name}}.{self.name} "
            f"WHERE {{condition}}"
        )

    def statement_backup(self, name: Optional[str] = None) -> str:
        name: str = name or f"{self.name}_bk"
        return reduce_stm(
            re.sub(
                r"{database_name}\.{ai_schema_name}\.\w+",
                f"{{database_name}}.{{ai_schema_backup}}.{name}",
                self.statement_create(),
            )
        )

    def statement_transfer(self, name: Optional[str] = None) -> str:
        name: str = name or f"{self.name}_bk"
        return reduce_stm(
            f"TRUNCATE TABLE {{database_name}}.{{ai_schema_backup}}.{name};"
            f"INSERT INTO {{database_name}}.{{ai_schema_backup}}.{name} "
            f"(SELECT {', '.join(self.profile.columns(pk_included=True))} "
            f"FROM {{database_name}}.{{ai_schema_name}}.{self.name})"
        )

    def statement_vacuum(self, option: str) -> str:
        return reduce_stm(
            f"vacuum {option} {{database_name}}.{{ai_schema_name}}.{self.name}"
        )

    def conflict_set(
        self,
        excluded: Optional[list] = None,
        included: Optional[list] = None,
        word_self: Optional[str] = None,
        word_target: Optional[str] = None,
        sep: Optional[str] = None,
        cast_type: bool = False,
    ) -> str:
        """Return setting conflict statement string :ingest: SET COLUMN =
        EXCLUDED.COLUMN, COLUMN = EXCLUDED.COLUMN, ...

        :update:
                WHERE
                TABLE.COLUMN = TABLE_UD.COLUMN AND
                TABLE.COLUMN = TABLE_UD.COLUMN AND
                ...
        """
        _excluded: list = self.validate_columns(excluded or [])
        _included: list = self.validate_columns(
            included or self.profile.columns(pk_included=False)
        )
        word_self: str = f"{word_self}." if word_self else ""
        word_target: str = word_target or "excluded"
        sep: str = sep or ","
        return f" {sep} ".join(
            [
                (
                    f"{word_self}{feature.name} = {word_target}.{feature.name}"
                    + (f"::{feature.datatype}" if cast_type else "")
                )
                for feature in self.profile.features
                if (
                    filter_not_null(feature.datatype)
                    and feature.name not in _excluded
                    and feature.name in _included
                )
            ]
        )


class FunctionStatement(Function):
    """Function Model which enhance with generator method for any Postgres
    statement."""

    def statement_check(self) -> str:
        return reduce_stm(
            f"SELECT CASE WHEN EXISTS(SELECT * FROM pg_proc "
            f"WHERE proname = '{self.name}') THEN 'True' ELSE 'False' "
            f"END AS check_exists"
        )

    def statement(self) -> str:
        """Return function profile statement."""
        return reduce_stm(self.profile.statement)

    def statement_func_drop(self, cascade: bool = False) -> str:
        """Generate drop statement.

        :statement:

        DROP FUNCTION IF EXISTS DATABASE.SCHEMA.FUNCTION_NAME CASCADE
        """
        _cascade: str = "CASCADE" if cascade else ""
        return reduce_stm(
            f"DROP FUNCTION IF EXISTS "
            f"{{database_name}}.{{ai_schema_name}}.{self.name} {_cascade}"
        )


class SchemaStatement(Schema):
    """Schema Model with enhance with generator method for any Postgres
    statement."""

    def statement_check(self) -> str:
        """Generate check schema statement."""
        return reduce_stm(
            f"SELECT CASE WHEN EXISTS("
            f"SELECT FROM {{database_name}}.information_schema.schemata "
            f"WHERE schema_name = '{self.name}'"
            f") THEN 'True' ELSE 'False' END AS check_exists"
        )

    def statement_create(self) -> str:
        """Generate create schema statement."""
        return reduce_stm(f"CREATE SCHEMA IF NOT EXISTS {self.name}")

    def statement_drop(self, cascade: bool = False) -> str:
        """Generate drop schema statement."""
        _cascade: str = "CASCADE" if cascade else ""
        return reduce_stm(f"DROP SCHEMA IF EXISTS {self.name} {_cascade}")


class ControlStatement:

    def __init__(self, name: str) -> None:
        self.tbl: Table = Table.parse_name(fullname=name)
        self.name: str = self.tbl.name
        self.cols: list[str] = self.tbl.profile.columns(pk_included=True)
        self.cols_no_pk: list[str] = self.tbl.profile.columns(pk_included=False)
        self.pk: list[str] = self.tbl.profile.primary_key

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def __str__(self) -> str:
        return self.name

    @staticmethod
    def _case_params(col: str) -> str:
        return reduce_stm(
            f"CASE WHEN {col} = 'int'  THEN '0' "
            f"     WHEN {col} = 'str'  THEN '' "
            f"     WHEN {col} = 'list' THEN '[]' END "
        )

    @classmethod
    def statement_params(cls) -> str:
        return reduce_stm(
            f"SELECT parameter_name AS param_name "
            f",      parameter_type AS param_type "
            f",      CASE WHEN active_flg = 'N' "
            f"            THEN ({cls._case_params('parameter_type')}) "
            f"            ELSE parameter_value "
            f"       END AS param_value "
            f"FROM   {{database_name}}.{{ai_schema_name}}.ctr_data_parameter "
            f"WHERE  module_type = {{module_type}}"
        )

    def statement_pull(self) -> str:
        return reduce_stm(
            f"SELECT {{select_columns}} "
            f"FROM {{database_name}}.{{ai_schema_name}}.{self.name} "
            f"WHERE {{primary_key_filters}} {{active_flag}} {{condition}}"
        )

    def statement_create(self) -> str:
        return reduce_stm(
            f"INSERT INTO {{database_name}}.{{ai_schema_name}}.{self.name} "
            f"AS {self.tbl.shortname} ( {{columns_pair}} ) "
            f"VALUES (  {{values}}  ) "
            f"on conflict ( {{primary_key}} ) do update "
            f"set {{set_value_pairs}} "
            f"{{status_filter}} {{row_record_filter}} {{condition}}"
        )

    def statement_push(self) -> str:
        return reduce_stm(
            f"UPDATE {{database_name}}.{{ai_schema_name}}.{self.name} "
            f"AS {self.tbl.shortname} "
            f"set  {{update_values_pairs}} "
            f"where {{filter}} {{condition}}"
        )
