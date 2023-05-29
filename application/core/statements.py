# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import re
from typing import (
    List,
    Union,
    Optional,
)
from pydantic import Field
from application.core.validators import (
    Column,
    Partition,
    Profile,
    Table,
    Function,
)
from application.utils.convertor import (
    reduce_stm,
)


def filter_not_null(datatype: str) -> bool:
    return all(not re.search(word, datatype) for word in ['default', 'serial'])


class CatalogValidateError(ValueError):
    """Error for validation process of catalog"""


class ColumnStatement(Column):
    """Column Model which enhance with generator method for any Postgres statement

    :usage:
        ..> col = Column( ... )
        ... col_stm = ColumnStatement.parse_obj(col)
        ... col_stm.statement()

    """
    def statement(self, pk: bool = True, fk: bool = False) -> str:
        """Return string statement of column value"""
        pk_stm: str = (
            " PRIMARY KEY " if pk and self.pk else ' '
        )
        fk_stm: str = (
            f" REFERENCES {{ai_schema_name}}.{self.fk['table']}( {self.fk['column']} )"
            if fk and self.fk
            else ''
        )
        return (
            f"{self.datatype} "
            f"{'NULL' if self.nullable else 'NOT NULL'} "
            f"{'UNIQUE' if self.unique else ''}"
            f"{pk_stm}"
            f"{self.check or ''}"
            f"{fk_stm}"
        )

    def constraints(self, prefix: Optional[str] = None) -> list:
        """Return list of all constraint that relate with the column"""
        results: list = []
        prefix: str = f"{prefix}_" if prefix else ''
        if self.nullable:
            results.append(
                f"ALTER COLUMN {self.name} SET NOT NULL"
            )
        else:
            results.append(
                f"ALTER COLUMN {self.name} DROP NOT NULL"
            )
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
                f"ADD CONSTRAINT REFERENCES {self.fk['table']} ( {self.fk['column']} )"
            )
        return results


class PartitionStatement(Partition):
    """Partition Model which enhance with generator method for any Postgres statement"""
    def statement(self) -> str:
        """Return"""
        if self.type:
            return f"PARTITION BY {self.type} ( {', '.join(self.columns)} )"
        return ''


class ProfileStatement(Profile):
    """Profile Model which enhance with generator method for any Postgres statement"""
    features: List[ColumnStatement] = Field(..., description='Mapping Column features with position order')
    partition: Union[PartitionStatement, dict] = Field(default_factory=dict, description='Partition properties')

    def statement_features(self) -> str:
        """Generate combination of features statement"""
        return ", ".join(
            [feature.statement(pk=False, fk=True) for feature in self.features]
        )

    def statement_pk(self) -> str:
        """Generate primary key statement"""
        return f', PRIMARY KEY ( {", ".join(prim)} )' if (prim := self.primary_key) else ""


class TableStatement(Table):
    """Table Model which enhance with generator method for any Postgres statement"""
    profile: ProfileStatement = Field(..., description='Profile data of catalog')

    def statement_create(
            self, bk: bool = False, bk_name: Optional[str] = None
    ) -> str:
        """Generate create statement

        :statement:

            CREATE TABLE IF NOT EXISTS DATABASE.SCHEMA.TABLE_NAME
            (
                COLUMN1 DATATYPE01 CONSTRAINTS,
                COLUMN1 DATATYPE02 CONSTRAINTS,
                ...,
                CONSTRAINTS
            )
        """
        create: str = reduce_stm((
            f"CREATE TABLE IF NOT EXISTS {{database_name}}.{{ai_schema_name}}.{self.name}"
            f"( {self.profile.statement_features()} {self.profile.statement_pk()} )"
            f"{self.profile.partition.statement()}"
        ))
        return (
            re.sub(
                r"{database_name}\.{ai_schema_name}\.\w+",
                f"{{database_name}}.{{ai_schema_name_backup}}.{bk_name or f'{self.name}_bk'}",
                create
            ) if bk else create
        )

    def statement_create_partition(self, start: str, end: Optional[str] = None) -> str:
        """Generate create partition statement

        :statement:

            CREATE TABLE IF NOT EXISTS DATABASE.SCHEMA.TABLE_NAME_PARTITION
            PARTITION OF TABLE_NAME
            FOR VALUE FROM ( START ) TO ( END )

        docs:
            - https://www.enterprisedb.com/postgres-tutorials/how-use-table-partitioning-scale-postgresql
            - https://www.postgresql.fastware.com/postgresql-insider-prt-ove
        """
        if self.profile.partition.type and self.profile.partition.type == 'range':
            if end:
                return reduce_stm((
                    f"CREATE TABLE IF NOT EXISTS {{database_name}}.{{ai_schema_name}}.{self.name}_{start}_{end}"
                    f"PARTITION OF {{table_name}} FOR VALUES FROM ('{start}') TO ('{end}')"
                ))
            raise ValueError("Partition type range should contain end value")
        raise ValueError(f"Does not support for partition type {self.profile.partition.type!r}")

    def statement_update(self, suffix: Optional[str] = None) -> str:
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
        suffix: str = suffix or 'ud'
        primary_key_columns: str = self.conflict_set(
            included=self.profile.primary_key,
            word_self=self.shortname,
            word_target=f"{self.shortname}_{suffix}",
            sep='and'
        )
        return reduce_stm((
            f"UPDATE {{database_name}}.{{ai_schema_name}}.{self.name} AS {self.shortname}"
            f"SET {{string_columns_pairs}} FROM ( values {{string_values}} )"
            f"AS {self.shortname}_{suffix}( {{string_columns}} )"
            f"WHERE {primary_key_columns}"
        ))

    def statement_insert(self) -> str:
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
        conflict: str = (
                f" ON CONFLICT ( {pk} ) DO UPDATE SET {self.conflict_set()}" 
                f"WHERE {self.shortname}.update_date <= excluded.update_date"
            ) if (pk := self.profile.statement_pk()) else ""
        return reduce_stm((
            f"INSERT INTO {{database_name}}.{{ai_schema_name}}.{self.name} AS {self.shortname}"
            f"( {{string_columns}} ) VALUES  {{string_values}}{conflict}"
        ))

    def statement_drop(self, cascade: bool = False) -> str:
        """Generate drop statement

        :statement:

            DROP TABLE IF EXISTS DATABASE.SCHEMA.TABLE_NAME CASCADE

        """
        _cascade: str = 'cascade' if cascade else ''
        return reduce_stm((
            f"DROP TABLE IF EXISTS {{database_name}}.{{ai_schema_name}}.{self.name} {cascade}"
        ))

    def conflict_set(
            self,
            excluded: Optional[list] = None,
            included: Optional[list] = None,
            word_self: Optional[str] = None,
            word_target: Optional[str] = None,
            sep: Optional[str] = None
    ) -> str:
        """Return setting conflict statement string"""
        _excluded: list = self.validate_columns(excluded or [])
        _included: list = self.validate_columns(included or self.profile.columns(pk_included=False))
        word_self: str = f"{word_self}." if word_self else ''
        word_target: str = word_target or 'excluded'
        sep: str = sep or ','
        return f" {sep} ".join([
            f"{word_self}{feature.name} = {word_target}.{feature.name}"
            for feature in self.profile.features
            if (
                    filter_not_null(feature.datatype)
                    and feature.name not in _excluded
                    and feature.name in _included
            )
        ])


class FunctionStatement(Function):
    """"""

    def statement_create(self) -> str:
        return self.profile.statement

    def statement_drop(self, cascade: bool = False) -> str:
        """Generate drop statement

        :statement:

            DROP FUNCTION IF EXISTS DATABASE.SCHEMA.FUNCTION_NAME CASCADE

        """
        _cascade: str = 'cascade' if cascade else ''
        return reduce_stm((
            f"DROP FUNCTION IF EXISTS {{database_name}}.{{ai_schema_name}}.{self.name} {cascade}"
        ))
