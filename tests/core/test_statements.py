import unittest

from app.core.statements import (
    ColumnStatement,
)
from app.core.validators import Column


class ColumnStatementTestCase(unittest.TestCase):
    """Test Case for Column object from statements file."""

    def setUp(self) -> None:
        """Set up input attributes for parsing to the Column model."""
        self.input_01: dict = {
            "name": "column_name",
            "datatype": (
                "varchar( 128 ) not null unique primary key "
                "check(column_name <> 'DEMO')"
            ),
        }
        self.input_02: dict = {
            "name": "column_name",
            "datatype": "varchar( 128 ) not null unique",
            "pk": True,
            "check": "check(column_name <> 'DEMO')",
        }
        self.input_default: dict = {"name": "default", "datatype": "bigint"}

    def tearDown(self) -> None: ...

    def test_parsing_01_to_statement(self):
        pre_result: Column = Column.parse_obj(self.input_01)
        result: ColumnStatement = ColumnStatement.parse_obj(pre_result)

        respec_statement: str = (
            "varchar( 128 ) NOT NULL UNIQUE PRIMARY KEY "
            "check(column_name <> 'DEMO')"
        )
        self.assertEqual(respec_statement, result.statement())

        respec_constraints: list = [
            "ALTER COLUMN column_name DROP NOT NULL",
            "ADD CONSTRAINT column_name_ck check(column_name <> 'DEMO')",
            "ADD CONSTRAINT column_name_unq UNIQUE(column_name)",
            "ADD CONSTRAINT PRIMARY KEY",
        ]
        self.assertListEqual(respec_constraints, result.constraints())


class ProfileStatementTestCase(unittest.TestCase): ...


class TableStatementTestCase(unittest.TestCase):
    def setUp(self) -> None: ...

    def test_parsing_01_to_statement_update(self):
        respec: str = ""
        result: str = ""
        self.assertEqual(respec, result)
