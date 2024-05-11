import datetime
import unittest
from unittest import mock

from pydantic import ValidationError

from app.core.models import Status, TaskComponent, TaskMode
from app.core.validators import (
    Column,
    Profile,
    Table,
    TableFrontend,
    Task,
)


class ColumnValidatorTestCase(unittest.TestCase):
    """Test Case for Column object from validators file."""

    def setUp(self) -> None:
        """Set up input attributes for parsing to the Column model."""
        self.maxDiff = None
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

    def test_parsing_01_from_object(self):
        respec: dict = {
            "name": "column_name",
            "datatype": "varchar( 128 )",
            "nullable": False,
            "unique": True,
            "default": None,
            "check": "check(column_name <> 'DEMO')",
            "pk": True,
            "fk": {},
        }
        result: Column = Column.parse_obj(self.input_01)
        self.assertDictEqual(respec, result.dict(by_alias=False))

    def test_parsing_02_from_object(self):
        respec: dict = {
            "name": "column_name",
            "datatype": "varchar( 128 )",
            "nullable": False,
            "unique": True,
            "default": None,
            "check": "check(column_name <> 'DEMO')",
            "pk": True,
            "fk": {},
        }
        result: Column = Column.parse_obj(self.input_02)
        self.assertDictEqual(respec, result.dict(by_alias=False))

    def test_compare_01_and_02(self):
        _result_01: Column = Column.parse_obj(self.input_01)
        _result_02: Column = Column.parse_obj(self.input_02)
        self.assertEqual(_result_01, _result_02)

    def test_raise_with_null(self):
        with self.assertRaises(ValidationError) as context:
            Column.parse_obj({})
        error_wrapper: ValidationError = context.exception
        errors: list = error_wrapper.errors()
        print(errors)
        self.assertEqual(len(errors), 1)
        self.assertEqual("__root__", errors[0]["loc"][0])
        self.assertTrue(
            "datatype does not contain in values" in errors[0]["msg"]
        )

    def test_raise_with_no_datatype(self):
        with self.assertRaises(ValidationError) as context:
            Column.parse_obj({"name": "no_datatype"})
        error_wrapper: ValidationError = context.exception
        errors: list = error_wrapper.errors()
        self.assertEqual(len(errors), 1)
        self.assertEqual("__root__", errors[0]["loc"][0])
        self.assertTrue(
            "datatype does not contain in values" in errors[0]["msg"]
        )


class ProfileValidatorTestCase(unittest.TestCase):
    """Test Case for Column object from validators file."""

    def setUp(self) -> None:
        self.maxDiff = None
        self.input_01: dict = {
            "features": {
                1: {
                    "name": "column_name",
                    "datatype": "datatype_for_01 not null",
                }
            },
            "primary_key": ["column_name"],
            "foreign_key": {"column_name": "other( column_name )"},
        }
        self.input_02: dict = {
            "features": [
                {"name": "column_name", "datatype": "datatype_for_01 not null"}
            ],
            "primary_key": ["column_name"],
            "foreign_key": {
                "column_name": {
                    "ref_table": "other",
                    "ref_column": "column_name",
                }
            },
        }
        self.input_03: dict = {
            "features": {
                "column_name": {"datatype": "datatype_for_01 not null"},
                "column_name_2": {"datatype": "int", "nullable": False},
            },
            "primary_key": ["column_name"],
            "partition": {"type": "range", "columns": ["column_name_2"]},
        }

    def tearDown(self) -> None: ...

    def test_parsing_01_from_object(self):
        result = Profile.parse_obj(self.input_01)
        respec: dict = {
            "features": [
                {
                    "name": "column_name",
                    "datatype": "datatype_for_01",
                    "nullable": False,
                    "default": None,
                    "unique": False,
                    "check": None,
                    "pk": True,
                    "fk": {
                        "table": "other",
                        "column": "column_name",
                    },
                }
            ],
            "primary_key": ["column_name"],
            "foreign_key": [
                {
                    "name": "column_name",
                    "ref_table": "other",
                    "ref_column": "column_name",
                }
            ],
            "partition": {},
        }
        print(result.dict(by_alias=False))
        self.assertDictEqual(respec, result.dict(by_alias=False))

    def test_parsing_02_from_object(self):
        result = Profile.parse_obj(self.input_02)
        respec: dict = {
            "features": [
                {
                    "name": "column_name",
                    "datatype": "datatype_for_01",
                    "nullable": False,
                    "default": None,
                    "unique": False,
                    "check": None,
                    "pk": True,
                    "fk": {
                        "table": "other",
                        "column": "column_name",
                    },
                }
            ],
            "primary_key": ["column_name"],
            "foreign_key": [
                {
                    "name": "column_name",
                    "ref_table": "other",
                    "ref_column": "column_name",
                }
            ],
            "partition": {},
        }
        print(result.dict(by_alias=False))
        self.assertDictEqual(respec, result.dict(by_alias=False))

    def test_parsing_03_from_object(self):
        result = Profile.parse_obj(self.input_03)
        respec: dict = {
            "features": [
                {
                    "name": "column_name",
                    "datatype": "datatype_for_01",
                    "nullable": False,
                    "default": None,
                    "unique": False,
                    "check": None,
                    "pk": True,
                    "fk": {},
                },
                {
                    "name": "column_name_2",
                    "datatype": "int",
                    "nullable": False,
                    "default": None,
                    "unique": False,
                    "check": None,
                    "pk": False,
                    "fk": {},
                },
            ],
            "primary_key": ["column_name"],
            "foreign_key": [],
            "partition": {"type": "range", "columns": ["column_name_2"]},
        }
        print(result.dict(by_alias=False))
        self.assertDictEqual(respec, result.dict(by_alias=False))

    def test_compare_01_and_02(self):
        _result_01: Profile = Profile.parse_obj(self.input_01)
        _result_02: Profile = Profile.parse_obj(self.input_02)
        self.assertTrue(_result_01 == _result_02)

    def test_parsing_04_from_null(self):
        result = Profile.parse_obj({})
        respec: dict = {
            "features": [],
            "primary_key": [],
            "foreign_key": [],
            "partition": {},
        }
        print(result.dict(by_alias=False))
        self.assertDictEqual(respec, result.dict(by_alias=False))

    def test_raise_with_primary_key_only(self):
        with self.assertRaises(ValidationError) as context:
            Profile.parse_obj({"primary_key": ["column_name"]})
        error_wrapper: ValidationError = context.exception
        errors: list = error_wrapper.errors()
        print(errors)
        self.assertTrue(len(errors) == 1)
        self.assertEqual("primary_key", errors[0]["loc"][0])
        self.assertEqual("value_error", errors[0]["type"])
        self.assertTrue(
            "column_name in primary key does not exists in features"
            in errors[0]["msg"]
        )

    def test_raise_with_partition_column(self): ...


class TableValidatorTestCase(unittest.TestCase):
    """Test Case for Table object from validators file."""

    def setUp(self) -> None:
        self.maxDiff = None
        self.input_01: dict = {
            "name": "table_name",
            "profile": {
                "features": {
                    "column_name": {"datatype": "datatype_for_01 not null"},
                    "column_name_2": {"datatype": "int", "nullable": False},
                },
                "primary_key": ["column_name"],
                "partition": {"type": "range", "columns": ["column_name_2"]},
            },
            "process": {
                "process_01": {
                    "parameter": [],
                    "statement": "statement_01",
                }
            },
        }

    # docs: https://stackoverflow.com/questions/4481954/trying-to-mock-datetime-date-today-but-not-working
    @mock.patch("app.core.validators.datetime", warps=datetime.datetime)
    def test_parsing_01_from_object_sql(self, mock_datetime: mock.MagicMock):
        mock_datetime.now.return_value = datetime.datetime(2023, 3, 13, 0, 0)
        result = Table.parse_obj(self.input_01)
        respec: dict = {
            "name": "table_name",
            "shortname": "tn",
            "prefix": "table",
            "type": "sql",
            "process_max": 1,
            "profile": {
                "features": [
                    {
                        "name": "column_name",
                        "datatype": "datatype_for_01",
                        "nullable": False,
                        "unique": False,
                        "default": None,
                        "check": None,
                        "pk": True,
                        "fk": {},
                    },
                    {
                        "name": "column_name_2",
                        "datatype": "int",
                        "nullable": False,
                        "unique": False,
                        "default": None,
                        "check": None,
                        "pk": False,
                        "fk": {},
                    },
                ],
                "primary_key": ["column_name"],
                "foreign_key": [],
                "partition": {
                    "type": "range",
                    "columns": ["column_name_2"],
                },
            },
            "process": {
                "process_01": {
                    "name": "process_01",
                    "parameter": [],
                    "priority": 1,
                    "statement": "statement_01; ",
                }
            },
            "initial": {},
            "tag": {
                "author": "undefined",
                "description": None,
                "labels": [],
                "version": datetime.date(2023, 3, 13),
                "ts": datetime.datetime(2023, 3, 13, 0, 0),
            },
        }
        self.assertDictEqual(respec, result.dict(by_alias=False))

    def test_parsing_02_from_object_sql(self): ...

    def test_parsing_03_from_object_py(self): ...

    @mock.patch("app.core.validators.datetime", warps=datetime.datetime)
    def test_parsing_from_name_sql(self, mock_datetime: mock.MagicMock):
        mock_datetime.now.return_value = datetime.datetime(2023, 3, 13, 0, 0)
        statement: str = (
            "insert into {database_name}.{ai_schema_name}.ai_actual_sales_mch3 "
            "as aasm ( select cat_mch3_code , sum(actual_sales_qty) as "
            "actual_sales_qty , sum(actual_sales_value) as actual_sales_value "
            ", start_of_month from {database_name}.{ai_schema_name}."
            "ai_actual_sales_article where ( start_of_month between "
            "date_trunc('month', (date '{data_date}') - interval "
            "'{date_range_recheck_month} month')::date and date_trunc('month',"
            " (date '{run_date}' - interval '{date_range_sla_month} "
            "month'))::date ) group by 1,4 ) on conflict ( cat_mch3_code, "
            "start_of_month ) do update set actual_sales_qty = "
            "excluded.actual_sales_qty , actual_sales_value = "
            "excluded.actual_sales_value where ( round(aasm.actual_sales_qty, "
            "3) <> round(excluded.actual_sales_qty, 3) and "
            "excluded.actual_sales_qty > 0 ) or ( "
            "round(aasm.actual_sales_value, 3) <> "
            "round(excluded.actual_sales_value, 3) and "
            "excluded.actual_sales_value > 0 ); "
        )
        result = Table.parse_name("sql:ai_actual_sales_mch3")
        respec: dict = {
            "name": "ai_actual_sales_mch3",
            "shortname": "aasm",
            "prefix": "ai",
            "type": "sql",
            "process_max": 1,
            "profile": {
                "features": [
                    {
                        "name": "cat_mch3_code",
                        "datatype": "varchar( 32 )",
                        "nullable": False,
                        "unique": False,
                        "default": None,
                        "check": None,
                        "pk": True,
                        "fk": {},
                    },
                    {
                        "name": "actual_sales_qty",
                        "datatype": "numeric( 24, 8 )",
                        "nullable": True,
                        "unique": False,
                        "default": None,
                        "check": None,
                        "pk": False,
                        "fk": {},
                    },
                    {
                        "name": "actual_sales_value",
                        "datatype": "numeric( 24, 8 )",
                        "nullable": True,
                        "unique": False,
                        "default": None,
                        "check": None,
                        "pk": False,
                        "fk": {},
                    },
                    {
                        "name": "start_of_month",
                        "datatype": "date",
                        "nullable": False,
                        "unique": False,
                        "default": None,
                        "check": None,
                        "pk": True,
                        "fk": {},
                    },
                ],
                "primary_key": ["cat_mch3_code", "start_of_month"],
                "foreign_key": [],
                "partition": {},
            },
            "process": {
                "from_ai_actual_sales_article": {
                    "name": "from_ai_actual_sales_article",
                    "parameter": [
                        "data_date",
                        "date_range_recheck_month",
                        "date_range_sla_month",
                        "run_date",
                    ],
                    "priority": 1,
                    "statement": statement,
                }
            },
            "initial": {},
            "tag": {
                "author": "undefined",
                "description": None,
                "labels": [],
                "version": datetime.date(1970, 1, 1),
                "ts": datetime.datetime(2023, 3, 13, 0, 0),
            },
        }
        self.assertDictEqual(respec, result.dict(by_alias=False))

    @mock.patch("app.core.validators.datetime", warps=datetime.datetime)
    def test_parsing_from_shortname_sql(self, mock_datetime: mock.MagicMock):
        mock_datetime.now.return_value = datetime.datetime(2023, 3, 13, 0, 0)
        statement: str = (
            "insert into {database_name}.{ai_schema_name}.ai_actual_sales_mch3 "
            "as aasm ( select cat_mch3_code , sum(actual_sales_qty) as "
            "actual_sales_qty , sum(actual_sales_value) as actual_sales_value "
            ", start_of_month from {database_name}.{ai_schema_name}."
            "ai_actual_sales_article where ( start_of_month between "
            "date_trunc('month', (date '{data_date}') - interval "
            "'{date_range_recheck_month} month')::date and date_trunc('month',"
            " (date '{run_date}' - interval '{date_range_sla_month} "
            "month'))::date ) group by 1,4 ) on conflict ( cat_mch3_code, "
            "start_of_month ) do update set actual_sales_qty = "
            "excluded.actual_sales_qty , actual_sales_value = "
            "excluded.actual_sales_value where ( round(aasm.actual_sales_qty, "
            "3) <> round(excluded.actual_sales_qty, 3) and "
            "excluded.actual_sales_qty > 0 ) or ( "
            "round(aasm.actual_sales_value, 3) <> "
            "round(excluded.actual_sales_value, 3) and "
            "excluded.actual_sales_value > 0 ); "
        )
        result = Table.parse_shortname("aasm")
        respec: dict = {
            "name": "ai_actual_sales_mch3",
            "shortname": "aasm",
            "prefix": "ai",
            "type": "sql",
            "process_max": 1,
            "profile": {
                "features": [
                    {
                        "name": "cat_mch3_code",
                        "datatype": "varchar( 32 )",
                        "nullable": False,
                        "unique": False,
                        "default": None,
                        "check": None,
                        "pk": True,
                        "fk": {},
                    },
                    {
                        "name": "actual_sales_qty",
                        "datatype": "numeric( 24, 8 )",
                        "nullable": True,
                        "unique": False,
                        "default": None,
                        "check": None,
                        "pk": False,
                        "fk": {},
                    },
                    {
                        "name": "actual_sales_value",
                        "datatype": "numeric( 24, 8 )",
                        "nullable": True,
                        "unique": False,
                        "default": None,
                        "check": None,
                        "pk": False,
                        "fk": {},
                    },
                    {
                        "name": "start_of_month",
                        "datatype": "date",
                        "nullable": False,
                        "unique": False,
                        "default": None,
                        "check": None,
                        "pk": True,
                        "fk": {},
                    },
                ],
                "primary_key": ["cat_mch3_code", "start_of_month"],
                "foreign_key": [],
                "partition": {},
            },
            "process": {
                "from_ai_actual_sales_article": {
                    "name": "from_ai_actual_sales_article",
                    "parameter": [
                        "data_date",
                        "date_range_recheck_month",
                        "date_range_sla_month",
                        "run_date",
                    ],
                    "priority": 1,
                    "statement": statement,
                }
            },
            "initial": {},
            "tag": {
                "author": "undefined",
                "description": None,
                "labels": [],
                "version": datetime.date(1970, 1, 1),
                "ts": datetime.datetime(2023, 3, 13, 0, 0),
            },
        }
        self.assertDictEqual(respec, result.dict(by_alias=False))


class TableFrontendValidatorTestCase(unittest.TestCase):
    """Test Case for Table object from validators file."""

    def setUp(self) -> None:
        self.maxDiff = None
        self.input_01: dict = {
            "name": "table_name",
            "profile": {
                "features": {
                    "column_name": {"datatype": "datatype_for_01 not null"},
                    "column_name_2": {"datatype": "int", "nullable": False},
                },
                "primary_key": ["column_name"],
                "partition": {"type": "range", "columns": ["column_name_2"]},
            },
            "process": {
                "process_01": {
                    "parameter": [],
                    "statement": "statement_01",
                }
            },
        }

    @mock.patch("app.core.validators.datetime", warps=datetime.datetime)
    def test_parsing_01_from_object_sql(self, mock_datetime: mock.MagicMock):
        mock_datetime.now.return_value = datetime.datetime(2023, 3, 13, 0, 0)
        result = TableFrontend.parse_obj(self.input_01)
        respec: dict = {
            "name": "table_name",
            "shortname": "tn",
            "prefix": "table",
            "type": "sql",
            "profile": {
                "features": [
                    {
                        "name": "column_name",
                        "datatype": "datatype_for_01",
                        "nullable": False,
                        "unique": False,
                        "default": None,
                        "check": None,
                        "pk": True,
                        "fk": {},
                    },
                    {
                        "name": "column_name_2",
                        "datatype": "int",
                        "nullable": False,
                        "unique": False,
                        "default": None,
                        "check": None,
                        "pk": False,
                        "fk": {},
                    },
                ],
                "primary_key": ["column_name"],
                "foreign_key": [],
                "partition": {
                    "type": "range",
                    "columns": ["column_name_2"],
                },
            },
            "process": {
                "process_01": {
                    "name": "process_01",
                    "parameter": [],
                    "priority": 1,
                    "statement": "statement_01; ",
                }
            },
            "initial": {},
            "catalog": {
                "id": "tn",
                "initial": {},
                "name": "table_name",
                "prefix": "table",
                "process": {
                    "process_01": {
                        "name": "process_01",
                        "parameter": [],
                        "priority": 1,
                        "statement": "statement_01; ",
                    }
                },
                "profile": {
                    "features": [
                        {
                            "check": None,
                            "datatype": "datatype_for_01",
                            "default": None,
                            "fk": {},
                            "name": "column_name",
                            "nullable": False,
                            "pk": True,
                            "unique": False,
                        },
                        {
                            "check": None,
                            "datatype": "int",
                            "default": None,
                            "fk": {},
                            "name": "column_name_2",
                            "nullable": False,
                            "pk": False,
                            "unique": False,
                        },
                    ],
                    "foreign_key": [],
                    "partition": {
                        "columns": ["column_name_2"],
                        "type": "range",
                    },
                    "primary_key": ["column_name"],
                },
                "shortname": "tn",
                "type": "sql",
            },
            "tag": {
                "author": "undefined",
                "description": None,
                "labels": [],
                "version": datetime.date(2023, 3, 13),
                "ts": datetime.datetime(2023, 3, 13, 0, 0),
            },
        }
        print(result.catalog)
        self.assertDictEqual(respec, result.dict(by_alias=False))


class FunctionValidatorTestCase(unittest.TestCase):
    """Test Case for Function object from validators file."""

    def setUp(self) -> None: ...

    def test_parsing_01_from_object(self): ...


class PipelineValidatorTestCase(unittest.TestCase):
    """Test Case for Pipeline object from validators file."""

    def setUp(self) -> None: ...

    def test_parsing_01_from_object(self): ...


class TaskValidatorTestCase(unittest.TestCase):
    """Test Case for Task model from validators file."""

    def setUp(self) -> None:
        """Set up input attributes for parsing to the Task model."""
        self.maxDiff = None
        self.input_01: dict = {
            "module": "data",
            "parameters": {"table_name": "table_01"},
            "mode": TaskMode.BACKGROUND,
            "component": TaskComponent.FRAMEWORK,
        }
        self.input_02: dict = {
            "module": "demo",
            "parameters": {"dates": ["2023-01-01", "2023-02-01"]},
            "mode": TaskMode.BACKGROUND,
            "component": TaskComponent.FRAMEWORK,
        }
        self.input_03: dict = {
            "module": "payload",
            "parameters": {
                "tbl_name_short": "immsl",
                "table_name": "imp_min_max_service_level",
                "run_date": "2023-06-02",
                "update_date": "2022-06-22 10:11:00",
                "ingest_action": "update",
                "ingest_mode": "common",
                "background": "N",
                "payloads": {
                    "class_a": 0.96,
                    "class_b": 0.91,
                    "class_c": 0.86,
                },
            },
            "mode": TaskMode.FOREGROUND,
            "component": TaskComponent.INGESTION,
        }

    @mock.patch("app.core.base.datetime", warps=datetime.datetime)
    def test_parsing_01_from_object(self, mock_datetime: mock.MagicMock):
        mock_datetime.now.return_value = datetime.datetime(2023, 3, 13, 0, 0)
        respec = {
            "id": "20230313000000000022752001",
            "message": "",
            "module": "data",
            "parameters": {
                "cascade": False,
                "dates": ["2023-03-13"],
                "drop_schema": False,
                "drop_table": False,
                "mode": "common",
                "name": "table_01",
                "others": {},
                "type": "table",
            },
            "mode": TaskMode.BACKGROUND.value,
            "component": TaskComponent.FRAMEWORK.value,
            "release": {"date": None, "index": None, "pushed": False},
            "start_time": datetime.datetime(2023, 3, 13, 0, 0),
            "status": Status.WAITING.value,
        }
        result: Task = Task.parse_obj(self.input_01)
        print(result.dict(by_alias=False))
        self.assertDictEqual(respec, result.dict(by_alias=False))

    @mock.patch("app.core.base.datetime", warps=datetime.datetime)
    def test_parsing_02_from_object(self, mock_datetime: mock.MagicMock):
        mock_datetime.now.return_value = datetime.datetime(2023, 3, 13, 0, 0)
        respec = {
            "id": "20230313000000000009954672",
            "module": "demo",
            "parameters": {
                "type": "undefined",
                "name": "demo",
                "dates": ["2023-01-01", "2023-02-01"],
                "mode": "common",
                "drop_table": False,
                "drop_schema": False,
                "cascade": False,
                "others": {},
            },
            "mode": TaskMode.BACKGROUND.value,
            "component": TaskComponent.FRAMEWORK.value,
            "status": Status.WAITING.value,
            "message": "",
            "release": {"date": None, "index": None, "pushed": False},
            "start_time": datetime.datetime(2023, 3, 13, 0, 0),
        }
        result: Task = Task.parse_obj(self.input_02)
        print(result.dict(by_alias=False))
        self.assertDictEqual(respec, result.dict(by_alias=False))

    @mock.patch("app.core.base.datetime", warps=datetime.datetime)
    def test_parsing_03_from_object(self, mock_datetime: mock.MagicMock):
        mock_datetime.now.return_value = datetime.datetime(2023, 3, 13, 0, 0)
        respec = {
            "id": "20230313000000000040451751",
            "module": "payload",
            "parameters": {
                "type": "table",
                "name": "imp_min_max_service_level",
                "dates": ["2023-06-02"],
                "mode": "common",
                "drop_table": False,
                "drop_schema": False,
                "cascade": False,
                "others": {
                    "background": "N",
                    "ingest_action": "update",
                    "ingest_mode": "common",
                    "payloads": {
                        "class_a": 0.96,
                        "class_b": 0.91,
                        "class_c": 0.86,
                    },
                    "tbl_name_short": "immsl",
                    "update_date": "2022-06-22 10:11:00",
                },
            },
            "mode": TaskMode.FOREGROUND.value,
            "component": TaskComponent.INGESTION.value,
            "status": Status.WAITING.value,
            "message": "",
            "start_time": datetime.datetime(2023, 3, 13, 0, 0),
            "release": {"date": None, "index": None, "pushed": False},
        }
        result: Task = Task.parse_obj(self.input_03)
        print(result.dict(by_alias=False))
        self.assertDictEqual(respec, result.dict(by_alias=False))

    @mock.patch("app.core.base.datetime", warps=datetime.datetime)
    def test_runner_02(self, mock_datetime: mock.MagicMock):
        mock_datetime.now.return_value = datetime.datetime(2023, 3, 13, 0, 0)
        result: Task = Task.parse_obj(self.input_02)
        for idx, run_date in result.runner(start=0):
            print(idx, run_date)
            print(result.message)
            print(result.release)
        print(result.release)

    @mock.patch("app.core.base.datetime", warps=datetime.datetime)
    def test_parsing_from_make(self, mock_datetime: mock.MagicMock):
        mock_datetime.now.return_value = datetime.datetime(2023, 3, 13, 0, 0)
        respec = {
            "id": "20230313000000000063979606",
            "module": "test",
            "parameters": {
                "type": "undefined",
                "name": "test",
                "dates": ["2023-03-13"],
                "mode": "common",
                "drop_table": False,
                "drop_schema": False,
                "cascade": False,
                "others": {},
            },
            "mode": TaskMode.FOREGROUND.value,
            "component": TaskComponent.UNDEFINED.value,
            "status": Status.WAITING.value,
            "message": "",
            "start_time": datetime.datetime(2023, 3, 13, 0, 0),
            "release": {
                "date": None,
                "index": None,
                "pushed": False,
            },
        }
        result: Task = Task.make(module="test")
        self.assertDictEqual(respec, result.dict(by_alias=False))
