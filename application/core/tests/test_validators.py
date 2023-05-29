import unittest
from unittest import mock
import datetime
from pydantic import ValidationError
from application.core.validators import (
    Column,
    Profile,
    Table,
)


class ColumnValidatorTestCase(unittest.TestCase):
    """Test Case for Column object from validators file"""

    def setUp(self) -> None:
        """Set up input attributes for parsing to the Column model
        """
        self.maxDiff = None
        self.input_01: dict = {
            'name': 'column_name',
            'datatype': 'varchar( 128 ) not null unique primary key check(column_name <> \'DEMO\')'
        }
        self.input_02: dict = {
            'name': 'column_name',
            'datatype': 'varchar( 128 ) not null unique',
            'pk': True,
            'check': 'check(column_name <> \'DEMO\')',
        }
        self.input_default: dict = {
            'name': 'default',
            'datatype': 'bigint'
        }

    def tearDown(self) -> None:
        ...

    def test_parsing_01_from_object(self):
        respec: dict = {
            'name': 'column_name',
            'datatype': 'varchar( 128 )',
            'nullable': False,
            'unique': True,
            'default': None,
            'check': 'check(column_name <> \'DEMO\')',
            'pk': True,
            'fk': {},
        }
        result: Column = Column.parse_obj(self.input_01)
        self.assertDictEqual(respec, result.dict(by_alias=False))

    def test_parsing_02_from_object(self):
        respec: dict = {
            'name': 'column_name',
            'datatype': 'varchar( 128 )',
            'nullable': False,
            'unique': True,
            'default': None,
            'check': 'check(column_name <> \'DEMO\')',
            'pk': True,
            'fk': {},
        }
        result: Column = Column.parse_obj(self.input_02)
        self.assertDictEqual(respec, result.dict(by_alias=False))

    def test_compare_01_and_02(self):
        _result_01: Column = Column.parse_obj(self.input_01)
        _result_02: Column = Column.parse_obj(self.input_02)
        self.assertTrue(_result_01 == _result_02)

    def test_raise_with_null(self):
        with self.assertRaises(ValidationError) as context:
            Column.parse_obj({})
        error_wrapper: ValidationError = context.exception
        errors: list = error_wrapper.errors()
        print(errors)
        self.assertTrue(len(errors) == 1)
        self.assertEqual('__root__', errors[0]['loc'][0])
        self.assertTrue('datatype does not contain in values' in errors[0]['msg'])

    def test_raise_with_no_datatype(self):
        with self.assertRaises(ValidationError) as context:
            Column.parse_obj({'name': 'no_datatype'})
        error_wrapper: ValidationError = context.exception
        errors: list = error_wrapper.errors()
        self.assertTrue(len(errors) == 1)
        self.assertEqual('__root__', errors[0]['loc'][0])
        self.assertTrue('datatype does not contain in values' in errors[0]['msg'])


class ProfileValidatorTestCase(unittest.TestCase):
    """Test Case for Column object from validators file"""

    def setUp(self) -> None:
        self.maxDiff = None
        self.input_01: dict = {
            'features': {
                1: {
                    'name': 'column_name',
                    'datatype': 'datatype_for_01 not null'
                }
            },
            'primary_key': ['column_name'],
            'foreign_key': {
                "column_name": "other( column_name )"
            }
        }
        self.input_02: dict = {
            'features': [
                {
                    'name': 'column_name',
                    'datatype': 'datatype_for_01 not null'
                }
            ],
            'primary_key': ['column_name'],
            'foreign_key': {
                "column_name": {
                    'ref_table': 'other',
                    'ref_column': 'column_name',
                }
            }
        }
        self.input_03: dict = {
            'features': {
                'column_name': {
                    'datatype': 'datatype_for_01 not null'
                },
                'column_name_2': {
                    'datatype': 'int',
                    'nullable': False
                }
            },
            'primary_key': ['column_name'],
            'partition': {
                'type': 'range',
                'columns': ['column_name_2']
            }
        }

    def tearDown(self) -> None:
        ...

    def test_parsing_01_from_object(self):
        result = Profile.parse_obj(self.input_01)
        respec: dict = {
            'features': [
                {
                    'name': 'column_name',
                    'datatype': 'datatype_for_01',
                    'nullable': False,
                    'default': None,
                    'unique': False,
                    'check': None,
                    'pk': True,
                    'fk': {
                        'table': 'other',
                        'column': 'column_name',
                    },
                }
            ],
            'primary_key': ['column_name'],
            'foreign_key': [
                {'name': 'column_name', 'ref_table': 'other', 'ref_column': 'column_name'}
            ],
            'partition': {}
        }
        print(result.dict(by_alias=False))
        self.assertDictEqual(respec, result.dict(by_alias=False))

    def test_parsing_02_from_object(self):
        result = Profile.parse_obj(self.input_02)
        respec: dict = {
            'features': [
                {
                    'name': 'column_name',
                    'datatype': 'datatype_for_01',
                    'nullable': False,
                    'default': None,
                    'unique': False,
                    'check': None,
                    'pk': True,
                    'fk': {
                        'table': 'other',
                        'column': 'column_name',
                    },
                }
            ],
            'primary_key': ['column_name'],
            'foreign_key': [
                {'name': 'column_name', 'ref_table': 'other', 'ref_column': 'column_name'}
            ],
            'partition': {}
        }
        print(result.dict(by_alias=False))
        self.assertDictEqual(respec, result.dict(by_alias=False))

    def test_parsing_03_from_object(self):
        result = Profile.parse_obj(self.input_03)
        respec: dict = {
            'features': [
                {
                    'name': 'column_name',
                    'datatype': 'datatype_for_01',
                    'nullable': False,
                    'default': None,
                    'unique': False,
                    'check': None,
                    'pk': True,
                    'fk': {},
                },
                {
                    'name': 'column_name_2',
                    'datatype': 'int',
                    'nullable': False,
                    'default': None,
                    'unique': False,
                    'check': None,
                    'pk': False,
                    'fk': {},
                }
            ],
            'primary_key': ['column_name'],
            'foreign_key': [],
            'partition': {
                'type': 'range',
                'columns': ['column_name_2']
            }
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
            'features': [],
            'primary_key': [],
            'foreign_key': [],
            'partition': {}
        }
        print(result.dict(by_alias=False))
        self.assertDictEqual(respec, result.dict(by_alias=False))

    def test_raise_with_primary_key_only(self):
        with self.assertRaises(ValidationError) as context:
            Profile.parse_obj({
                'primary_key': ['column_name']
            })
        error_wrapper: ValidationError = context.exception
        errors: list = error_wrapper.errors()
        print(errors)
        self.assertTrue(len(errors) == 1)
        self.assertEqual('primary_key', errors[0]['loc'][0])
        self.assertEqual('value_error', errors[0]['type'])
        self.assertTrue('column_name in primary key does not exists in features' in errors[0]['msg'])

    def test_raise_with_partition_column(self):
        ...


class TableValidatorTestCase(unittest.TestCase):
    """Test Case for Table object from validators file"""
    def setUp(self) -> None:
        self.maxDiff = None
        self.input_01: dict = {
            'name': 'table_name',
            'profile': {
                'features': {
                    'column_name': {
                        'datatype': 'datatype_for_01 not null'
                    },
                    'column_name_2': {
                        'datatype': 'int',
                        'nullable': False
                    }
                },
                'primary_key': ['column_name'],
                'partition': {
                    'type': 'range',
                    'columns': ['column_name_2']
                }
            },
            'process': {
                'process_01': {
                    'parameter': [],
                    'statement': 'statement_01',
                }
            }
        }

    # docs: https://stackoverflow.com/questions/4481954/trying-to-mock-datetime-date-today-but-not-working
    @mock.patch("application.core.validators.datetime", warps=datetime.datetime)
    def test_parsing_01_from_object_sql(
            self,
            mock_datetime: mock.MagicMock
    ):
        mock_datetime.now.return_value = datetime.datetime(2023, 3, 13, 0, 0)
        result = Table.parse_obj(self.input_01)
        respec: dict = {
            'name': 'table_name',
            'shortname': 'tn',
            'prefix': 'table',
            'type': 'sql',
            'description': None,
            'profile': {
                'features': [
                    {
                        'name': 'column_name',
                        'datatype': 'datatype_for_01',
                        'nullable': False,
                        'unique': False,
                        'default': None,
                        'check': None,
                        'pk': True,
                        'fk': {},
                    },
                    {
                        'name': 'column_name_2',
                        'datatype': 'int',
                        'nullable': False,
                        'unique': False,
                        'default': None,
                        'check': None,
                        'pk': False,
                        'fk': {},
                    }
                ],
                'primary_key': ['column_name'], 'foreign_key': [],
                'partition': {
                    'type': 'range',
                    'columns': ['column_name_2'],
                }
            },
            'process': {
                'process_01': {
                    'name': 'process_01',
                    'parameter': [],
                    'priority': 1,
                    'statement': 'statement_01; '
                }
            },
            'initial': {},
            'tag': {
                'author': 'undefined',
                'labels': [],
                'version': datetime.date(2023, 3, 13),
                'ts': datetime.datetime(2023, 3, 13, 0, 0)
            }
        }
        print(result.dict(by_alias=False))
        self.assertDictEqual(respec, result.dict(by_alias=False))

    def test_parsing_02_from_object_sql(self):
        ...

    def test_parsing_02_from_object_py(self):
        ...


class FunctionValidatorTestCase(unittest.TestCase):
    """Test Case for Function object from validators file"""
    def setUp(self) -> None:
        ...

    def test_parsing_01_from_object(self):
        ...


class PipelineValidatorTestCase(unittest.TestCase):
    """Test Case for Pipeline object from validators file"""
    ...
