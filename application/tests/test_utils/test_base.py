import os
import yaml
import unittest
import application.utils.base as base


catalog_demo = {
    "version": "2022-01-01",
    "create": {
        "features": {
            "id": "serial",
            "name": "varchar( 128 ) null",
            "register_date": "date not null"
        },
        "primary_key": ["id"]
    },
    "update": {
        "process_num_01": {
            "parameter": ["parameter01"],
            "statement": "statement"
        },
        "process_num_02": {
            "parameter": ["parameter02"],
            "statements": {
                "with_temp_table": "",
                "with_row_table": ""
            }
        }
    }
}


class BaseTestCase(unittest.TestCase):
    def setUp(self) -> None:
        ...

    def tearDown(self) -> None:
        ...

    def test_dummy(self):
        ...
