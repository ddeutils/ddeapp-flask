import os
import yaml
import unittest
import application.utils.config as conf


parameters: dict = {
    "list_tbl_priority": ['ctr', 'src', 'ai', 'imp', 'plt'],
    "map_tbl_ps_sla": {
        "daily": 'date_range_sla_day'
    },
    "bs_stm": {
        "drop": {
            "tbl": "drop table if exists {{database_name}}.{{ai_schema_name}}.{table_name} {cascade}"
        }
    }
}

environment: str = """
APIKEY='testpass'
DB_HOST="host-name"
DB_PASS="password"
DB_PORT=5432
DB_USER=user-name
DB_NAME=database-name
"""


class ConfigTestCase(unittest.TestCase):
    def setUp(self) -> None:
        ...

    def tearDown(self) -> None:
        ...

    def test_dummy(self):
        ...
