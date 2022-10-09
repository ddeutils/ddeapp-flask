API Scenarios Handler
=====================

Document for how to use API requests by scenarios with AI Framework Application

#### Modules
- [Run Setup](#run-setup)
- [Run Normal](#run-normal)
- [Run Retention](#run-retention)
- [Ingestion Normal](#ingestion-normal)
- [Monitoring Process](#monitoring-process)
- [Dependencies](#dependencies) (_pending_)

Check Application Response
--------------------------

The first thing you should do after running the app is to perform a health check

*without `APIKEY`*
```shell
curl --location --request GET 'http://localhost:5000/'
# {'message': "Start: Application was running ..."}
```

*or included `APIKEY`*
```shell
curl --location --request GET 'http://localhost:5000/apikey' \
--header 'APIKEY: <api-key-in-env>'
```
- if `APIKEY` does not match with config,
```json
{
  "message":"Error: Unauthorised with 'APIKEY'"
}
```
- if `APIKEY` is match or does not set config parameter, which mean `APIKEY == None`,
```json
{
  "message":"Success: The AI app was running ..."
}
```

Run Setup
---------

#### *Input parameter*s
```python
"""
run_date: 'yyyy-mm-dd': default `today()` : "set run_date in ctr_data_pipeline after create"
table_name: (optional) : "specific table want to setup"
initial_data: ['Y', 'N', 'A', 'S', 'I'] : default 'N' : "Excecute the initial statement after create"
  'Y' : "all table in default pipeline"
  'A' : "only 'ai_' table prefix in default pipeline"
  'S' : "only 'src_' table prefix in default pipeline"
  'I' : "only 'imp_' table prefix in default pipeline"
drop_before_create: ['Y', 'N', 'C', 'A', 'S', 'I'] : default 'N' : "Drop table before create"
  'Y' : "all table in default pipeline"
  'C' : "only 'ctr_' table prefix in default pipeline"
  'A' : "only 'ai_' table prefix in default pipeline"
  'S' : "only 'src_' table prefix in default pipeline"
  'I' : "only 'imp_' table prefix in default pipeline"
drop_table: ['Y', 'N'] : default 'N' : "Force drop table in AI schema"
drop_scheme: ['Y', 'N'] : default 'N' : "Force drop AI schema in database"
background: ['Y', 'N'] : default 'Y': "Run with background task"
"""
```
> Note: If `'drop_schema' == 'Y'` or `'drop_table' == 'Y'` then this module will ignore `run_date`, `drop_before_create` and `initial_data` parameters

#### *Example*
- Initial setup for first time deployment with default pipeline, `pipeline_initial_setup`
```shell
curl --location --request POST 'http://localhost:5000/api/ai/run/setup' \
--header 'APIKEY: <api-key-in-env>' \
--form 'run_date="2019-01-01"' \
--form 'initial_data="Y"' \
--form 'drop_before_create="N"'
```

- Reset current and re-setup some table, such as `ai_article_master`
```shell
curl --location --request POST 'http://localhost:5000/api/ai/run/setup' \
--header 'APIKEY: <api-key-in-env>' \
--form 'table_name="ai_article_master"' \
--form 'run_date="2019-01-01"' \
--form 'initial_data="A"' \
--form 'drop_before_create="Y"'
```

- Drop all tables in AI schema
```shell
curl --location --request POST 'http://localhost:5000/api/ai/run/setup' \
--header 'APIKEY: <api-key-in-env>' \
--form 'drop_table="Y"' \
--form 'drop_scheme="N"'
```
> Note: the AI schema name that was set in environment variable `AI_SCHEMA`, default name is `ai`

- Drop all tables in AI schema and finally drop AI schema
```shell
curl --location --request POST 'http://localhost:5000/api/ai/run/setup' \
--header 'APIKEY: <api-key-in-env>' \
--form 'drop_table="Y"' \
--form 'drop_scheme="Y"'
```

- Drop only AI schema that do not have any dependencies
```shell
curl --location --request POST 'http://localhost:5000/api/ai/run/setup' \
--header 'APIKEY: <api-key-in-env>' \
--form 'drop_table="N"' \
--form 'drop_scheme="Y"'
```
> Note: If the schema has a dependencies but still wants to delete it, then set `permission_force_delete` parameter in `ctr_data_parameter` from `N` to `Y`

Run Normal
----------

#### *Input parameter*s
```python
"""
run_date: yyyy-mm-dd: default `today()` : "The date want to run"
pipeline_name: (optional) : "Specific pipeline name want to run"
table_name: (optional) : "Specific table want to run"
run_mode: ['common', 'rerun'] : default 'common' : "Mode want to run"
background: ['Y', 'N'] : default 'Y': "Run with background task"
"""
```
> Note: `pipeline_name` and `table_name` must exist only one in input parameter

#### *Example*

- Run with mode `common` and `pipeline_name` is `pipeline_order_by_phase`
```shell
curl --location --request POST 'http://localhost:5000/api/ai/run/data' \
--header 'APIKEY: <api-key-in-env>' \
--form 'pipeline_name="pipeline_order_by_phase"' \
--form 'run_date="2019-01-01"' \
--form 'run_mode="common"'
```

- Run with mode `common` and `table_name` is `ai_article_master`
```shell
curl --location --request POST 'http://localhost:5000/api/ai/run/data' \
--header 'APIKEY: <api-key-in-env>' \
--form 'table_name="sql:ai_article_master"' \
--form 'run_date="2019-01-01"' \
--form 'run_mode="common"'
```
> Note: When using `table_name` to run, the process table type prefix, such as `sql` or `func`, should be added before the `table_name` and intermediate with a `:`. The default will be `sql`, if it does not add.

- Run with mode `rerun` and `pipeline_name` is `pipeline_order_by_phase`
```shell
curl --location --request POST 'http://localhost:5000/api/ai/run/data' \
--header 'APIKEY: <api-key-in-env>' \
--form 'pipeline_name="pipeline_order_by_phase"' \
--form 'run_date="2019-01-01"' \
--form 'run_mode="rerun"'
```

- Run with mode `rerun` and `table_name` is `ai_article_master`
```shell
curl --location --request POST 'http://localhost:5000/api/ai/run/data' \
--header 'APIKEY: <api-key-in-env>' \
--form 'table_name="sql:ai_article_master"' \
--form 'run_date="2019-01-01"' \
--form 'run_mode="rerun"'
```

Run Retention
------------

#### *Input parameter*
```python
"""
run_date: 'yyyy-mm-dd': default `today()` : "The date want to run data retention mode"
table_name: (optional) : "Specific table name want to run"
backup_table: (optional) : "Backup data in current AI schema to new schema"
backup_schema: (optional) : "Backup the table in current AI schema to new schema"
background: ['Y', 'N'] : default 'Y': "Run with background task"
"""
```
#### *Example*

- Run data retention with all table in default pipeline, `pipeline_initial_retention`
```shell
curl --location --request POST 'http://localhost:5000/api/ai/run/retention' \
--header 'APIKEY: <api-key-in-env>' \
--form 'run_date="2019-01-01"'
```
> Note: The data retention have 2 options, `data_date` and `run_date`. By default, it is set to `data_date` with `data_retention_mode` parameter in `ctr_data_parameter` table.

- Run data retention and backup data with same table name to new schema name is `ai_archive`
```shell
curl --location --request POST 'http://localhost:5000/api/ai/run/retention' \
--header 'APIKEY: <api-key-in-env>' \
--form 'run_date="2019-01-01"' \
--form 'backup_schema="ai_archive"'
```

- Run data retention with some table, such as `ai_article_master` and backup to new table name in current schema
```shell
curl --location --request POST 'http://localhost:5000/api/ai/run/retention' \
--header 'APIKEY: <api-key-in-env>' \
--form 'table_name="ai_article_master"' \
--form 'run_date="2019-01-01"' \
--form 'backup_table="ai_article_master_bk"'
```

- Run data retention with some table, such as `ai_article_master` and backup to new table name in new schema name is `ai_archive`
```shell
curl --location --request POST 'http://localhost:5000/api/ai/run/retention' \
--header 'APIKEY: <api-key-in-env>' \
--form 'table_name="ai_article_master"' \
--form 'run_date="2019-01-01"' \
--form 'backup_table="ai_article_master_bk"' \
--form 'backup_schema="ai_archive"'
```

Ingestion Normal
----------------
#### *Input argument*
```python
"""
tbl_short_name: path: "The short name of target table that want to ingest data"
"""
```

#### *Input parameter*
```python
"""
run_date: yyyy-mm-dd : default `today()` : "The date want to run"
update_date: 'yyyy-mm-dd HH:MM:SS' : default `now()` : "The update datetime that ingest this data"
mode: ['update', insert'] : default 'insert': Mode want to action in target table
ingest_mode: ['common', 'merge'] : default 'common' : "Mode want to ingest"
background: ['Y', 'N'] : default 'Y' : "Run with background task"
data: Union[list, dict] : "data for ingest to target table. `not null` property should exists in data and `default` or `serial` property should not exists"
"""
```
#### *Example*

- Ingest data to table `imp_lead_time_inventory_cap_rdc` (`ilticr`) with `common` ingest mode
```shell
curl --location --request POST 'http://localhost:5000/api/ai/put/ilticr' \
--header 'APIKEY: <api-key-in-env>' \
--header 'Content-Type: application/json' \
--data-raw '{
    "background": "N",
    "ingest_mode": "common",
    "update_date": "2021-11-01 02:11:00",
    "data": [
        {
            "dc_code": "8910",
            "dc_name": "DC Korat",
            "rdc_code": "8910",
            "rdc_name": "RDC Korat",
            "lead_time_rdc": 7,
            "inventory_cap_value_rdc": 500000
        },
        {
            "dc_code": "8910",
            "dc_name": "DC Korat",
            "rdc_code": "8920",
            "rdc_name": "RDC BKK",
            "lead_time_rdc": 10,
            "inventory_cap_value_rdc": 100000
        },
        {
            "dc_code": "8920",
            "dc_name": "DC BKK",
            "rdc_code": "8930",
            "rdc_name": "RDC BKK",
            "lead_time_rdc": 5,
            "inventory_cap_value_rdc": 2000000
        }
    ]
}'
```

- Ingest data to table `imp_lead_time_inventory_cap_rdc` (`ilticr`) with `merge` ingest mode
```shell
curl --location --request POST 'http://localhost:5000/api/ai/put/ilticr' \
--header 'APIKEY: <api-key-in-env>' \
--header 'Content-Type: application/json' \
--data-raw '{
    "background": "N",
    "ingest_mode": "merge",
    "update_date": "2021-11-01 02:11:00",
    "data": [
        {
            "dc_code": "8910",
            "dc_name": "DC Korat",
            "data_merge": [
                {
                    "rdc_code": "8910",
                    "rdc_name": "RDC Korat",
                    "lead_time_rdc": 7,
                    "inventory_cap_value_rdc": 500000
                },
                {
                    "rdc_code": "8920",
                    "rdc_name": "RDC BKK",
                    "lead_time_rdc": 10,
                    "inventory_cap_value_rdc": 100000
                }
            ]
        },
        {
            "dc_code": "8920",
            "dc_name": "DC BKK",
            "data_merge": {
                "rdc_code": "8930",
                "rdc_name": "RDC BKK",
                "lead_time_rdc": 5,
                "inventory_cap_value_rdc": 2000000
            }
        }
    ]
}'
```

- Ingest data to table `imp_lead_time_inventory_cap_rdc` (`ilticr`) with `common` ingest mode and `update` mode
```shell
curl --location --request POST 'http://localhost:5000/api/ai/put/ilticr' \
--header 'APIKEY: <api-key-in-env>' \
--header 'Content-Type: application/json' \
--data-raw '{
    "background": "N",
    "mode": "update",
    "ingest_mode": "common",
    "update_date": "2021-11-01 02:11:00",
    "data": {
            "dc_code": "8910",
            "dc_name": "DC Korat",
            "rdc_code": "8910",
            "rdc_name": "RDC Korat",
            "lead_time_rdc": 5
    }
}'
```

Monitoring Process
------------------

#### *Input argument*
```python
"""
process_id: int: "The process_id that returning after request to run data or ingestion"
"""
```

#### *Example*

- Get status of process after Run Data or Ingestion, if `process_id` from response is `20220219205753523893063771`
```shell
curl --location --request GET 'http://localhost:5000/api/ai/get/opt/20220219205753523893063771' \
--header 'APIKEY: <api-key-in-env>'
```

Dependencies
------------

#### *Input argument*
```python
"""
tbl_short_name: path: "The short name of target table that want to get dependencies"
"""
```

#### *Example*

- Get dependencies of table `ai_article_master` (`aam`)
```shell
curl --location --request GET 'http://localhost:5000/api/ai/get/dpc/aam' \
--header 'APIKEY: <api-key-in-env>'
```