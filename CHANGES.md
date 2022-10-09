Changelog
=========


Version 1.0.0
-------------

> Pending

* [ ] Changing base code from `Flask` to `FastAPI` and use `Alembic` and `SQLAlchemy`/`Tortoise` 
  for database migration and database ORM
* [ ] Changing `psycogpg2` to `psycogpg3` (alias name is `psycopg`)


Version 0.1.0.b
---------------

> Pending

* [ ] Adding async function with Quart (It should be possible to migrate to Quart from Flask)
    - Quart has `add_background_task` method (migrate from `threading`)
    - Support `asyncpg`

---

Version 0.0.1
-------------

* Compare `Dataclasses` vs `Attrs` vs `Pydantic`

* [ ] Fixing #? change transaction table to **partition table
* [ ] Adding validate data view in database
* [ ] Add `Pydantic` for type validator and receive json data
* [ ] Adding swagger for API document
* [ ] Adding Flask Extensions for control I/O if application
* [ ] Adding unittest
* [x] Adding Migrate table properties process
* [x] Adding Load file from local to target table 
* [x] Change app framework from function to class instance framework code

*Note: `Version 0.*.*` still support requirement from `pre-version 1.*.*` and `pre-Version 2.*.*`*

---

Pre Version 2.1.0
-----------------
* [ ] Fixing #? Ingestion merge `PUT` and `DELETE` methods together with same endpoint
  * [ ] Adding Ingestion Module for delete data from platform with payload
* [ ] Change physical of transaction table from default to partition by range of date
* [ ] Change controlling thread for run data pipeline
* [ ] Add adjustment flow for `imp_mix_max_mos_criteria_fc/rdc/dc` values

---

Pre Version 2.0.4
-----------------

* [x] Add new table (`ai_report_forecast_brand`) for sync data to forecast adjust dashboard
* [x] Add new table (`ai_report_article_listing_master`) for channel value mapping to platform
* [x] Fixing #? condition for rounding `max_qty` value to integer type
* [x] Change adjustment logic for product class and min/max value
* [x] Adding `ctr_task_schedule` for control trigger for the data pipeline running
    * [x] Add schedule, `trigger`, for run data pipeline from updated S3 files trigger
    * [x] Add schedule, `cron job`, for run data pipeline with manual value
* [x] Adding ingest action mode, `update`, for update data to target table
* [x] Adding `ctr_s3_logging` for keep log from AWS Batch Job which ingest data from S3 to Database
  with `src_` prefix tables
* [x] Fixing #? logic of calculation rolling data for product class
* [x] Fixing #? logic of filter condition data for min max
* [x] Adding `ai_article_vendor_master`
* [x] Adding filter `cat_mch3_code` from source file to article master table
* [x] Adding refresh table (`vacuum`) process in retention module


Pre Version 2.0.3
-----------------
* [x] Add new source file from AWS S3 (`sales_order`)
* [x] Re-structure of `actual_sales_transaction`
* [x] Add new field for keep frequency for `product_class_freq`
* [x] Change logic for get default `product_class` with `product_class_freq` only
* [x] Change logic of min/max value with custom Month Of Supply (MOS) such as `{A: 2, B: 2, C: 1}`
  receive with `imp_min_max_mos_adjust_fc/rdc/dc` tables
* [x] Change `table_type` of replenishment tables from `transaction` to `report`


Pre Version 2.0.2
-----------------
* [x] Adding Flask-APSchedule for re-create tables, which solving table storage
* [x] Adding Ingestion Module for put data from platform in merge case
* [x] Adding Analytic a table dependency from catalog config
* [x] Change data type from `double precision` to `numeric`


Pre Version 2.0.1
-----------------
* [x] Adding Flask-APSchedule for control retention module and task checker
* [x] Adding Ingestion Module for put data from platform in common case
    * Adding `GET` request for check background task heartbeat after ingest data
* [x] Adding RDC hierarchy level between DC and Franchise
* [x] Change catalog config format from create statement to mapping features
* [x] Fixing #? adjustment and article replacement logic
* [x] Fixing #? allocate, proportion, and accumulate logic

Pre Version 2.0.0
-----------------

* [x] Adding `GET` request for check background task heartbeat after run data pipeline
* [x] Fixing #? the normal flow was changed to foreground task function
* [x] Adding support background task
* [x] Adding support task monitoring with logging/process table in database
* [x] Adding component which keep framework and analytic routes

*Note: pre-version from `1.x.x` to `2.x.x` was change application file skeleton*

---

Pre Version 1.1.1
-----------------

* [x] Adding `close_running` function for server down scenario
* [x] Adding `GET` request for shutdown framework application
* [x] Fixing #? `GET` request for health check without and include headers
* [x] Adding logging handler
* [x] Fixing #? config `*.yaml` file can keep more than one table process
* [x] Fixing #? order quantity report logic

Pre Version 1.1.0
-----------------

* [x] Adding support filter process name in node function
* [x] Adding support pipeline function for run data with multi-table processes

Pre Version 1.0.0
-----------------

* [x] Initial release
* [x] Original creation, `2021-10-01`