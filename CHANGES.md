# Changelog

## version 2.0.0

* [ ] Clear all Flask extensions and implement only the code without any external packages


## Version 1.0.0

> Pending

* [ ] Changing base code from `Flask` to `FastAPI` and use `Alembic` and `SQLAlchemy`/`Tortoise` 
  for database migration and database ORM
* [ ] Changing `psycogpg2` to `psycogpg3` (alias name is `psycopg`)


## Version 0.2.0.b

> Pending

* [ ] Adding async function with Quart (It should be possible to migrate to Quart from Flask)
    - Quart has `add_background_task` method (migrate from `threading`)
    - Support `asyncpg`
* [ ] Adding another database types like SQLite, MySQL, or SQL Server

---

## Version 0.2.0

* [ ] Reduce extension code which does not use or useless
    * [ ] Reduce static file
    * [ ] Reduce Flask's extension packages
  
* [ ] Design Pages and Capabilities flexible.

* [ ] Fixing #? change transaction table to **partition table
* [ ] Adding Data Quality Views in Database
* [ ] Add `Pydantic` for type validator and receive JSON data
* [ ] Adding swagger for API documents
* [ ] Adding Flask Extensions for control I/O if application
* [ ] Change physical of transaction table from default to partition by range of date
* [ ] Change controlling thread for run data pipeline to `Flask-Executor`
* [ ] Add adjustment flow for `imp_mix_max_mos_criteria_fc/rdc/dc` values

* [ ] Add Pull and Push Models on Control Object for any control tables
* [ ] Change the `objects` file from legacy slot classes to Pydantic class supported
* [ ] Change trigger value from `list` to `tuple` object

---

* [ ] Change swagger from `flasgger` to `flask-swagger-ui` 
* [ ] Adding and Fixing #? Ingestion
  * [ ] Fix merge `PUT` and `DELETE` methods together with same endpoint
  * [ ] Add module for delete data from platform with payload
  * [x] Add updatable merge from payload
  * [x] Add updatable common from payload
* [x] Change level of `utils` directory to inside `core` directory
* [x] Adding unittests for `core` directory
    * [x] Add `validators` test
    * [x] Add `statements` test
* [x] Documents rename from `DAF` to `DFA`

## Version 0.1.0

* [x] Adding unittests
    * [x] Add to `core` directory
* [x] Fixing #? bug of re-create function when start this application at the first time
* [x] Compare `Dataclasses` vs `Attrs` vs `Pydantic`
    * [x] Change the `base` file from legacy slot classes to Pydantic models
      * Change base file to validators and statements files
      * Add load object for loading `yaml` file
    * [x] Add status Enum object

## Version 0.0.1

* [x] Adding Migrate table properties process
* [x] Adding Load file from local to target table 
* [x] Change app framework from function to class instance framework code

* [x] Add frontend for monitoring and operation data pipelines
    * [x] Add Home page
    * [x] Adding Flask-Login for login and register process    
    * [x] Add Login and Register page
  
* [x] Adding Web catalogs for initialize authentication process
    * [x] Add User registration    
    * [x] Add Role assignment
    * [x] Add Group assignment
    * [x] Add Policy assignment
    * [x] Mapping all relation together

> **Note**: \
> The `Version 0.*.*` still support requirement from `pre-version 1.*.*` and
> `pre-Version 2.*.*`.\
> We can deploy only the backend mode (API mode) by this command; `python manage.py --api=True`.

---

## Pre Version 2.1.0

* [x] Add foreign key feature in table configuration
* [x] Adding initial data with Json files with key `file` or `files`

## Pre Version 2.0.4

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

## Pre Version 2.0.3

* [x] Add new source file from AWS S3 (`sales_order`)
* [x] Re-structure of `actual_sales_transaction`
* [x] Add new field for keep frequency for `product_class_freq`
* [x] Change logic for get default `product_class` with `product_class_freq` only
* [x] Change logic of min/max value with custom Month Of Supply (MOS) such as `{A: 2, B: 2, C: 1}`
  receive with `imp_min_max_mos_adjust_fc/rdc/dc` tables
* [x] Change `table_type` of replenishment tables from `transaction` to `report`

## Pre Version 2.0.2

* [x] Adding Flask-APSchedule for re-create tables, which solving table storage
* [x] Adding Ingestion Module for put data from platform in merge case
* [x] Adding Analytic a table dependency from catalog config
* [x] Change data type from `double precision` to `numeric`

## Pre Version 2.0.1

* [x] Adding Flask-APSchedule for control retention module and task checker
* [x] Adding Ingestion Module for put data from platform in common case
    * Adding `GET` request for check background task heartbeat after ingest data
* [x] Adding RDC hierarchy level between DC and Franchise
* [x] Change catalog config format from create statement to mapping features
* [x] Fixing #? adjustment and article replacement logic
* [x] Fixing #? allocate, proportion, and accumulate logic

## Pre Version 2.0.0

* [x] Adding `GET` request for check background task heartbeat after run data pipeline
* [x] Fixing #? the normal flow was changed to foreground task function
* [x] Adding support background task
* [x] Adding support task monitoring with logging/process table in database
* [x] Adding component which keep framework and analytic routes

> **Note**: \
> pre-version from `1.x.x` to `2.x.x` was change application file skeleton*

---

## Pre Version 1.1.1

* [x] Adding `close_running` function for server down scenario
* [x] Adding `GET` request for shutdown framework application
* [x] Fixing #? `GET` request for health check without and include headers
* [x] Adding logging handler
* [x] Fixing #? config `*.yaml` file can keep more than one table process
* [x] Fixing #? order quantity report logic

## Pre Version 1.1.0

* [x] Adding support filter process name in node function
* [x] Adding support pipeline function for run data with multi-table processes

## Pre Version 1.0.0

* [x] Initial release
* [x] Original creation, `2021-10-01`
