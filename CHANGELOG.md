# Changelogs

## Latest Changes

## 0.3.0

### :sparkles: Features

- :dart: feat: migrate pipeline objects to service. (_2024-05-11_)
- :dart: feat: migrate legacy node to service without diff check. (_2024-05-11_)
- :dart: feat: migrate backup process to NodeMigrate. (_2024-05-11_)
- :dart: feat: migrate old node initial and rename execute func. (_2024-05-10_)
- :dart: feat: add validate table on target database after do any process. (_2024-05-10_)
- :dart: feat: add auto create table on node init process. (_2024-05-10_)
- :dart: feat: migrate legacy out on ingestion task. (_2024-05-10_)
- :dart: feat: add add external parameters on MapParameterService. (_2024-05-10_)
- :dart: feat: migrate ingest method from legacy and add convertor to core. (_2024-05-10_)
- :dart: feat: migrate Control from legacy successful. (_2024-05-09_)
- :dart: feat: add push and create control data. (_2024-05-09_)
- :dart: feat: add pull control data. (_2024-05-09_)
- :dart: feat: add enter and exit override method to Task. (_2024-05-08_)
- :dart: feat: add test task for create custome task logging to table. (_2024-05-08_)
- :dart: feat: add control statement that will use instead LegacyControl. (_2024-05-08_)
- :dart: feat: add NodeLocal for loading csv file to target database. (_2024-05-07_)

### :black_nib: Code Changes

- :construction: refactored: remove legacy code from main. (_2024-05-11_)
- :construction: refactored: migrated code from legacy Node to core. (_2024-05-10_)
- :construction: refactored: remove Action and Function from legacy. (_2024-05-10_)
- :construction: refactored: refactore code and rewrite return list to iterator. (_2024-05-09_)
- :construction: refactored: remove html code that not uses. (_2024-05-07_)

### :bug: Fix Bugs

- :gear: fixed: auto create from control setup. (_2024-05-11_)
- :gear: fixed: remove condition that filter out column to ingest. (_2024-05-10_)
- :gear: fixed: default passed from serial does not valid value. (_2024-05-10_)
- :gear: fixed: remove merge_dicts from core. (_2024-05-09_)
- :gear: fixed: fix testcase that change statement. (_2024-05-07_)

### :postbox: Dependencies

- :pushpin: deps: remove strenum package and use inherite from str. (_2024-05-08_)

## 0.2.2

### :sparkles: Features

- :dart: feat: add node legacy for initial ingest. (_2024-05-07_)
- :dart: feat: add create statement on modern node service. (_2024-05-07_)
- :dart: feat: add ActionQuery to use instead legacy Action for pushdown query. (_2024-05-07_)
- :dart: feat: add schema setup for target database for the first setup. (_2024-05-06_)

### :black_nib: Code Changes

- :art: style: change code style that exceed limi length. (_2024-05-07_)
- :construction: refactored: add method on services file that will implement. (_2024-05-07_)
- :art: style: add debug text for table control does not exists. (_2024-05-06_)
- :construction: refactored: edit char length in code. (_2024-05-06_)

### :bug: Fix Bugs

- :gear: fixed: fix path of logging utils that does not change from old ver. (_2024-05-07_)
- :gear: fixed: uncomment control tables for frontend. (_2024-05-06_)
- :gear: fixed: fix legacy path that does not match with real path. (_2024-05-06_)

## 0.2.1

### :sparkles: Features

- :dart: feat: add middleware wrapper (_2024-01-13_)
- :dart: feat: add pg_stat_statements to postgres docker compose file (_2024-01-04_)

### :black_nib: Code Changes

- :test_tube: tests: move testcase on any dir to main test dir. (_2024-05-06_)
- :construction: refactored: run pre-commit for all files. (_2024-05-06_)
- :construction: refactored: refactored main app code that raise when start-up. (_2024-05-06_)
- :art: styled: change comment license length line (_2024-01-14_)
- :test_tube: test: add app test case (_2024-01-14_)
- :construction: refactored: change components to blueprints (_2024-01-14_)
- :construction: refactored: change format code with pre-commit (_2024-01-14_)
- :test_tube: test: add test workflow (_2024-01-14_)
- :construction: refactored: change application to app (_2024-01-14_)
- :test_tube: tests: update validators test cases (_2024-01-13_)
- :construction: refactored: upgrade import module that use inside package (_2024-01-13_)
- :test_tube: test: prepare test case (_2024-01-13_)
- :construction: refactored: change infomation of .{demo}.env file (_2023-12-17_)
- :bulb: v0.2.0: delete base from legacy and move objects to legacy (#6) (_2023-06-05_)

### :card_file_box: Documents

- :page_facing_up: docs: update README.md file (_2023-12-17_)

### :bug: Fix Bugs

- :gear: fixed: edit docker compose file for postgres db. (_2024-05-06_)
- :gear: fixed: remove comma on test workflow. (_2024-05-05_)
- :gear: fixed: prepare for run on local (_2023-12-17_)

### :package: Build & Workflow

- :toolbox: build: add env to publish workflow. (_2024-05-06_)
- :toolbox: build: upgrade pip installation with uv on test workflow. (_2024-05-06_)
- :toolbox: build: fixed docker compose raise permission denied. (_2024-05-06_)
- :toolbox: build: add docker provisioning on test workflow. (_2024-05-06_)
- :toolbox: build: update __version__ on init file. (_2024-05-05_)
- :toolbox: build: add deps installation on test workflow. (_2024-05-05_)
- :toolbox: build: change cache type on tests workflow (_2024-01-14_)

### :postbox: Dependencies

- :pushpin: deps: change analytic packages deps on requirement file. (_2024-05-06_)
- :pushpin: deps: update dependencies on pystan. (_2024-05-06_)
- :pushpin: deps: update dependencies and split requirement files for analytic. (_2024-05-05_)
- :pushpin: deps: update flask dependencies that support flask 3.0 (_2024-01-14_)

