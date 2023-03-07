AI API Result Handler
=====================

### Scenarios
- [Complete Process](#complete-process)
- [Rerun Data](#rerun-data)
- [Reach Quota](#reach-quota)
- [Time Out](#application-load-balancer-time-out)
- [Code Error](#code-error)

Complete Process
----------------

---

#### **Example scenario**

_command_
```shell
$ curl --location --request POST 'http://localhost:5000/api/ai/run/data' \
--header 'APIKEY: <api-key-in-env>' \
--form 'table_name="sql:ai_forecast_mch3"' \
--form 'run_date="2021-01-01"' \
--form 'run_mode="common"' \
--form 'background="N"'
```

_response output_
```json
{
  "message": "[ run_date: 2021-01-01 ]\nSuccess: Running 'ai_forecast_mch3' mode 'common' with logging value (52 rows, 2 sec)",
  "process_time": 5
}
```

Rerun Data
----------

---

If current `run_date` parameter is less than the previous `run_date` in `ctr_data_pipeline` table, then the process will throw error statement.

#### **Example scenario**

_command_
```shell
$ curl --location --request POST 'http://localhost:5000/api/ai/run/data' \
--header 'APIKEY: <api-key-in-env>' \
--form 'table_name="sql:ai_forecast_mch3"' \
--form 'run_date="2021-01-01"' \
--form 'run_mode="common"' \
--form 'background="N"'
```

_response output_
```json
{
  "message": "[ run_date: 2021-01-01 ]\nError: Please check value of `run_date`, which less than the current control running date: '2021-01-02'",
  "process_time": 3
}
```

**Problem solving principle**

> The solution in this case is using `rerun` mode, which change `run_mode` parameter from `common` to `rerun`.
> The framework will skip compare `run_date` logic between parameter `run_date` and logging `run_date` in `ctr_data_pipeline` table value.

_new command_
```shell
$ curl --location --request POST 'http://localhost:5000/api/ai/run/data' \
--header 'APIKEY: <api-key-in-env>' \
--form 'table_name="sql:ai_forecast_mch3"' \
--form 'run_date="2021-01-01"' \
--form 'run_mode="rerun"' \
--form 'background="N"'
```

Reach Quota
-----------

---

If a table process that is frequently run on the same `run_date` exceeds the limit, which setting on column `run_count_max` in `ctr_data_pipeline` table, then this process will skip and throw warning logging response.

#### **Example scenario**

_command_
```shell
$ curl --location --request POST 'http://localhost:5000/api/ai/run/data' \
--header 'APIKEY: <api-key-in-env>' \
--form 'table_name="sql:ai_forecast_mch3"' \
--form 'run_date="2021-01-01"' \
--form 'run_mode="common"' \
--form 'background="N"'
```

_response output_
```json
{
  "message": "[ run_date: 2021-01-01 ]\nWarning: the running quota of 'ai_forecast_mch3' has been reached.",
  "process_time": 3
}
```

**Problem solving principle**

> Simply solution for this case is running this process in next running day or edit `run_date` to next running day.
> But if it is really necessary to work this `run_date` again, then the data on column `run_count_now` in the `ctr_data_pipeline` table must be corrected, such as change from current value to `0` or less than `run_count_max`.

_new command_
```shell
$ curl --location --request POST 'http://localhost:5000/api/ai/run/data' \
--header 'APIKEY: <api-key-in-env>' \
--form 'table_name="sql:ai_forecast_mch3"' \
--form 'run_date="2021-01-02"' \
--form 'run_mode="common"' \
--form 'background="N"'
```

Application Load Balancer Time Out
----------------------------------

---

If the processing time is taking longer than the Application Load Balancer gateway time-out already set, a gateway time-out statement in html format will be returned.

**Example scenario**

_command_
```shell
$ curl --location --request POST 'http://localhost:5000/api/ai/run/data' \
--header 'APIKEY: <api-key-in-env>' \
--form 'table_name="sql:ai_forecast_mch3"' \
--form 'run_date="2021-01-01"' \
--form 'run_mode="common"' \
--form 'background="N"'
```

_response output_
```html
<html>
<head><title>504 Gateway Time-out</title></head>
<body>
<center><h1>504 Gateway Time-out</h1></center>
</body>
</html>
```

**Problem solving principle**

> Simply solution for this case is change to run process with background, which mean edit parameter `background` from `N` to `Y`.
> Or send request form for adjust Application Load Balance timeout value to Operation/Infrastructure team.

_new command_
```shell
$ curl --location --request POST 'http://localhost:5000/api/ai/run/data' \
--header 'APIKEY: <api-key-in-env>' \
--form 'table_name="sql:ai_forecast_mch3"' \
--form 'run_date="2021-01-01"' \
--form 'run_mode="common"' \
--form 'background="Y"'
```

Code Error
----------

---

This will happen due to deployment failure after successful code testing in the development environment.

**Example scenario**

_command_
```shell
$ curl --location --request POST 'http://localhost:5000/api/ai/run/data' \
--header 'APIKEY: <api-key-in-env>' \
--form 'table_name="sql:ai_forecast_mch3"' \
--form 'run_date="2021-01-01"' \
--form 'run_mode="common"' \
--form 'background="N"'
```

_response output_

- If exception is ProgrammingError class, then response be
```json
{
  "message": "[ run_date: 2021-01-01 ]\nError: ProgrammingError: ...",
  "process_time": 5
}
```

- If exception is KeyError class, then response be
```json
{
  "message": "[ run_date: 2021-01-01 ]\nError: KeyError: ...",
  "process_time": 3
}
```

- If exception raise because `ctr_data_pipeline` table dose not exists, then response be
```json
{
  "message": "Error: <class 'psycopg2.errors.UndefinedTable'>: relation \"ai.ctr_data_pipeline\" does not exist\nLINE 3:  from ai.ctr_data_pipeline\n                        ^\n"
}
```

- The finally is Exception class, then response be
```json
{
  "message": "[ run_date: 2021-01-01 ]\nError: Exception: ..."
}
```

**Problem solving principle**

> If case is not possibility fix by edit request parameters or endpoint, then send request form for fixing coding in AI Framework Application to Data Engineering team

