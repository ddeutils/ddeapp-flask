# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from __future__ import annotations

import ast
import builtins
import inspect
import time
from collections.abc import Iterator
from datetime import date, datetime
from itertools import takewhile
from typing import Any, Optional, Union

from pydantic import Field, validator
from typing_extensions import Self

from conf.settings import settings

from .__types import DictKeyStr
from .base import (
    PARAMS,
    get_cal_date,
    get_plural,
    get_process_date,
    get_run_date,
    registers,
    sort_by_priority,
)
from .connections import (
    ParamType,
    query_execute,
    query_execute_row,
    query_insert_from_csv,
    query_select,
    query_select_check,
    query_select_df,
    query_select_one,
    query_select_row,
    query_transaction,
)
from .convertor import Statement, Value
from .errors import (
    CatalogArgumentError,
    ControlPipelineNotExists,
    ControlProcessNotExists,
    ControlTableNotExists,
    ControlTableValueError,
    DatabaseProcessError,
    FuncArgumentError,
    FuncNotFound,
    FuncRaiseError,
    ObjectBaseError,
    TableArgumentError,
    TableNotFound,
    TableNotImplement,
)
from .models import (
    UNDEFINED,
    ParameterMode,
    ParameterType,
    Status,
    TaskComponent,
    TaskMode,
    reduce_text,
)
from .schemas import (
    SCH_DEFAULT,
    WTM_DEFAULT,
    ControlSchedule,
    ControlWatermark,
)
from .statements import (
    ColumnStatement,
    ControlStatement,
    FunctionStatement,
    SchemaStatement,
    TableStatement,
    reduce_in_value,
    reduce_stm,
    reduce_value,
    reduce_value_pairs,
)
from .utils import (
    AI_APP_PATH,
    Environs,
    hash_string,
    logging,
    must_bool,
    must_list,
    only_one,
    ptext,
    split_iterable,
)
from .validators import (
    Choose,
    FrameworkParameter,
    MapParameter,
    ReleaseDate,
)
from .validators import (
    Pipeline as PipelineCatalog,
)
from .validators import (
    Task as BaseTask,
)

env = Environs(env_name=".env")
logger = logging.getLogger(__name__)


def null_or_str(value: str) -> Optional[str]:
    return None if value == "None" else value


def query_explain(statement: str, parameters: ParamType = True) -> dict:
    """Enhance query function for explain analytic."""
    stm = Statement(statement)
    query: str = stm.generate().strip()
    if query.count(";") > 1:
        raise FuncRaiseError(
            "query should not contain `;` more than 1 letter in 1 query"
        )
    if stm.type == "dql":
        _statement: str = PARAMS.ps_stm.explain.format(query=query.rstrip(";"))
        return {
            "QUERY PLAN": ast.literal_eval(
                query_select_one(_statement, parameters=parameters)[
                    "QUERY PLAN"
                ]
            )
        }
    elif stm.type == "dml":
        _statement: str = PARAMS.ps_stm.explain.format(
            query=Statement.add_row_num(query.rstrip(";")).rstrip(";")
        )
        return {
            "QUERY PLAN": ast.literal_eval(
                query_select_one(_statement, parameters=parameters)[
                    "QUERY PLAN"
                ]
            )
        }
    raise FuncArgumentError(
        "query explain support only statement type be `dql` or `dml` only"
    )


class MapParameterService(MapParameter):

    @validator("ext_params", always=True)
    def __prepare_ext_params(cls, value: DictKeyStr) -> DictKeyStr:
        try:
            return value | Control.params()
        except DatabaseProcessError:
            logger.warning("Control Data Parameter does not exists.")
            return value

    @validator("fwk_params", pre=True, always=True)
    def __prepare_fwk_params(
        cls,
        value: Union[DictKeyStr, FrameworkParameter],
    ) -> FrameworkParameter:
        print("Prepare Framework Params: ", value)
        if isinstance(value, dict):
            return FrameworkParameter.parse(
                {
                    "run_id": hash_string(get_run_date(fmt="%Y%m%d%H%M%S%f")),
                    "run_date": get_run_date(fmt="%Y-%m-%d"),
                    "run_mode": UNDEFINED,
                }
                | value
            )
        return value

    @validator("fwk_params")
    def ___post_fwk_params(cls, value):
        print("Prepare Framework Params Post: ", value)
        return value

    def add_ext_params(self, params: DictKeyStr) -> Self:
        logger.debug("Add more external parameters ...")
        self.__dict__["ext_params"] = self.ext_params | params
        return self

    def ext_params_refresh(self) -> Self:
        """Refresh External parameters."""
        self.__dict__["ext_params"] = self.ext_params | Control.params()
        return self

    def filter_params(
        self,
        params: list[str],
        additional: Optional[DictKeyStr] = None,
    ) -> DictKeyStr:
        _full_params: DictKeyStr = (
            self.fwk_params.dict(by_alias=False)
            | self.ext_params
            | (additional or {})
            | {"update_date": get_run_date(fmt="%Y-%m-%d %H:%M:%S")}
        )
        try:
            return {param: _full_params[param] for param in params}
        except KeyError as k:
            raise CatalogArgumentError(
                f"Catalog does not map config parameter for {k!r}"
            ) from k


class Schema(SchemaStatement):
    """Schema Service Model."""

    name: str = Field(default=env.AI_SCHEMA, description="Schema name")

    def exists(self) -> bool:
        """Push exists statement to target database."""
        return query_select_check(self.statement_check(), parameters=True)

    def create(self) -> Schema:
        """Push create statement to target database."""
        query_execute(self.statement_create())
        return self

    def drop(self, cascade: bool = False) -> Schema:
        """Push drop statement to target database."""
        query_execute(self.statement_drop(cascade=cascade))
        return self


class BaseAction(MapParameterService, FunctionStatement):

    def __init__(self, **data):
        super().__init__(**data)
        _auto_create: bool = must_bool(
            self.fwk_params.task_params.others.get("auto_create", "Y"),
            force_raise=True,
        )
        if self.type == "query":
            try:
                self.explain()
            except DatabaseProcessError as err:
                raise FuncRaiseError(
                    f"{self.name!r} was raised the statement error "
                    f"from query explain checking"
                ) from err
        else:
            if not self.exists():
                if _auto_create:
                    self.create()
                else:
                    raise FuncNotFound(
                        f"Table {self.name} not found in the AI Database, "
                        f"Please set `func_auto_create`."
                    )

    def exists(self) -> bool:
        """Push exists statement to target database."""
        return query_select_check(self.statement_check())

    def create(self) -> None:
        """Push create statement to target database."""
        if self.type not in ("func", "view", "mview"):
            raise FuncRaiseError(
                f"Function type {self.type!r} does not support create"
            )
        query_execute(self.statement(), parameters=True)

    def drop(self) -> None:
        if self.type == "func":
            query_execute(self.statement_func_drop(), parameters=True)
        else:
            raise FuncRaiseError(
                f"Function type {self.type!r} does not support drop"
            )

    def explain(self):
        if self.type != "query":
            raise FuncRaiseError(
                f"Function type {self.type!r} does not support for explain."
            )
        return query_explain(
            self.statement(),
            parameters=self.filter_params(self.profile.parameter),
        )


class Action(BaseAction): ...


class ActionQuery(BaseAction):

    def execute(self, limit: int = 10):
        """Push down the query to target database."""
        if self.type not in {"query", "view", "mview"}:
            raise FuncRaiseError(
                f"Function type {self.type!r} does not support for execute "
                f"query method."
            )
        stm: Statement = Statement(self.profile.statement)
        if stm.type == "dql":
            return {
                index: result
                for index, result in takewhile(
                    lambda x: x[0] <= limit,
                    enumerate(
                        query_select(
                            stm.generate(),
                            parameters=self.filter_params(
                                self.profile.parameter
                            ),
                        ),
                        start=1,
                    ),
                )
            }
        elif stm.type == "dml":
            return {
                1: {
                    "row_number": query_select_row(
                        stm.generate(),
                        parameters=self.filter_params(self.profile.parameter),
                    )
                }
            }
        raise FuncArgumentError(
            f"Function {self.name!r} does not support `push_query` "
            f"with statement type {stm.type!r}"
        )


class BaseNode(MapParameterService, TableStatement):
    """Base Node Service Model."""

    @classmethod
    def parse_task(
        cls,
        name: str,
        fwk_params: DictKeyStr,
        ext_params: Optional[DictKeyStr] = None,
    ) -> Self:
        """Parsing all parameters from Task before Running the Node."""
        return cls.parse_name(
            name,
            additional={
                "fwk_params": fwk_params,
                "ext_params": (ext_params or {}),
            },
        )

    @property
    def run_type(self) -> str:
        run_types: dict = PARAMS.map_tbl_run_type.copy()
        return next(
            (
                run_types[col]
                for col in self.profile.columns(pk_included=True)
                if col in run_types
            ),
            "daily",
        )

    def exists(self) -> bool:
        """Push exists statement to target database."""
        return query_select_check(self.statement_check(), parameters=True)

    def create(
        self,
        force_drop: bool = False,
        cascade: bool = False,
    ) -> Self:
        """Execute create statement to target database."""
        statements: list[str] = [self.statement_create()]
        if force_drop:
            rows: int = self.count()
            log: str = f"with {rows} row{get_plural(rows)} " if rows > 0 else ""
            statements.insert(0, self.statement_drop(cascade=cascade))
            logger.info(
                f"Drop table {self.name!r} {log}before create successful"
            )
            self.push(
                values={
                    "data_date": (
                        Control.params(module="framework").get(
                            "data_setup_initial_date", "2018-10-31"
                        )
                    ),
                }
            )
        query_transaction(statements, parameters=True)
        return self

    def create_partition(self): ...

    def drop(
        self,
        cascade: bool = False,
        execute: bool = True,
    ) -> Optional[str]:
        rows: int = self.count()
        logger.warning(
            f"Drop table {self.name!r} "
            f"{f'with {rows} row{get_plural(rows)} ' if rows > 0 else ''}"
            f"successful"
        )
        if execute:
            query_execute(self.statement_drop(cascade=cascade), parameters=True)
            return
        return self.statement_drop(cascade=cascade)

    def drop_partition(self): ...

    def make_log(self, values: Optional[dict] = None):
        try:
            return Control("ctr_data_logging").create(
                values=(
                    {
                        "table_name": self.name,
                        "data_date": f"{self.fwk_params.run_date:%Y-%m-%d}",
                        "run_date": f"{self.fwk_params.run_date:%Y-%m-%d}",
                        "action_type": "common",
                        "row_record": 0,
                        "process_time": 0,
                    }
                    | (values or {})
                )
            )
        except DatabaseProcessError:
            logger.warning("Cannot create log to `ctr_data_logging` ...")

    def pull_log(self, action_type: str, all_flag: bool = False):
        """Pull logging data from the Control Data Logging."""
        return Control("ctr_data_logging").pull(
            pm_filter={
                "table_name": self.name,
                "run_date": (
                    self.fwk_params.run_date.strftime("%Y-%m-%d")
                    if not all_flag
                    else "*"
                ),
                "action_type": action_type,
            },
            all_flag=all_flag,
        )

    def log(self, values: Optional[dict] = None):
        try:
            return Control("ctr_data_logging").push(
                values=(
                    {
                        "table_name": self.name,
                        "run_date": f"{self.fwk_params.run_date:%Y-%m-%d}",
                        "action_type": "common",
                    }
                    | (values or {})
                )
            )
        except DatabaseProcessError:
            logger.warning("Cannot update log to `ctr_data_logging` ...")

    def make_watermark(self, values: Optional[dict] = None):
        return Control("ctr_data_pipeline").create(
            values=(
                {
                    "system_type": PARAMS.map_tbl_sys.get(
                        self.prefix, UNDEFINED
                    ),
                    "table_name": self.name,
                    "table_type": UNDEFINED,
                    "data_date": f"{self.fwk_params.run_date:%Y-%m-%d}",
                    "run_date": f"{self.fwk_params.run_date:%Y-%m-%d}",
                    "run_type": self.run_type,
                    "run_count_now": 0,
                    "run_count_max": 0,
                    "rtt_value": 0,
                    "rtt_column": UNDEFINED,
                }
                | (values or {})
            )
        )

    def push(self, values: Optional[DictKeyStr] = None) -> int:
        """Update logging watermark to Control Pipeline."""
        _values: dict = {"table_name": self.name} | (values or {})
        try:
            return Control("ctr_data_pipeline").push(values=_values)
        except DatabaseProcessError as err:
            logger.warning(
                f"Does not update control data pipeline table because of, \n"
                f"{ptext(str(err))}"
            )
            return 0

    def count(self, condition: Optional[Union[str, list]] = None) -> int:
        if condition:
            _condition: list = (
                [condition] if isinstance(condition, str) else condition
            )
            return query_select_row(
                self.statement_count_condition(),
                parameters={"condition": " and ".join(_condition)},
            )
        return query_select_row(self.statement_count(), parameters={})

    def pull_metadata(self) -> dict[str, Any]:
        return {
            rows["column_name"]: {
                "order": int(rows["ordinal_position"]),
                "datatype": rows["data_type"],
                "nullable": eval(rows["nullable"]),
            }
            for rows in query_select(self.statement_columns(), parameters=True)
        }


class Node(BaseNode):

    choose: list[str] = Field(default_factory=list, description="Node choose")
    watermark: ControlWatermark = Field(
        default_factory=dict,
        description="Node watermark data from Control Pipeline",
    )

    def __init__(self, **data):
        super().__init__(**data)
        # TODO: We should re-design this step
        self.__validate_create()
        self.__validate_quota()

    def __validate_create(self) -> None:
        _auto_create: bool = self.validate_name_flag(
            self.fwk_params.task_params.others.get("auto_create", "N")
        )
        _auto_init: bool = self.validate_name_flag(
            self.fwk_params.task_params.others.get("auto_init", "N")
        )
        print(
            "Validate Create Task Params Other: ",
            self.fwk_params.task_params.others,
        )
        print("Validate Create auto_create: ", _auto_create)
        if not self.exists():
            if not _auto_create:
                raise TableNotFound(
                    "Please set `auto_create` be True or setup via API."
                )
            logger.info(f"Auto create {self.name!r} because set auto flag")
            self.create()
            if _auto_init:
                self.init()
        if (
            self.watermark.table_name == UNDEFINED
            and self.fwk_params.run_mode != TaskComponent.RECREATED
        ):
            if not _auto_create:
                raise ControlTableNotExists(
                    f"Table name: {self.name} does not exists "
                    f"in Control data pipeline"
                )
            logger.info(
                f"Auto insert configuration data to `ctr_data_pipeline` "
                f"for {self.name!r}"
            )
            self.make_watermark()
            self.watermark_refresh()

    def __validate_quota(self) -> None:
        if (
            self.fwk_params.run_date < self.watermark.run_date
            and self.fwk_params.run_mode == "common"
        ):
            raise ControlTableValueError(
                f"Please check the `run_date`, which less than the current "
                f"running date: {self.watermark.run_date:'%Y-%m-%d'}"
            )

    def filter(self, choose: list[str]) -> Self:
        self.__dict__["choose"] = choose
        return self

    @property
    def split_choose(self) -> Choose:
        _process: dict[str, list[str]] = {"reject": [], "filter": []}
        for process in self.choose:
            if process.startswith("!"):
                _process["reject"].append(process.split("!")[-1])
            else:
                _process["filter"].append(process)
        _filter: list[str] = _process["filter"]
        return Choose(
            included=(
                [_ for _ in self.process.keys() if _ in _filter]
                if _filter
                else list(self.process.keys())
            ),
            excluded=_process["reject"],
        )

    @property
    def has_quota(self) -> bool:
        return not (
            self.watermark.run_count_now < self.watermark.run_count_max
            or self.watermark.run_count_max == 0
        ) and (self.fwk_params.run_date == self.watermark.run_date)

    @validator("watermark", pre=True, always=True)
    def __prepare_watermark(cls, value: DictKeyStr, values):
        try:
            wtm: DictKeyStr = WTM_DEFAULT | Control("ctr_data_pipeline").pull(
                pm_filter=[values["name"]]
            )
        except DatabaseProcessError:
            wtm = WTM_DEFAULT
        return ControlWatermark.parse_obj(wtm | value)

    @validator("choose", pre=True, always=True)
    def __prepare_choose(cls, value: Union[str, list[str]]) -> list[str]:
        return list(set(value)) if isinstance(value, list) else [value]

    def watermark_refresh(self):
        logger.debug("Add more external parameters ...")
        self.__dict__["watermark"] = ControlWatermark.parse_obj(
            obj=Control("ctr_data_pipeline").pull(pm_filter=[self.name])
        )
        return self

    def process_date(self) -> date:
        return get_process_date(
            self.fwk_params.run_date,
            self.watermark.run_type,
            date_type="date",
        )

    def delete_with_condition(self, condition: str) -> int:
        return query_select_row(
            reduce_stm(
                PARAMS.ps_stm.push_del_with_condition, add_row_number=False
            ),
            parameters={"table_name": self.name, "condition": condition},
        )

    def delete_with_date(
        self,
        del_date: str,
        del_mode: Optional[str] = None,
    ) -> int:
        if len(self.watermark.rtt_column) > 1:
            raise CatalogArgumentError(
                "Delete with date does not support for multi rtt columns."
            )
        _primary_key: list[str] = self.profile.primary_key
        _primary_key_group: str = ",".join(
            [str(_) for _ in range(1, len(_primary_key) + 1)]
        )
        _primary_key_mark_a: str = ",".join(
            [f"a.{col}" for col in _primary_key]
        )
        _primary_key_join_a_and_b: str = "on " + " and ".join(
            [f"a.{col} = b.{col}" for col in _primary_key]
        )
        if self.watermark.table_type == "master" and del_mode == "rtt":
            _stm: str = reduce_stm(PARAMS.ps_stm.push_del_with_date.master_rtt)
        elif self.watermark.table_type != "master":
            _stm: str = reduce_stm(PARAMS.ps_stm.push_del_with_date.not_master)
        else:
            return 0
        return query_select_row(
            _stm,
            parameters={
                "ctr_rtt_col": self.watermark.rtt_column[0],
                "ctr_rtt_value": self.watermark.rtt_value,
                "del_operation": ("<" if del_mode == "rtt" else ">="),
                "del_date": del_date,
                "table_name": self.name,
                "primary_key": ", ".join(_primary_key),
                "primary_key_group": _primary_key_group,
                "primary_key_mark_a": _primary_key_mark_a,
                "primary_key_join_a_and_b": _primary_key_join_a_and_b,
            },
        )

    def pull_max_data_date(self, default: bool = True) -> Optional[date]:
        """Pull max data date that use the retention column for sorting."""
        _default_value: Optional[date] = (
            datetime.strptime("1990-01-01", "%Y-%m-%d").date()
            if default
            else None
        )
        if any(col == "undefined" for col in self.watermark.rtt_column):
            return _default_value
        elif len(self.watermark.rtt_column) > 1:
            raise CatalogArgumentError(
                "Pull max data date does not support for composite rtt columns"
            )
        return date.fromisoformat(
            query_select_one(
                PARAMS.ps_stm.pull_max_data_date,
                parameters={
                    "table_name": self.name,
                    "ctr_rtt_col": self.watermark.rtt_column[0],
                },
            ).get("max_date", _default_value)
        )

    def init(self) -> Self:
        if init := self.initial:
            _start_time: datetime = self.fwk_params.checkpoint()
            rows: int = self.__execute(init, force_sql=True)
            self.make_log(
                values={
                    "action_type": "initial",
                    "row_record": rows,
                    "process_time": self.fwk_params.duration(_start_time),
                    "status": 0,
                }
            )
            self.log(
                values={"data_date": f"{self.pull_max_data_date():%Y-%m-%d}"}
            )
            logger.info(
                f"Success initial value after create with {rows} row"
                f"{get_plural(rows)}"
            )
        return self

    def __execute(
        self,
        process: dict,
        force_sql: bool = False,
        additional: Optional[dict] = None,
    ) -> int:
        import pandas as pd

        _add: dict = additional or {}
        parameters: dict = self.filter_params(
            process["parameter"], additional=_add
        )

        if (self.type == "sql") or force_sql:
            # Push table process with `sql` type
            return query_select_row(
                statement=process["statement"], parameters=parameters
            )

        # Push table process with `func` type
        _func: callable = process.get("function")
        _func_params: list = list(inspect.signature(_func).parameters.keys())

        if not (
            _key := only_one(_func_params, PARAMS.map_func.input, default=False)
        ):
            raise TableNotImplement(
                f"Process function: {_func.__name__} of {self.name!r} "
                f"does not have input argument like `input_df`"
            )
        _input_df: pd.DataFrame = query_select_df(
            statement=process["load"]["statement"],
            parameters=self.filter_params(
                process["load"]["parameter"], additional=_add
            ),
        )
        if _input_df.empty:
            logger.warning("Input DataFrame of function was empty")
            return 0
        _func_value: str = self.__validate_func_output_type(
            _func(**{_key: _input_df}, **parameters)
        )
        if not _func_value:
            logger.warning("Output string of `function_value` was empty")
            return 0
        return query_select_row(
            statement=process["save"]["statement"],
            parameters=self.filter_params(
                process["save"]["parameter"],
                additional=_add | {"function_value": _func_value},
            ),
        )

    def execute(
        self,
        included: list,
        excluded: list,
        act_type: str,
        params: Optional[dict] = None,
        raise_if_error: bool = True,
    ) -> dict[int, int]:
        """Push all table processes to target database."""
        _rs: dict[int, int] = {1: 0}
        _start_time: datetime = self.fwk_params.checkpoint()
        _run_date: str = f"{self.fwk_params.run_date:%Y-%m-%d}"
        self.make_log(
            values={
                "run_date": params.get("run_date", _run_date),
                "data_date": params.get("data_date", _run_date),
                "action_type": act_type,
            }
        )
        ext_filter_mock: bool = must_bool(
            self.ext_params.get("data_normal_common_filter_mockup", "N"),
            force_raise=True,
        )
        for index, (name, ps) in enumerate(self.process.items(), start=1):
            if (
                (name.lower().startswith("mockup_data") and ext_filter_mock)
                or name in excluded
                or name not in included
            ):
                logger.warning(f"Filter {self.type.upper()} process: {name!r}")
                _rs[index] = 0
                continue
            logger.info(
                f"Priority {index:02d}: {self.type.upper()} process: {name!r}"
            )
            try:
                _rs[index] = self.__execute(ps.dict(), additional=params)
                logger.info(
                    f"Success with running process with {_rs[index]} "
                    f"row{get_plural(_rs[index])}"
                )
                self.log(
                    values={
                        "run_date": params.get("run_date", _run_date),
                        "action_type": act_type,
                        "row_record": reduce_text(str(_rs)),
                        "process_time": self.fwk_params.duration(_start_time),
                        "status": Status.SUCCESS.value,
                    }
                )
            except DatabaseProcessError as err:
                _rs[index] = 0
                self.log(
                    values={
                        "run_date": params.get("run_date", _run_date),
                        "action_type": act_type,
                        "row_record": reduce_text(str(_rs)),
                        "process_time": self.fwk_params.duration(_start_time),
                        "status": Status.FAILED.value,
                    }
                )
                if raise_if_error:
                    raise err
                logger.error(f"Error: {err.__class__.__name__}: {str(err)}")
                break
        return _rs

    def __validate_func_output_type(self, value: Any) -> str:
        import pandas as pd

        if isinstance(value, str):
            return value
        elif isinstance(value, pd.DataFrame):
            raise TableNotImplement(
                f"Output of process function in {self.name!r} "
                f"does not support for DataFrame type yet."
            )
        raise TableArgumentError(
            f"Output of process function in {self.name!r} "
            f"does not support for {type(value)!r} type."
        )

    def _prepare_before_rerun(self, sla: int) -> tuple[date, date]:
        _run_date: date = (
            self.fwk_params.run_date
            if self.fwk_params.run_mode == "common"
            else self.process_date
        )
        _data_date: date = self.watermark.data_date
        if self.fwk_params.run_mode == "common":
            return _run_date, _data_date

        if self.fwk_params.run_date < self.watermark.data_date:
            _run_date: date = self.process_date
            _data_date: date = get_cal_date(
                data_date=self.process_date,
                mode="sub",
                run_type=self.watermark.run_type,
                cal_value=(1 + sla),
                date_type="date",
            )
        if (
            self.ext_params.get("data_normal_rerun_reset_data", "N") == "Y"
            and self.watermark.table_type == "transaction"
        ):
            del_record: int = self.delete_with_date(
                del_date=f"{_run_date:%Y-%m-%d}",
                del_mode=self.fwk_params.run_mode,
            )
            logger.info(f"Delete current data transaction: {del_record} rows")
        logger.info(
            f"Reset (data_date, run_date) from "
            f"({self.watermark.data_date:%Y-%m-%d}, "
            f"{self.fwk_params.run_date:%Y-%m-%d}) to "
            f"({_data_date:%Y-%m-%d}, {_run_date:%Y-%m-%d})"
        )
        return _run_date, _data_date

    def process_run_count(self, row_record: dict) -> int:
        return (
            int(float(self.watermark.run_count_now)) + 1
            if (
                self.fwk_params.run_date == self.watermark.run_date
                and max(row_record.values(), default=0) != 0
            )
            else 0
        )

    def process_count(self) -> int:
        _excluded: list = self.split_choose.excluded
        if _included := self.split_choose.included:
            return len(_included) - len(
                set(_included).intersection(set(_excluded))
            )
        return self.process_max - len(_excluded)

    def process_start(self) -> dict[int, int]:
        _additional: dict[str, Any] = self.ext_params.copy()
        if self.ext_params.get("data_normal_rerun_reset_sla", "N") == "Y":
            _ps_sla: int = 0
            for key, value in _additional.items():
                if key in PARAMS.map_tbl_ps_sla.values() and isinstance(
                    value, int
                ):
                    _additional[key] = 0
                logger.warning(f"Reset `{key}` parameter values to 0")
        else:
            _ps_sla: int = _additional.get(
                PARAMS.map_tbl_ps_sla[self.watermark.run_type], 1
            )
        _run_date, _data_date = self._prepare_before_rerun(sla=_ps_sla)
        _row_record: dict[int, int] = self.execute(
            included=self.split_choose.included,
            excluded=self.split_choose.excluded,
            act_type=self.fwk_params.run_mode,
            params=(
                _additional
                | {
                    "run_date": _run_date.strftime("%Y-%m-%d"),
                    "data_date": _data_date.strftime("%Y-%m-%d"),
                }
            ),
        )
        if self.fwk_params.run_mode == "common":
            self.push(
                values={
                    "data_date": f"{self.pull_max_data_date():%Y-%m-%d}",
                    "run_date": f"{self.fwk_params.run_date:%Y-%m-%d}",
                    "run_count_now": self.process_run_count(
                        row_record=_row_record
                    ),
                }
            )
        else:
            self.push(
                values={
                    "data_date": f"{self.pull_max_data_date():%Y-%m-%d}",
                }
            )
        return _row_record


class NodeManage(Node):

    @property
    def name_backup(self) -> tuple[str, str]:
        _bk_name: bool = must_bool(self.ext_params.get("backup_table"))
        _bk_schema: bool = must_bool(self.ext_params.get("backup_schema"))
        return (
            f"{self.name}_bk" if _bk_name else "",
            f"{env.AI_SCHEMA}_bk" if _bk_schema else "",
        )

    def __backup(
        self,
        backup_name: str,
        backup_schema: Optional[str] = None,
        raise_if_error: bool = True,
    ) -> int:
        _start_time: datetime = self.fwk_params.checkpoint()
        self.make_log(values={"action_type": "backup"})
        _stm_all: list = [
            self.statement_backup(name=backup_name),
            self.statement_transfer(name=backup_name),
        ]
        _schema_name_bk: str = env.get("AI_SCHEMA", "ai")
        if backup_schema and ((backup_schema or "") != _schema_name_bk):
            _schema = Schema(name=_schema_name_bk)
            if _schema.exists():
                logger.info(f"Create backup schema: {_schema_name_bk!r}")
                _stm_all.insert(0, _schema.statement_create())
            _schema_name_bk: str = backup_schema
        logger.info(f"Create backup table: '{_schema_name_bk}.{backup_name}'")
        try:
            _rs: int = query_transaction(
                _stm_all,
                parameters={"ai_schema_backup": _schema_name_bk},
            )
            logger.info(
                f"Backup {self.name} to '{_schema_name_bk}.{backup_name}' "
                f"successful with {_rs} row{get_plural(_rs)}"
            )
            self.log(
                values={
                    "action_type": "backup",
                    "row_record": _rs,
                    "process_time": self.fwk_params.duration(_start_time),
                    "status": 0,
                }
            )
        except DatabaseProcessError as err:
            _rs: int = 0
            self.log(
                values={
                    "action_type": "backup",
                    "process_time": self.fwk_params.duration(_start_time),
                    "status": 1,
                }
            )
            if raise_if_error:
                raise err
            logger.error(f"Error: {err.__class__.__name__}: {str(err)}")
        return _rs

    def backup(self) -> int:
        backup_name, backup_schema = self.name_backup
        if any(backup_name == x["table_name"] for x in Control.tables()):
            raise TableNotImplement(
                f"default backup table name {backup_name!r} was "
                f"duplicated with any table in catalog"
            )
        return self.__backup(
            backup_name=backup_name, backup_schema=backup_schema
        )

    def retention_date(
        self,
        rtt_mode: str,
        rtt_date_type: Optional[str] = None,
    ) -> date:
        """Pull min retention date with mode, like `data_date` or `run_date`"""
        _input_date: date = (
            self.pull_max_data_date()
            if rtt_mode == "data_date"
            else self.fwk_params.run_date
        )
        _run_type: str = rtt_date_type or "monthly"
        return get_process_date(
            run_date=get_cal_date(
                data_date=_input_date,
                mode="sub",
                run_type=_run_type,
                cal_value=self.watermark.rtt_value,
                date_type="date",
            ),
            run_type=_run_type,
            date_type="date",
        )

    def __retention(self, rtt_date: date):
        _start_time: datetime = self.fwk_params.checkpoint()
        self.make_log(
            values={
                "data_date": rtt_date.strftime("%Y-%m-%d"),
                "action_type": "retention",
            }
        )
        try:
            _rtt_row: int = self.delete_with_date(
                del_date=rtt_date.strftime("%Y-%m-%d"), del_mode="rtt"
            )
            logger.info(
                f"Success Delete data_date that less than {rtt_date:%Y-%m-%d} "
                f"with {_rtt_row} row{get_plural(_rtt_row)}"
            )
            self.log(
                values={
                    "action_type": "retention",
                    "row_record": _rtt_row,
                    "process_time": self.fwk_params.duration(_start_time),
                    "status": 0,
                }
            )
        except DatabaseProcessError as err:
            _rtt_row: int = 0
            self.log(
                values={
                    "action_type": "retention",
                    "process_time": self.fwk_params.duration(_start_time),
                    "status": 1,
                }
            )
            raise err
        return _rtt_row

    def retention(self):
        if self.watermark.rtt_value == 0:
            logger.info(
                "Skip retention process because `ctr_rtt_value` equal 0 or "
                "does not set in Control Framework table"
            )
            return 0
        logger.info(
            f"Process Retention value: {self.watermark.rtt_value} month with "
            f"mode: {self.ext_params.get('data_retention_mode', 'data_date')}"
        )
        return self.__retention(
            rtt_date=self.retention_date(
                rtt_mode=self.ext_params.get("data_retention_mode", "data_date")
            )
        )

    def vacuum(self, options: Optional[list[str]] = None) -> None:
        _option: str = ", ".join(options) if options else "full"
        return query_execute(self.statement_vacuum(_option), parameters=True)

    def diff(self):
        """Get mapping of different columns between config table and target
        table."""
        get_cols, pull_cols, compare, ctx = self.__gen_columns_diff()
        merge_cols, ctx = self.__gen_columns_diff_map(get_cols, pull_cols, ctx)
        try:
            if ctx["col_update"]:
                self.__diff_update(get_cols, compare["left"])
            elif ctx["col_delete"]:
                self.__diff_delete(compare["right"])
            elif ctx["col_diff"]:
                self.__diff_merge(merge_cols)
            else:
                logger.info(
                    f"Dose not any change from configuration data "
                    f"in table {self.name!r}"
                )
        except DatabaseProcessError as err:
            logger.error(
                f"Push different columns process of {self.name!r} "
                f"failed with {err}"
            )

    def __diff_update(self, get_cols: dict, not_exists_cols: set):
        """Update column properties of target table in database
        TODO: ALTER [ COLUMN ] column { SET | DROP } NOT NULL
        """
        logger.info(
            f"Update column properties or add new column"
            f"{get_plural(len(not_exists_cols))}, "
            f"{', '.join([repr(_) for _ in not_exists_cols])} of table "
            f"{self.name!r} in database"
        )
        add_col_list = [
            (name, props["datatype"])
            for name, props in get_cols.items()
            if name in not_exists_cols
        ]
        print(
            ", ".join(f"add column {col[0]} {col[1]}" for col in add_col_list)
        )
        # query_execute(statement=params.ps_stm.alter, parameters={
        #     'table_name': self.tbl_name,
        #     'action': ', '.join(
        #         f'add column {col[0]} {col[1]}'
        #         for col in add_col_list
        #     )
        # })

    def __diff_delete(self, exists_cols: set):
        """Delete column in target table with not exists from configuration
        data."""
        logger.info(
            f"Drop column{get_plural(len(exists_cols))}, "
            f"{', '.join([repr(_) for _ in exists_cols])} of table "
            f"{self.name!r} in database"
        )
        print(", ".join(f"drop column {col}" for col in exists_cols))
        # query_execute(statement=params.ps_stm.alter, parameters={
        #     'table_name': self.tbl_name,
        #     'action': ', '.join(f'drop column {col}' for col in exists_cols)
        # })

    def __diff_merge(self, merge_cols: dict):
        """Transfer full data from old table to new created table from merge
        columns."""
        mapping_col_insert: list = []
        mapping_col_select: list = []
        for col_name, col_attrs in merge_cols.items():
            mapping_col_insert.append(col_name)
            if _not_match := col_attrs["not_match"]:
                stm_select: str = col_name

                # Check nullable not match
                if _nullable := _not_match.get("nullable"):
                    # Generate default value with data-type
                    if not _nullable[0] and _nullable[1]:
                        # stm_select: str = f"coalesce({col_name}, {default})"
                        raise TableNotImplement(
                            f"Table column different merge process does not "
                            f"support for change `null` to `not null` "
                            f"with column: {col_name!r} in {self.name!r}"
                        )

                # Check data-type not match
                if _datatype := _not_match.get("datatype"):
                    stm_select: str = (
                        f"{stm_select}::{_datatype[0]} as {col_name}"
                    )

                mapping_col_select.append(stm_select)
                continue
            mapping_col_select.append(col_name)

        logger.info(
            f"insert into {{database_name}}.{{ai_schema_name}}.{self.name} "
            f"\n\t\t ( {', '.join(mapping_col_insert)} ) \n"
            f"\t\t select {', '.join(mapping_col_select)} \n"
            f"\t\t from {{database_name}}.{{ai_schema_name}}."
            f"{self.name}_old;"
        )
        # NOTE:
        # query_execute(statement=params.ps_stm.push_merge, parameters={
        #     'table_name': self.tbl_name,
        #     'mapping_insert': ', '.join(mapping_col_insert),
        #     'mapping_select': ', '.join(mapping_col_select)
        # })

    def __gen_columns_diff(
        self,
    ) -> tuple[
        dict[str, ColumnStatement], DictKeyStr, DictKeyStr, dict[str, bool]
    ]:
        context: dict[str, bool] = {
            "col_not_equal": False,
            "col_update": False,
            "col_delete": False,
            "col_diff": False,
        }
        _pull_cols: dict = self.pull_metadata()
        _get_cols: dict[str, ColumnStatement] = self.profile.to_mapping(pk=True)

        compare_diff: dict = {"left": set(), "right": set(), "all": set()}
        compare_diff["left"].update(
            set(_get_cols.keys()).difference(set(_pull_cols.keys()))
        )
        compare_diff["right"].update(
            set(_pull_cols.keys()).difference(set(_get_cols.keys()))
        )
        compare_diff["all"].update(
            set(_get_cols.keys()).intersection(set(_pull_cols.keys()))
        )

        if len(_get_cols) != len(_pull_cols) or sorted(
            _get_cols.keys()
        ) != sorted(_pull_cols.keys()):
            context["col_not_equal"] = True
            if not compare_diff["right"]:
                context["col_update"] = True
            elif not compare_diff["left"]:
                context["col_delete"] = True

        return _get_cols, _pull_cols, compare_diff, context

    @staticmethod
    def __gen_columns_diff_map(
        _get_cols: DictKeyStr,
        _pull_cols: DictKeyStr,
        context: dict[str, bool],
    ) -> tuple[DictKeyStr, dict[str, bool]]:
        """Generate mapping of different matrix of column properties."""
        results: dict = {}

        if context["col_not_equal"]:
            _get_cols: dict = {
                k: _get_cols[k] for k in _get_cols if k in _pull_cols
            }

        for col_name, col_attrs in _get_cols.items():
            result: dict = {"match": {}, "not_match": {}}
            for _check in {"order", "datatype", "nullable"}:
                if _pull_cols[col_name][_check] != col_attrs[_check]:
                    context["col_diff"] = True
                    if _check == "order":
                        # Cannot use update or delete because order of column
                        # does not align with config data
                        context["col_update"] = False
                        context["col_delete"] = False
                    result["not_match"][_check] = (
                        col_attrs[_check],
                        _pull_cols[col_name][_check],
                    )
                else:
                    result["match"][_check] = (
                        col_attrs[_check],
                        _pull_cols[col_name][_check],
                    )
                results[col_name]: dict = result
        return results, context


class NodeLocal(Node):
    """Node for Local File loading."""

    def load(
        self,
        filename: str,
        chuck: int = 10_000,
        truncate: bool = False,
        compress: Optional[str] = None,
    ) -> int:
        file_props: DictKeyStr = {
            "filepath": AI_APP_PATH / f"{registers.path.data}/{filename}",
            "table": self.name,
            "props": {
                "delimiter": "|",
                "encoding": "utf-8",
                "engine": "python",
            },
        }
        rows: int = 0
        for chunk, row in query_insert_from_csv(
            file_props,
            chunk_size=chuck,
            truncate=truncate,
            compress=compress,
        ):
            logger.info(
                f"Success with first chuck size {chunk} "
                f"with {row} row{get_plural(row)}"
            )
            rows += row
        return rows


class NodeIngest(Node):
    """Node that implement Ingestion method that use on the ingestion
    component."""

    def __ingest(
        self,
        payloads: list,
        mode: str,
        action: str,
        update_date: datetime,
    ) -> tuple[int, int]:
        ps_row_success: int = 0
        ps_row_failed: int = 0
        _start_time: datetime = self.fwk_params.checkpoint()
        self.make_log(
            values={
                "data_date": update_date.strftime("%Y-%m-%d"),
                "update_date": update_date.strftime("%Y-%m-%d %H:%M:%S"),
                "action_type": "ingestion",
            }
        )

        # Note: Chuck size for merge mode will be split from the first level
        # of payloads.
        for index, data in enumerate(
            split_iterable(payloads, settings.APP_INGEST_CHUCK),
            start=1,
        ):
            try:
                cols, values = Value(
                    values=data,
                    update_date=update_date.strftime("%Y-%m-%d %H:%M:%S"),
                    mode=mode,
                    action=action,
                    expected_cols=self.profile.to_mapping(pk=True),
                    expected_pk=self.profile.primary_key,
                ).generate()
                row_ingest: int = (
                    query_execute_row(
                        self.statement_update(suffix="UD"),
                        parameters={
                            "string_columns": ", ".join(cols),
                            "string_columns_pairs": ", ".join(
                                [
                                    (
                                        f"{col.name} = {self.shortname}_UD."
                                        f"{col.name}::{col.datatype}"
                                    )
                                    for col in self.profile.features
                                    if col.name not in self.profile.primary_key
                                ]
                            ),
                            "string_values": values,
                        },
                    )
                    if action == "update"
                    else query_execute_row(
                        self.statement_insert(),
                        parameters={
                            "string_columns": ", ".join(cols),
                            "string_values": values,
                        },
                    )
                )
                _failed: int = 0
                if row_ingest != (_row_generate := (values.count("), (") + 1)):
                    _failed: int = _row_generate - row_ingest
                    ps_row_failed += _failed
                ps_row_success += row_ingest
                logger.info(
                    f"Ingest chunk {index:02d}: "
                    f"(success: {row_ingest} row{get_plural(row_ingest)},"
                    f"false: {_failed} row{get_plural(_failed)})"
                )
                self.log(
                    values={
                        "action_type": "ingestion",
                        "row_record": {1: ps_row_success, 2: ps_row_failed},
                        "process_time": self.fwk_params.duration(_start_time),
                        "status": 0,
                    }
                )
            except ObjectBaseError as err:
                self.log(
                    values={
                        "action_type": "ingestion",
                        "row_record": {1: ps_row_success, 2: ps_row_failed},
                        "process_time": self.fwk_params.duration(_start_time),
                        "status": 1,
                    }
                )
                logger.error(f"Error: {err.__class__.__name__}: {str(err)}")
                raise err
        return ps_row_success, ps_row_failed

    def ingest(self) -> tuple[int, int]:
        """Ingest Data from the input payload."""
        _action: str = self.ext_params.get("ingest_action", "insert")
        if (_mode := self.ext_params.get("ingest_mode", "common")) not in (
            "common",
            "merge",
        ) or _action not in ("insert", "update"):
            raise TableArgumentError(
                f"Pair of ingest mode {_mode!r} and action mode {_action!r} "
                f"does not support yet."
            )
        _update_date: datetime = (
            datetime.fromisoformat(_update)
            if (_update := self.ext_params.get("update_date"))
            else get_run_date("datetime")
        )
        if self.watermark.data_date > _update_date.date():
            raise ControlTableValueError(
                f"Please check value of `update_date`, which less than "
                f"the current control data date: "
                f"'{self.watermark.data_date:'%Y-%m-%d'}'"
            )
        _payloads: Union[dict, list] = self.ext_params.get("payloads", [])
        _rs: tuple[int, int] = self.__ingest(
            payloads=[_payloads] if isinstance(_payloads, dict) else _payloads,
            mode=_mode,
            action=_action,
            update_date=_update_date,
        )
        self.push(values={"data_date": _update_date.strftime("%Y-%m-%d")})
        return _rs


class BasePipeline(MapParameterService, PipelineCatalog):
    """Pipeline Service Model."""

    @classmethod
    def parse_task(
        cls,
        name: str,
        fwk_params: DictKeyStr,
        ext_params: Optional[DictKeyStr] = None,
    ) -> Self:
        """Parsing all parameters from Task before Running the Node."""
        return cls.parse_name(
            name,
            additional={
                "fwk_params": fwk_params,
                "ext_params": (ext_params or {}),
            },
        )

    @classmethod
    def pull_watermarks(
        cls,
        pipe_id: Optional[str] = None,
        included_cols: Optional[list] = None,
        all_flag: bool = False,
    ):
        """Pull tacking data from the Control Task Schedule."""
        if pipe_id:
            _pipe_id: str = pipe_id
        elif all_flag:
            _pipe_id: str = "*"
        else:
            raise ObjectBaseError(
                "Pull Task Schedule should pass pipeline id or check all flag."
            )
        return Control("ctr_task_schedule").pull(
            pm_filter={"pipeline_id": _pipe_id},
            included=included_cols,
            all_flag=all_flag,
        )

    def make_watermark(self, values: Optional[dict] = None):
        return Control("ctr_task_schedule").create(
            values=(
                {
                    "pipeline_id": self.id,
                    "pipeline_name": self.name,
                    "pipeline_type": self.schedule,
                    "tracking": "SUCCESS",
                    "active_flg": "true",
                }
                | (values or {})
            )
        )

    def push(self, values: Optional[dict] = None):
        """Update tacking information to the Control Task Schedule."""
        return Control("ctr_task_schedule").push(
            values=(
                {"pipeline_id": self.id, "tracking": "SUCCESS"} | (values or {})
            )
        )


class Pipeline(BasePipeline):

    watermark: ControlSchedule = Field(
        default_factory=dict,
        description="Pipeline watermark data from Control Task Schedule",
    )

    def __init__(self, **data):
        super().__init__(**data)
        self.__validate_create()
        if (
            self.watermark.pipeline_id == UNDEFINED
            and self.fwk_params.run_mode != TaskComponent.RECREATED
        ):
            ...

    def __validate_create(self): ...

    @property
    def internal_gen(self) -> bool:
        return self.name in ("retention_search", "control_search")

    @validator("watermark", pre=True, always=True)
    def __prepare_watermark(cls, value: DictKeyStr, values):
        try:
            wtm: DictKeyStr = SCH_DEFAULT | cls.pull_watermarks(
                pipe_id=values["name"]
            )
        except DatabaseProcessError:
            wtm = SCH_DEFAULT
        return ControlSchedule.parse_obj(wtm | value)

    def watermark_refresh(self):
        logger.debug("Add more external parameters ...")
        self.__dict__["watermark"] = ControlSchedule.parse_obj(
            obj=self.pull_watermarks(pipe_id=self.id)
        )
        return self

    def process_nodes(
        self,
        auto_update: bool = True,
    ) -> Iterator[tuple[int, NodeManage]]:
        """Node method for passing pipeline parameters to all node in the
        pipeline."""
        for order, node in self.nodes.items():
            yield (
                order,
                NodeManage.parse_task(
                    name=node["name"],
                    fwk_params={
                        "run_id": self.fwk_params.run_id,
                        "run_date": f"{self.fwk_params.run_date:%Y-%m-%d}",
                        "run_mode": self.fwk_params.run_mode,
                        "task_params": self.fwk_params.task_params,
                    },
                    ext_params=self.ext_params,
                ).filter(node.get("choose", [])),
            )
        if auto_update and not self.internal_gen:
            self.push(values={"pipeline_id": [self.id, *self.alert]})


class PipelineManage(Pipeline):
    def __check_trigger_function(
        self,
        trigger: Union[str, list, set],
        ctr_schedules: dict,
    ):
        if isinstance(trigger, str):
            if not (pln_trigger := ctr_schedules.get(trigger, {})):
                logger.warning(
                    f"Pipeline ID: {trigger!r} does not exists in "
                    f"`ctr_task_schedule` or active_flg equal 'N' "
                    f"from `trigger` in '{self.name}'"
                )
                raise ControlPipelineNotExists(
                    f"Pipeline ID: {trigger!r} does not exists in "
                    f"`ctr_task_schedule` or active_flg equal 'N' "
                    f"from `trigger` in '{self.name}'"
                )
            return (
                (pln_trigger["update_date"] > self.watermark.update_date)
                and (
                    pln_trigger["tracking"]
                    == self.watermark.tracking
                    == "SUCCESS"
                )
            ) or (
                (pln_trigger["update_date"] <= self.watermark.update_date)
                and (
                    pln_trigger["tracking"] == "SUCCESS"
                    and self.watermark.tracking == "FAILED"
                )
            )
        if run_flags := [
            self.__check_trigger_function(_trigger, ctr_schedules)
            for _trigger in trigger
        ]:
            return (
                any(run_flags) if isinstance(trigger, set) else all(run_flags)
            )
        return False

    def check_triggered(self) -> bool:
        """Return True if pipeline ..."""
        _triggers: Union[set, list] = self.trigger.copy()
        _all_pipe_schedules: dict = {
            _ctr_value["pipeline_id"]: {
                "tracking": _ctr_value["tracking"],
                "update_date": datetime.fromisoformat(
                    _ctr_value["update_date"]
                ),
            }
            for _ctr_value in self.pull_watermarks(
                included_cols=["pipeline_id", "tracking", "update_date"],
                all_flag=True,
            )
        }
        return self.__check_trigger_function(_triggers, _all_pipe_schedules)

    def check_scheduled(self, group: str, waiting_process: int = 300) -> bool:
        if not self.schedule:
            return False

        if group not in self.schedule:
            return False

        if self.watermark.tracking == "FAILED":
            logger.warning(
                f"Pipeline ID: {self.id!r} was `FAILED` status, "
                f"please check `ctr_task_process` with pipeline_name = "
                f"{self.name!r}."
            )
            return False
        while self.watermark.tracking == "PROCESSING":
            logger.info(f"Waiting Pipeline ID: {self.id!r} processing ...")
            time.sleep(waiting_process)
            self.watermark_refresh()
        return True


class Task(BaseTask):
    """Task Service Model.

    Notes:
        -   For starting any api tasks, I will create this Task model with the 4
            keys (module, parameters, mode, and component).
        -   Task will generate the task ID from a module value, ``param.type``,
            and ``param.name``.
        -   Task has the runner method that return generator of datetime from
            the ``param.dates``.
        -   ``Task.start`` and ``Task.finish`` is the main of create and update
            the task status log to logging table at target database.

    Examples:

        >>> task: Task = Task.make(module="demo_docstring")
        ... task.start()
        ... "Do Something"
        ... task.finish()
    """

    def __enter__(self) -> Self:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.finish()

    @classmethod
    def pull(cls, task_id: str):
        """Pull Task from target database with Task ID."""
        if ctr_process := (
            Control("ctr_task_process").pull(pm_filter=[task_id])
        ):
            mode, _type = ctr_process.get(
                "process_type", "foreground|undefined"
            ).split("|", maxsplit=1)
            component, module = ctr_process.get(
                "process_module", "undefined|undefined"
            ).split("|", maxsplit=1)
            run_mode: str = ctr_process.get("run_mode", "common")
            _date: Optional[str] = null_or_str(ctr_process.get("run_date_get"))
            return cls(
                module=module,
                parameters={
                    "name": module,
                    "type": ParameterType(_type),
                    "mode": ParameterMode(run_mode),
                    "dates": must_list(ctr_process["run_date_put"]),
                    **ctr_process,
                },
                mode=TaskMode(mode),
                component=TaskComponent(component),
                status=Status(int(ctr_process["status"])),
                id=task_id,
                message=ctr_process["process_message"],
                release=ReleaseDate(date=_date),
            )
        raise ControlProcessNotExists(
            f"Process ID: {task_id} does not exists in Control Task Process "
            f"table."
        )

    def start(self, task_total: Optional[int] = 1) -> int:
        """Start Task.

        :rtype: int
        :return: Return 0 if the release was pushed (runner index !=
            start index). But push the log to target database when
            release was not pushed.
        """
        return (
            0
            if self.release.pushed
            else self.create(values={"process_number_put": task_total})
        )

    def finish(self) -> int:
        """Update task log to target database."""
        return self.push(
            values=(
                {
                    "process_name_get": "null",
                    "run_date_get": "null",
                }
                if self.status == Status.SUCCESS
                else {}
            )
        )

    def create(self, values: Optional[dict] = None) -> int:
        """Create information to the Control Data Logging."""
        return Control("ctr_task_process").create(
            values=(
                {
                    "process_id": self.id,
                    "process_name_put": self.parameters.name,
                    "process_name_get": "null",
                    "run_date_put": reduce_text(str(self.parameters.dates)),
                    "run_date_get": self.release.date,
                    "process_message": (
                        f"Start {self.mode} process {self.parameters.type} "
                        f"`{self.parameters.name}` ..."
                    ),
                    "process_number_put": 1,
                    "process_number_get": 1,
                    "process_module": f"{self.component}|{self.module}",
                    "process_type": f"{self.mode}|{self.parameters.type}",
                }
                | (values or {})
            )
        )

    def push(self, values: Optional[dict] = None) -> int:
        """Update information to the Control Data Logging."""
        return Control("ctr_task_process").push(
            values=(
                {
                    "process_id": self.id,
                    "process_message": reduce_text(self.message),
                    "process_time": self.duration(),
                    "status": self.status,
                }
                | (values or {})
            )
        )


class Control(ControlStatement):

    def __init__(self, name: str, *, params: Optional[dict] = None) -> None:
        super().__init__(name=name)
        self.defaults: dict[str, Union[str, int]] = {
            "update_date": get_run_date(fmt="%Y-%m-%d %H:%M:%S"),
            "process_time": 0,
            "status": Status.WAITING.value,
        } | (params or {})

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.name})"

    def __str__(self) -> str:
        return self.name

    @classmethod
    def params(cls, module: Optional[str] = None) -> DictKeyStr:
        logger.debug("Loading params from `ctr_data_parameter` by Control ...")
        _results: dict[str, Any] = {}
        try:
            _results: dict[str, Any] = {
                value["param_name"]: (
                    ast.literal_eval(value["param_value"])
                    if value["param_type"] in ("list", "dict")
                    else getattr(builtins, value["param_type"])(
                        value["param_value"]
                    )
                )
                for value in query_select(
                    cls.statement_params(),
                    parameters=reduce_value_pairs(
                        {"module_type": (module or "*")}
                    ),
                )
            }
        except DatabaseProcessError:
            logger.warning(
                "Control Data Parameter does not exists, so, it will use empty."
            )

        # NOTE: Calculate special parameters that logic was provided by vendor.
        proportion_value: int = _results.get("proportion_value", 3)
        proportion_inc_curr_m: str = _results.get(
            "proportion_inc_current_month_flag", "N"
        )
        return {
            "window_start": (
                proportion_value
                if proportion_inc_curr_m == "N"
                else (proportion_value - 1)
            ),
            "window_end": (1 if proportion_inc_curr_m == "N" else 0),
            **_results,
        }

    @classmethod
    def tables(
        cls,
        condition: Optional[str] = None,
    ) -> Iterator[dict[str, str]]:
        """Get all tables with `condition` argument from `ctr_data_pipeline` in
        target database and convert to python list of dictionary type."""
        logger.debug("Loading tables from `ctr_data_pipeline` by Control ...")
        for name in sort_by_priority(
            [
                tbl["table_name"]
                for tbl in cls("ctr_data_pipeline").pull(
                    pm_filter={"table_name": "*"},
                    included=["table_name"],
                    condition=condition,
                    all_flag=True,
                )
            ]
        ):
            yield {"table_name": name}

    def create(
        self,
        values: dict,
        condition: Optional[str] = None,
    ) -> int:
        _ctr_columns = filter(
            lambda _col: _col not in {"primary_id"}, self.cols
        )
        _add_column: DictKeyStr = self.defaults | {
            "tracking": "SUCCESS",
            "active_flg": "Y",
        }
        _row_record_filter: str = ""
        _status_filter: str = ""
        for col in _ctr_columns:
            value_old: Union[str, int] = (
                _add_column.get(col, "null")
                if col not in values
                else values[col]
            )
            values[col]: str = reduce_value(value_old)
        if "status" in list(_ctr_columns):
            _status_filter: str = "where excluded.status = '2'"
        if "row_record" in list(_ctr_columns):
            _row_record_filter: str = (
                f"{'or' if _status_filter else 'where'} "
                f"{self.tbl.shortname}.row_record <= excluded.row_record"
            )
        _set_value_pairs: str = ", ".join(
            [f"{_} = excluded.{_}" for _ in self.cols_no_pk]
        )
        return query_select_row(
            self.statement_create(),
            parameters={
                "columns_pair": ", ".join(values),
                "values": ", ".join(values.values()),
                "primary_key": ", ".join(self.pk),
                "set_value_pairs": _set_value_pairs,
                "row_record_filter": _row_record_filter,
                "status_filter": _status_filter,
                "condition": (
                    f"""AND ({condition.replace('"', "'")})"""
                    if condition
                    else ""
                ),
            },
        )

    def push(
        self,
        values: DictKeyStr,
        condition: Optional[str] = None,
    ) -> int:
        _add_column: dict = self.defaults | {"tacking": "PROCESSING"}
        for col, default in _add_column.items():
            if col in self.cols and col not in values:
                values[col] = default
        _update_values: dict[str, str] = {
            k: reduce_value(str(v))
            for k, v in values.items()
            if k not in self.pk
        }
        _filter: list[str] = [
            f"{self.tbl.shortname}.{_} in {reduce_in_value(values[_])}"
            for _ in self.pk
        ]
        return query_select_row(
            self.statement_push(),
            parameters={
                "update_values_pairs": ", ".join(
                    [f"{k} = {v}" for k, v in _update_values.items()]
                ),
                "filter": " AND ".join(_filter),
                "condition": (
                    f"""AND ({condition.replace('"', "'")})"""
                    if condition
                    else ""
                ),
            },
        )

    def pull(
        self,
        pm_filter: Union[list, dict],
        condition: Optional[str] = None,
        included: Optional[list] = None,
        *,
        active_flag: Optional[str] = None,
        all_flag: Optional[bool] = False,
    ) -> DictKeyStr:
        """Pull data from the control table."""
        if len(self.pk) > 1 and isinstance(pm_filter, list):
            raise TableNotImplement(
                f"Pull control does not support `pm_filter` with `list` type "
                f"when {self.name} have primary keys more than 1"
            )
        elif isinstance(pm_filter, dict):
            if any(col not in self.pk for col in pm_filter):
                raise TableArgumentError(
                    f"Pull control does not support value in `pm_filter` "
                    f"with keys, {str(pm_filter.keys())}"
                )
            _pm_filter: dict = reduce_value_pairs(pm_filter)
        else:
            _pm_filter: dict = {
                self.pk[0]: ", ".join(map(reduce_value, pm_filter))
            }
        _pm_filter_stm: str = " and ".join(
            [f"{pk} in ({_pm_filter[pk]})" for pk in self.pk]
        )
        return (query_select if all_flag else query_select_one)(
            self.statement_pull(),
            parameters={
                "select_columns": ", ".join(
                    col for col in self.cols if col in (included or self.cols)
                ),
                "primary_key_filters": _pm_filter_stm,
                "active_flag": (
                    f"and active_flg in ('{(active_flag or 'Y')}')"
                    if "active_flg" in self.cols
                    else ""
                ),
                "condition": (
                    f"""AND ({condition.replace('"', "'")})"""
                    if condition
                    else ""
                ),
            },
        )


__all__ = (
    "Schema",
    "Action",
    "ActionQuery",
    "Node",
    "NodeLocal",
    "NodeIngest",
    "NodeManage",
    "Pipeline",
    "PipelineManage",
    "Task",
    "Control",
)
