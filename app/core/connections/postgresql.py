# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------

import asyncio
import functools
import warnings

import pandas as pd
from cryptography.utils import CryptographyDeprecationWarning

with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        category=CryptographyDeprecationWarning,
    )
    from sshtunnel import SSHTunnelForwarder

from collections.abc import Iterator
from typing import (
    Optional,
    Union,
)

from sqlalchemy import (
    create_engine,
    text,
)
from sqlalchemy.engine.url import URL
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.errors import DatabaseProcessError
from app.core.utils.config import Environs
from app.core.utils.reusables import chunks

env = Environs()

ParamType: type = Optional[Union[dict, bool]]

DRIVER: str = env.DB_DRIVER or "postgresql+psycopg2"


def generate_url(
    conf_replace: Optional[dict] = None,
    driver: Optional[str] = None,
):
    """Generate Database URI."""
    driver: str = driver or DRIVER
    _db_conf: dict = {
        "drivername": env.DB_DRIVER or driver,
        "database": env.DB_NAME,
        "username": env.DB_USER,
        "password": env.DB_PASS,
        "host": env.DB_HOST,
        "port": env.DB_PORT or None,
    }
    return URL.create(**(_db_conf | (conf_replace or {})))


def query_format(
    statement: Union[str, list], parameters: ParamType
) -> Union[str, list]:
    database_name: str = "database" if "sqlite" in DRIVER else env.DB_NAME
    _db_param = {
        "database_name": database_name,
        "ai_schema_name": env.get("AI_SCHEMA", "ai"),
        "main_schema_name": env.get("MAIN_SCHEMA", "public"),
    }
    if isinstance(parameters, bool):
        if parameters:
            parameters: ParamType = {}
        else:
            return statement
    if isinstance(statement, str):
        return statement.format(**(_db_param | parameters))
    return [_state.format(**(_db_param | parameters)) for _state in statement]


def ssh_connect():
    from conf import settings

    return SSHTunnelForwarder(
        **{
            "ssh_address_or_host": (env.SSH_HOST, int(env.SSH_PORT)),
            "ssh_username": env.SSH_USER,
            "ssh_private_key": str(
                settings.BASE_PATH / "conf" / env.SSH_PRIVATE_KEY
            ),
            "remote_bind_address": (env.DB_HOST, int(env.DB_PORT)),
            "local_bind_address": ("localhost", int(env.DB_PORT)),
        }
    )


def convert_local(function: callable) -> callable:
    """Connect private AWS RDS.

    Make sure the inbound security group rules of the private database
    instance allow access from the EC2 instance OR from subnet of the
    EC2 instance.
    """

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        if eval(env.get("SSH_FLAG", "False")):
            server = ssh_connect()
            if not server.is_alive:
                server.start()
            _db_conf: dict = {
                "database": env.DB_NAME,
                "username": env.DB_USER,
                "password": env.DB_PASS,
                "host": "localhost",
                "port": env.DB_PORT,
            }
            kwargs["conf_replace"] = _db_conf
        return function(*args, **kwargs)

    return wrapper


@convert_local
def generate_engine(
    conf_replace: Optional[dict] = None,
    parameters: Optional[dict] = None,
):
    """Generate database engine."""
    return create_engine(
        generate_url(conf_replace=conf_replace),
        pool_pre_ping=True,
        **(parameters or {}),
    )


@convert_local
def query_select(
    statement: str,
    conf_replace: Optional[dict] = None,
    parameters: ParamType = None,
) -> Iterator[dict]:
    engine = create_engine(
        generate_url(conf_replace=conf_replace), pool_pre_ping=True
    )
    _statement: str = (
        query_format(statement, parameters)
        if parameters is not None
        else statement
    )
    with engine.connect() as conn:
        with conn.begin():
            try:
                output_df = pd.read_sql_query(
                    text(_statement),
                    con=conn,
                    dtype="str",
                )
            except SQLAlchemyError as error:
                raise DatabaseProcessError(
                    f"{type(error).__module__}:{type(error).__name__}: "
                    f"{str(error.__dict__['orig'])}"
                    f" \nWith statement: \n{_statement}"
                ) from error
        yield from output_df.to_dict(orient="index").values()


@convert_local
def query_select_df(
    statement: str,
    conf_replace: Optional[dict] = None,
    parameters: ParamType = None,
) -> pd.DataFrame:
    engine = create_engine(
        generate_url(conf_replace=conf_replace),
        pool_pre_ping=True,
    )
    _statement: str = (
        query_format(statement, parameters)
        if parameters is not None
        else statement
    )
    with engine.connect() as conn:
        with conn.begin():
            try:
                output_df = pd.read_sql_query(
                    text(_statement),
                    con=conn,
                    dtype="str",
                )
            except SQLAlchemyError as error:
                raise DatabaseProcessError(
                    f"{type(error).__module__}:{type(error).__name__}: "
                    f"{str(error.__dict__['orig'])}"
                    f" \nWith statement: \n{_statement}"
                ) from error
        return output_df


@convert_local
def query_select_one(
    statement: str,
    conf_replace: Optional[dict] = None,
    parameters: ParamType = None,
) -> dict:
    engine = create_engine(
        generate_url(conf_replace=conf_replace),
        pool_pre_ping=True,
    )
    _statement: str = (
        query_format(statement, parameters)
        if parameters is not None
        else statement
    )
    with engine.connect() as conn:
        try:
            with conn.begin():
                output_df = pd.read_sql_query(
                    text(_statement), con=conn, dtype="str"
                )
        except SQLAlchemyError as error:
            raise DatabaseProcessError(
                f"{type(error).__module__}:{type(error).__name__}: "
                f"{str(error.__dict__['orig'])}"
                f" \nWith statement: \n{_statement}"
            ) from error
    return output_df.to_dict(orient="index").get(0, {})


@convert_local
def query_insert_from_csv(
    file_properties: dict,
    conf_replace: Optional[dict] = None,
    chunk_size: int = 10000,
    truncate: bool = False,
    compress: Optional[str] = None,
):
    """
    :file_properties:
        file_path: '/data/success/<file_name>.csv
        target: '<schema_name>.<table_name>'
        props:
            delimiter: '<delimiter>'
            engine: 'python'
    """
    engine = create_engine(
        generate_url(conf_replace=conf_replace),
        pool_pre_ping=True,
    )
    with engine.connect() as conn:
        with conn.begin():
            file_df = pd.read_csv(
                file_properties["filepath"],
                encoding=file_properties["props"].get("encoding", "utf-8"),
                delimiter=file_properties["props"].get("delimiter", ","),
                engine=file_properties["props"].get("engine", "python"),
                compression=(compress or "infer"),
            )
            if_exists_param: str = "append"
            for idx, chunk in enumerate(chunks(file_df, chunk_size)):
                if truncate:
                    if_exists_param = "replace" if idx == 0 else "append"
                try:
                    chunk.to_sql(
                        con=engine,
                        name=file_properties["table"],
                        schema=env.get("AI_SCHEMA", "ai"),
                        if_exists=if_exists_param,
                        index=False,
                    )

                except SQLAlchemyError as error:
                    raise DatabaseProcessError(
                        f"{type(error).__module__}:{type(error).__name__}: "
                        f"{str(error.__dict__['orig'])}"
                    ) from error
                yield (idx + 1) * chunk_size, len(chunk)


@convert_local
def query_execute(
    statement: Union[str, list],
    conf_replace: Optional[dict] = None,
    parameters: ParamType = None,
) -> None:
    engine = create_engine(
        generate_url(conf_replace=conf_replace),
        pool_pre_ping=True,
    )
    _statement: Union[str, list] = (
        query_format(statement, parameters)
        if parameters is not None
        else statement
    )
    with engine.execution_options(
        isolation_level="AUTOCOMMIT"
    ).connect() as conn:
        try:
            for _state in (
                _statement if isinstance(_statement, list) else [_statement]
            ):
                conn.execute(text(_state))
        except SQLAlchemyError as error:
            raise DatabaseProcessError(
                f"{type(error).__module__}:{type(error).__name__}: "
                f"{str(error.__dict__['orig'])}"
                f" \nWith statement: \n{_statement}"
            ) from error


@convert_local
def query_execute_row(
    statement: Union[str, list],
    conf_replace: Optional[dict] = None,
    parameters: ParamType = None,
) -> int:
    engine = create_engine(
        generate_url(conf_replace=conf_replace),
        pool_pre_ping=True,
    )
    _statement: Union[str, list] = (
        query_format(statement, parameters)
        if parameters is not None
        else statement
    )
    with engine.execution_options(
        isolation_level="AUTOCOMMIT"
    ).connect() as conn:
        try:
            for _state in (
                _statement if isinstance(_statement, list) else [_statement]
            ):
                return conn.execute(text(_state)).rowcount
        except SQLAlchemyError as error:
            raise DatabaseProcessError(
                f"{type(error).__module__}:{type(error).__name__}: "
                f"{str(error.__dict__['orig'])}"
                f" \nWith statement: \n{_statement}"
            ) from error


@convert_local
def query_transaction(
    statement: Union[str, list],
    conf_replace: Optional[dict] = None,
    parameters: ParamType = None,
) -> int:
    engine = create_engine(
        generate_url(conf_replace=conf_replace), pool_pre_ping=True
    )
    _statement: Union[str, list] = (
        query_format(statement, parameters)
        if parameters is not None
        else statement
    )
    with engine.execution_options(autocommit=False).connect() as conn:
        with conn.begin():
            try:
                for _state in (
                    _statement if isinstance(_statement, list) else [_statement]
                ):
                    _row: int = conn.execute(text(_state)).rowcount
                return _row
            except SQLAlchemyError as error:
                # conn.rollback()
                raise DatabaseProcessError(
                    f"{type(error).__module__}:{type(error).__name__}: "
                    f"{str(error.__dict__['orig'])}"
                    f" \nWith statement: \n{_statement}"
                ) from error


@convert_local
async def query_execute_async(
    statement: Union[str, list],
    conf_replace: Optional[dict] = None,
) -> None:
    """
    usage:
        asyncio.run(query_execute_async('select * from ai.ctr_data_pipeline'))
    """
    engine = create_async_engine(
        generate_url(conf_replace=conf_replace, driver="asyncpg"),
        echo=False,
        pool_pre_ping=True,
    )

    # Pause the execution of the function for a short period of time
    # await asyncio.sleep(0.1)
    async with engine.execution_options(
        isolation_level="AUTOCOMMIT"
    ).connect() as conn:
        async with conn.begin():
            await conn.execute(text(statement))
    await engine.dispose()
    await asyncio.sleep(0.1)
