# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------

import os
import sys

import click

os.environ["AI_APP_PATH"] = os.path.abspath(os.path.dirname(__file__))


@click.group()
def cli(): ...


@click.command(
    name="load",
    short_help="load data from file in local to database",
)
@click.option(
    "-f",
    "--filename",
    required=True,
    type=str,
    help="source filename with path after `../data/` folder",
)
@click.option(
    "-t",
    "--target",
    required=True,
    type=str,
    help="target table name short, such as `ai_article_master` -> `aam`",
)
@click.option(
    "--truncate",
    "--no-truncate",
    default=False,
)
@click.option(
    "-c",
    "--compress",
    type=str,
    default="infer",
    help="compress type of file such as `gzip`, `bz2`, `zip`, or `xz`",
)
def load(filename: str, target: str, truncate: bool, compress: str):
    """Load data from local file to target database.

    :usage:     ..> $ python manage.py load -f "<filename>.csv" -t
    "<table-name>"
    """
    from app.app import load_data

    if target.startswith("'"):
        target: str = target.strip("'")
    if filename.startswith("'"):
        filename: str = filename.strip("'")
    if compress.startswith("'"):
        compress: str = compress.strip("'")
    click.echo(f"Start load data from {filename!r} to table {target!r}")
    load_data(
        filename=filename, target=target, truncate=truncate, compress=compress
    )


@click.command(
    name="migrate",
    short_help="migrate catalog from configuration to target database",
)
@click.option(
    "-c",
    "--condition",
    type=str,
    help=(
        "filter condition for get the list of table name "
        "from control pipeline table"
    ),
)
@click.option(
    "--debug",
    type=bool,
    default=False,
    help="migrate all tables with debug mode",
)
def migrate(condition: str, debug: bool):
    """Migrate catalog table from configuration file to target database.

    Example:
    >> $ python manage.py migrate --debug=true.
    """
    os.environ["DEBUG"] = str(debug).capitalize()

    from app.app import migrate_table

    migrate_table()


@click.command(
    name="init",
    short_help="run initial setup to target database",
)
@click.option(
    "--force-drop",
    is_flag=True,
    help="Force drop the Control tables before create",
)
def init(force_drop: bool = False):
    """Run initial setup to target database."""
    from app.controls import (
        push_ctr_setup,
        push_func_setup,
        push_schema_setup,
    )

    click.echo("Start Initial create tables to target database ...")
    push_schema_setup()
    push_func_setup(force_drop=force_drop)
    push_ctr_setup(force_drop=force_drop)


@click.command(
    name="run",
    short_help="run application server",
)
@click.option("--debug", is_flag=True, help="run application with debug mode")
@click.option(
    "-t", "--thread", is_flag=True, help="run application with thread mode"
)
@click.option(
    "--api", is_flag=True, help="run application only the API component"
)
@click.option(
    "--server",
    is_flag=True,
    help="run application with WSGI server",
)
def runserver(
    debug: bool = False,
    thread: bool = False,
    api: bool = False,
    server: bool = False,
):
    """Run WSGI Application or Server which implement by waitress.

    Example:

        $ python manage.py run --debug=true --server=True

    Note:
        - Re-loader will be True if run server in debug mode
            >> app.run(use_reloader=False)
    """
    os.environ["DEBUG"] = str(debug).capitalize()

    from flask import Flask

    from app.app import create_app
    from app.core.utils.logging_ import get_logger

    logger = get_logger(__name__)

    click.echo("Start Deploy the application with input argument")
    logger.debug("Testing logging ...")

    # application factory able to use file `wsgi.py`
    app: Flask = create_app(frontend=(not api))
    if server:
        from waitress import serve

        serve(
            app=app,
            host="0.0.0.0",
            port=5000,
            threads=4,
            url_scheme="http",
            # The URL prefix for adding in the font of main application.
            url_prefix="",
        )
    else:
        app.run(
            **{
                "debug": debug,
                "threaded": thread,
                "host": "0.0.0.0",
                "port": 5000,
                "processes": 1,
            }
        )


@click.command(name="test", short_help="test application server")
def test():
    """Test Application Server."""
    from app.controls import push_testing

    push_testing()
    sys.exit(0)


cli.add_command(load)
cli.add_command(migrate)
cli.add_command(init)
cli.add_command(runserver)
cli.add_command(test)


if __name__ == "__main__":
    cli()
