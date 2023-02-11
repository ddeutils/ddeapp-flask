# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import click
import os

os.environ['AI_APP_PATH'] = os.path.abspath(os.path.dirname(__file__))


@click.group()
def cli():
    ...


@click.command(name='load', short_help='load data from file in local to database')
@click.option(
    '-f', '--filename',
    required=True,
    type=str,
    help='source filename with path after `../data/` folder'
)
@click.option(
    '-t', '--target',
    required=True,
    type=str,
    help='target table name short, such as `ai_article_master` -> `aam`'
)
@click.option(
    '--truncate', '--no-truncate',
    default=False,
)
@click.option(
    '-c', '--compress',
    type=str,
    default='infer',
    help='compress type of file such as `gzip`, `bz2`, `zip`, or `xz`'
)
def load(filename: str, target: str, truncate: bool, compress: str):
    """Load data from local file to target database
    :usage:
        >> $ python manage.py load -f "<filename>.csv" -t "<table-name>"
    """
    from application.app import load_data
    if target.startswith("'"):
        target: str = target.strip("'")
    if filename.startswith("'"):
        filename: str = filename.strip("'")
    if compress.startswith("'"):
        compress: str = compress.strip("'")
    click.echo(f"Start load data from {filename!r} to table {target!r}")
    load_data(filename=filename, target=target, truncate=truncate, compress=compress)


@click.command(name='migrate', short_help='migrate catalog from configuration to target database')
@click.option(
    '-c', '--condition',
    type=str,
    help='filter condition for get the list of table name from control pipeline table'
)
@click.option(
    '--debug',
    type=bool,
    default=False,
    help='migrate all tables with debug mode'
)
def migrate(condition: str, debug: bool):
    """Migrate catalog table from configuration file to target database
    :usage:
        >> $ python manage.py migrate --debug=true
    """
    os.environ['DEBUG'] = str(debug).capitalize()

    from application.app import migrate_table
    migrate_table()


@click.command(name='run', short_help='run application server')
@click.option(
    '--debug',
    type=bool,
    default=False,
    help='run server with debug mode'
)
@click.option(
    '-t', '--thread',
    type=bool,
    default=True,
    help='run server with thread mode'
)
@click.option(
    '--api',
    type=bool,
    default=False,
    help='run server only the API component'
)
def runserver(debug: bool, thread: bool, api: bool):
    """Run Application Server
    :usage:
        >> $ python manage.py run --debug=true

    :note:
        - Re-loader will be True if run server in debug mode
          app.run(use_reloader=False)
    """
    os.environ['DEBUG'] = str(debug).capitalize()

    from application.app import create_app

    # application factory able to use file `wsgi.py`
    app = create_app(frontend=(not api))
    app.run(**{
        "debug": debug,
        "threaded": thread,
        "host": '0.0.0.0',
        "port": 5000,
        "processes": 1,
    })


@click.command(name='test', short_help='test application server')
def test():
    """Test Application Server
    """
    ...


cli.add_command(load)
cli.add_command(migrate)
cli.add_command(runserver)


if __name__ == '__main__':
    cli()
