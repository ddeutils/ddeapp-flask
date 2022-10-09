import atexit
import pytz
from typing import Optional
from flask import (
    Flask,
    request,
    jsonify,
)
from flask.logging import default_handler
from .utils.logging_ import logging

logger = logging.getLogger(__name__)


def create_app(settings_override: Optional[str] = None):
    """
    Create a Flask application using the app factory pattern.
    :param settings_override: Override settings
    :return: Flask app
    """
    app = Flask(
        __name__,
        static_folder='static',
        static_url_path='',
        instance_relative_config=False
    )
    app.logger.removeHandler(default_handler)
    app.config.update({
        'SCHEDULER_TIMEZONE': pytz.timezone('Asia/Bangkok'),
        'SCHEDULER_EXECUTORS': {
            "default": {
                "type": "threadpool",
                "max_workers": 20
            }
        },
        'SCHEDULER_JOB_DEFAULTS': {"coalesce": False, "max_instances": 10},
        'SCHEDULER_API_ENABLED': False
    })

    if settings_override:
        app.config.update(settings_override)

    with app.app_context():

        # Initialize Extensions
        extensions(app)

        # Initialize Blueprints
        from .securities import apikey_required
        from .components.framework import frameworks
        from .components.analytic import analytics
        from .components.ingestion import ingestion
        app.register_blueprint(frameworks, url_prefix='/api/ai/run')
        app.register_blueprint(analytics, url_prefix='/api/ai/get')
        app.register_blueprint(ingestion, url_prefix='/api/ai/')

        @app.before_request
        def before_request():
            """
            usecase:
                if 'logged_in' not in session and request.endpoint != 'login':
                    return redirect(url_for('login'))
            """
            pass

        @app.route('/')
        def index():
            logger.info("Start: Application was running ...")
            resp = jsonify({'message': "Success: Application was running ..."})
            resp.status_code = 200
            return resp

        @app.route('/apikey')
        @apikey_required
        def apikey():
            logger.info("Success: The AI app was running ...")
            resp = jsonify({'message': "Success: Connect with the apikey, the application was running ..."})
            resp.status_code = 200
            return resp

        @app.route('/opr/shutdown', methods=['GET'])
        @apikey_required
        def shutdown():
            """shutdown server"""

            def shutdown_server():
                func = request.environ.get('werkzeug.server.shutdown')
                if func is None:
                    raise RuntimeError('Not running with the Werkzeug Server')
                func()

            shutdown_server()
            resp = jsonify({'message': 'Success: Server shutting down ...'})
            resp.status_code = 200
            return resp

        @app.errorhandler(405)
        @app.errorhandler(404)
        def _handle_api_error(exception):
            """handle error code 405, 404"""
            if request.path.startswith('/api/ai/run'):
                logger.error(str(exception))
            return jsonify(error=str(exception)), exception.code

        # Init function and control framework table to database
        if app.debug:
            from .controls import push_func_setup, push_ctr_setup
            push_func_setup()
            push_ctr_setup()

        return app


def extensions(app):
    """
    Register 0 or more extensions (mutates the app passed in).
    :param app: Flask application instance
    :return: None
    """
    from application.schedules import scheduler
    scheduler.init_app(app)

    from application.controls import push_ctr_stop_running
    atexit.register(push_ctr_stop_running)
    atexit.register(lambda: scheduler.shutdown(wait=False))
    scheduler.start()

    # from application.extensions import limiter
    # limiter.init_app(app)


def load_data(filename: str, target: str, truncate: bool = False, compress: Optional[str] = None) -> None:
    """
    Load data from local to target database
    :return: None
    """
    from application.controls import push_load_file_to_db
    push_load_file_to_db(filename, target, truncate=truncate, compress=compress)


def migrate_table() -> None:
    from application.controls import pull_migrate_tables
    pull_migrate_tables()
