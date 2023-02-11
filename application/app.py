# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import atexit
import json
from typing import Optional
from flask import (
    Flask,
    request,
    jsonify,
    make_response,
    g,
    render_template,
    redirect,
    url_for,
)
from flasgger import LazyJSONEncoder
from flask.logging import default_handler
from celery import Celery
from .utils.logging_ import logging

logger = logging.getLogger(__name__)


def make_celery(app):
    """Create a new Celery object and tie together the Celery config to the app's
    config. Wrap all tasks in the context of the application.

    :param app: Flask app
    :return: Celery app

    :ref:
        - https://testdriven.io/blog/flask-and-celery/
    """
    celery = Celery(app.import_name)
    celery.conf.update(
        app.config.get("CELERY_CONFIG", {})
    )

    class ContextTask(celery.Task):
        def __call__(self, *args, **kwargs):
            with app.app_context():
                return self.run(*args, **kwargs)

    celery.Task = ContextTask
    return celery


def create_app(
        settings_override: Optional[dict] = None,
        frontend: bool = True,
):
    """Create a Flask application using the app factory pattern.

    :param settings_override: Override settings
    :param frontend: Run Flask server included frontend component flag
    :return: Flask app
    """
    app = Flask(
        __name__,

        # setup the template folder
        template_folder='./templates',

        # setup the static folder and url path for request static file,
        # like url_for('static', filename='<filename-path>')
        static_folder='./static',
        static_url_path='/',

        # setup the instance folder
        # instance_path='./instance',
        instance_relative_config=False
    )

    # Set default logging handler from Flask to manual logging.
    app.logger.removeHandler(default_handler)

    # Set the custom Encoder (Inherit it if you need to customize)
    app.json_encoder = LazyJSONEncoder

    # Set configuration from config object that create in `/conf/__init__.py`
    app.config.from_object('conf.DevConfig')

    # update override configuration to app
    if settings_override:
        app.config.update(settings_override)

    # Set the Jinja Template environment config
    app.jinja_env.lstrip_blocks = True
    app.jinja_env.trim_blocks = True

    with app.app_context():

        # Initialize Flask's Extensions that create by 3'rd party
        extensions(app)

        # Initialize Custom Jinja template Filters
        filters(app)

        # Initialize Blueprints for Core Engine
        from .securities import apikey_required
        from .components.api import (
            analytics,
            frameworks,
            ingestion
        )
        app.register_blueprint(frameworks, url_prefix='/api/ai/run')
        app.register_blueprint(analytics, url_prefix='/api/ai/get')
        app.register_blueprint(ingestion, url_prefix='/api/ai')

        # Exempt CSRF with API blueprints
        from .extensions import csrf
        csrf.exempt(frameworks)
        csrf.exempt(analytics)
        csrf.exempt(ingestion)

        if frontend:
            # Initialize Blueprints for Controller Static
            from .components.controllers import (
                errors,
                users,
                admin,
                auth,
            )
            app.register_blueprint(errors)
            app.register_blueprint(users)
            app.register_blueprint(admin, url_prefix='/admin')
            app.register_blueprint(auth, url_prefix='/api/auth')

            # Exempt CSRF to the admin and auth page
            csrf.exempt(admin)
            csrf.exempt(auth)

            # Initialize Blueprints for Backend Static
            from .components.frontend import (
                nodes,
                logs,
                catalogs
            )
            app.register_blueprint(nodes)
            app.register_blueprint(logs)
            app.register_blueprint(catalogs)

        from flask_login import current_user
        from .constants import HTTP_200_OK
        from .extensions import (
            limiter,
            cache,
        )

        @app.before_request
        def before_request():
            """
            use-case:
                if 'logged_in' not in session and request.endpoint != 'login':
                    return redirect(url_for('login'))
            """
            if frontend:
                # Initialize the g parameter for use in any layouts
                # from .components.controllers.main.forms import SearchForm
                # g.search_form = SearchForm()
                ...

        @app.get('/api')
        def api_index():
            logger.info("Start: Application was running ...")
            return jsonify({
                'message': "Success: Application was running ..."
            }), HTTP_200_OK

        @app.get('/apikey')
        @apikey_required
        def apikey():
            logger.info("Success: The AI app was running ...")
            return jsonify({
                'message': "Success: Connect with the apikey, the application was running ..."
            }), HTTP_200_OK

        @app.get("/")
        @app.get("/home")
        @limiter.limit("5/second", override_defaults=False)
        def home():
            return redirect(url_for('nodes.pipeline'))

        @app.get('/about')
        @limiter.limit("5/second", override_defaults=False)
        def about():
            body = render_template('indexes/about.html')
            response = make_response(body)
            response.headers['X-Powered-By'] = 'Not-PHP/1.0'
            return response

        @app.get('/alert')
        def alert():
            return make_response(
                render_template('indexes/alert.html')
            )

        @app.get('/beta')
        @cache.cached(timeout=5, unless=lambda: current_user.is_authenticated)
        def beta():
            from datetime import datetime
            return jsonify({
                "message": f"Coming Soon, {datetime.now()}"
            }), HTTP_200_OK

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
            return jsonify({
                'message': 'Success: Server shutting down ...'
            }), HTTP_200_OK

        # Init function and control framework table to database
        if app.debug:
            from .controls import (
                push_func_setup,
                push_ctr_setup,
            )
            push_func_setup()
            push_ctr_setup()

        @app.teardown_appcontext
        def called_on_teardown(error=None):
            if error:
                logger.warning(f'Tearing down with error, {error}')

            # For resource management
            from .extensions import db
            db.session.remove()
            db.engine.dispose()

        # TODO: Catch error `psycopg2.OperationalError`
        # With after_request we can handle the CORS response headers avoiding to add extra code to our endpoints
        # docs: https://stackoverflow.com/questions/25594893/how-to-enable-cors-in-flask/52875875#52875875
        @app.after_request
        def after_request_func(response):
            origin = request.headers.get('Origin')
            print(f"origin: {origin}")
            if request.method == 'OPTIONS':
                response = make_response()
                response.headers.add('Access-Control-Allow-Credentials', 'true')
                response.headers.add('Access-Control-Allow-Headers', 'Content-Type')
                # response.headers.add('Access-Control-Allow-Headers', 'x-csrf-token')
                response.headers.add(
                    'Access-Control-Allow-Methods',
                    'GET, POST, OPTIONS, PUT, PATCH, DELETE'
                )
            else:
                response.headers.add('Access-Control-Allow-Credentials', 'true')
            if origin:
                response.headers.add('Access-Control-Allow-Origin', origin)
            return response

    return app


def filters(app):
    """Register 0 or more custom Jinja filters on an element

    :param app: Flask application instance
    :return: None

    :example:
        - {{ post.content | makeimg | taguser | truncate(200) | urlize(40, true) }}
    """
    import re
    from datetime import datetime
    from flask import (
        Markup,
        render_template,
    )
    from functools import partial
    from .utils.reusables import to_pascal_case

    @app.template_filter('tag-user')
    def tag_user(text):
        return Markup(re.sub(r'@([a-zA-Z0-9_]+)', r'<a href="/\1">@\1</a>', text))

    @app.template_filter('make-img')
    def make_image(text):
        return re.sub(r'img([a-zA-Z0-9_./:-]+)', r'<img width="100px" src="\1">', text)

    @app.template_filter('dumps')
    def dumps(text):
        return (
            json.dumps(json.loads(text), indent=4, separators=(',', ': '))
            if text else ""
        )

    @app.template_filter('pascal')
    def pascal_case(text, joined: str = ''):
        return to_pascal_case(text, joined)

    def render_partial(name: str, renderer: Optional = None, **context_data) -> Markup:
        return Markup(renderer(name, **context_data))

    @app.context_processor
    def inject_render_partial():
        return {
            'render_partial': partial(render_partial, renderer=render_template),
            'now': datetime.utcnow(),
        }

    # Add global function mapping to Jinja template.
    helpers = {
        'len': len,
        'isinstance': isinstance,
        'str': str,
        'type': type,
        'enumerate': enumerate,
    }
    app.jinja_env.globals.update(**helpers)


def extensions(app):
    """Register 0 or more extensions (mutates the app passed in).

    :param app: Flask application instance
    :return: None
    """
    from .controls import push_ctr_stop_running
    atexit.register(push_ctr_stop_running)

    from .extensions import (
        limiter,
        cache,
        db,
        login_manager,
        bcrypt,
        jwt_manager,
        swagger,
        cors,
        csrf,
        assets,
        scheduler
    )
    limiter.init_app(app)
    cache.init_app(app)
    csrf.init_app(app)
    jwt_manager.init_app(app)
    swagger.init_app(app)
    cors.init_app(app)
    login_manager.init_app(app)
    bcrypt.init_app(app)
    db.init_app(app)

    # Flask Assets
    assets.init_app(app)
    assets.url = app.static_url_path
    assets.directory = app.static_folder
    assets.cache = f"{app.static_folder}/assets/cache"
    # assets.append_path('assets')

    # scheduler.init_app(app)
    # if app.debug:
    #     # Register shutdown schedule
    #     atexit.register(lambda: scheduler.shutdown(wait=False))
    #     scheduler.start()


def load_data(
        filename: str,
        target: str,
        truncate: bool = False,
        compress: Optional[str] = None
) -> None:
    """Load data from local to target database

    :return: None
    """
    from .controls import push_load_file_to_db
    push_load_file_to_db(filename, target, truncate=truncate, compress=compress)


def migrate_table() -> None:
    from .controls import pull_migrate_tables
    pull_migrate_tables()
