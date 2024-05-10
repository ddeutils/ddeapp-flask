# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
import atexit
import json
from pathlib import Path
from typing import Optional

from celery import Celery, Task
from flasgger import LazyJSONEncoder
from flask import (
    Flask,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    url_for,
)
from flask.logging import default_handler
from werkzeug.debug import DebuggedApplication
from werkzeug.middleware.proxy_fix import ProxyFix

from conf import settings

from .core.utils.logging_ import logging

logger = logging.getLogger(__name__)


def make_celery(app: Flask) -> Celery:
    """Create a new Celery object and tie together the Celery config to the
    app's config. Wrap all tasks in the context of the application.

    :param app: Flask app
    :return: Celery app

    :ref:
        - https://testdriven.io/blog/flask-and-celery/
        - https://flask.palletsprojects.com/en/2.3.x/patterns/celery/
    """

    class FlaskTask(Task):
        def __call__(self, *args: object, **kwargs: object) -> object:
            with app.app_context():
                return self.run(*args, **kwargs)

    celery_app = Celery(app.name, task_cls=FlaskTask)
    celery_app.config_from_object(app.config.get("CELERY_CONFIG", {}))
    celery_app.set_default()
    app.extensions["celery"] = celery_app
    return celery_app


def create_app(
    settings_override: Optional[dict] = None,
    frontend: bool = True,
    recreated: bool = False,
):
    """Create a Flask application using the app factory pattern.

        Flask is a WSGI application. A WSGI server is used to run the
    application, converting incoming HTTP requests to the standard WSGI environ,
    and converting outgoing WSGI responses to HTTP responses.
        docs: https://flask.palletsprojects.com/en/2.2.x/deploying/

    :param settings_override: Override settings
    :param frontend: Run Flask server included frontend component flag
    :param recreated: Re-create table in target database
    :type recreated: bool(=False)

    :return: Flask app
    """
    app = Flask(
        __name__,
        # set up the template folder
        template_folder="./templates",
        # set up the static folder and url path for request static file,
        # like url_for('static', filename='<filename-path>')
        static_folder="./static",
        static_url_path="/",
        # set up the instance folder
        # instance_path='./instance',
        instance_relative_config=False,
    )

    # Set default logging handler from Flask to manual logging.
    app.logger.removeHandler(default_handler)

    # Set the custom Encoder (Inherit it if you need to customize)
    app.json_encoder = LazyJSONEncoder

    # Set configuration from config object that create in `/conf/__init__.py`
    app.config.from_object(settings)

    # update override configuration to app
    app.config.update(settings_override or {})

    # Set the Jinja Template environment config
    app.jinja_env.lstrip_blocks = True
    app.jinja_env.trim_blocks = True

    with app.app_context():

        # Initialize Flask's Extensions that create by 3'rd party
        extensions(app)

        # Initialize Custom Jinja template Filters
        filters(app)

        # Initialize Blueprints for Core Engine
        from .blueprints.api import analytics, frameworks, ingestion
        from .securities import apikey_required

        app.register_blueprint(frameworks, url_prefix="/api/ai/run")
        app.register_blueprint(analytics, url_prefix="/api/ai/get")
        app.register_blueprint(ingestion, url_prefix="/api/ai")

        # Exempt CSRF with API blueprints
        from .extensions import csrf

        csrf.exempt(frameworks)
        csrf.exempt(analytics)
        csrf.exempt(ingestion)

        # Exempt Limiter with API blueprints
        from .extensions import limiter

        limiter.exempt(ingestion)

        if frontend:
            # Initialize Blueprints for Controller Static
            from .blueprints.controllers import (
                admin,
                auth,
                errors,
                users,
            )

            app.register_blueprint(errors)
            app.register_blueprint(users)
            app.register_blueprint(admin, url_prefix="/admin")
            app.register_blueprint(auth, url_prefix="/api/auth")

            # Exempt CSRF to the admin and auth page
            csrf.exempt(admin)
            csrf.exempt(auth)

            # Initialize Blueprints for Backend Static
            from .blueprints.frontend import catalogs, logs, nodes

            app.register_blueprint(nodes)
            app.register_blueprint(logs)
            app.register_blueprint(catalogs)

        from flask_login import current_user

        from .core.constants import HTTP_200_OK
        from .extensions import (
            cache,
            limiter,
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

            logger.debug(f'Request "{request.method} {request.url}"')

        @app.get("/api")
        def api_index():
            logger.info("Start: Application was running ...")
            return (
                jsonify({"message": "Success: Application was running ..."}),
                HTTP_200_OK,
            )

        @app.get("/apikey")
        @apikey_required
        def apikey():
            """API Key endpoint returning a JSON message.

            This is using docstrings for specifications.
            ---
            parameters:
                -   name: APIKEYRequest
                    in: header
                    type: string
                    required: true
            response:
                200:
                    description: Successful message
                400:
                    description: APIKEY does not exist in header
                403:
                    description: The provided API key value is not valid
            """
            logger.info("Success: The AI app was running ...")
            return (
                jsonify(
                    {
                        "message": (
                            "Connect with the apikey successful, "
                            "the application was running ..."
                        )
                    }
                ),
                HTTP_200_OK,
            )

        @app.get("/health")
        def health():
            return (
                jsonify({"message": "DFA Flask Postgres started!!!"}),
                HTTP_200_OK,
            )

        @app.get("/")
        @app.get("/home")
        @limiter.limit("5/second", override_defaults=False)
        def home():
            if frontend:
                return redirect(url_for("nodes.pipeline"))
            return redirect(url_for("api_index"))

        @app.get("/about")
        @limiter.limit("5/second", override_defaults=False)
        def about():
            body = render_template("indexes/about.html")
            response = make_response(body)
            response.headers["X-Powered-By"] = "Not-PHP/1.0"
            return response

        @app.get("/alert")
        def alert():
            return make_response(render_template("indexes/alert.html"))

        @app.get("/beta")
        @cache.cached(timeout=5, unless=lambda: current_user.is_authenticated)
        def beta():
            from datetime import datetime

            return (
                jsonify({"message": f"Coming Soon, {datetime.now()}"}),
                HTTP_200_OK,
            )

        @app.route("/opr/shutdown", methods=["GET"])
        @apikey_required
        def shutdown():
            """Shutdown server."""

            def shutdown_server():
                func = request.environ.get("werkzeug.server.shutdown")
                if func is None:
                    raise RuntimeError("Not running with the Werkzeug Server")
                func()

            shutdown_server()
            return (
                jsonify({"message": "Success: Server shutting down ..."}),
                HTTP_200_OK,
            )

        # Add events
        events(app)

        # Init function and control framework table to database
        if recreated:
            from .controls import (
                push_ctr_setup,
                push_func_setup,
                push_schema_setup,
            )

            push_schema_setup()
            push_func_setup()
            push_ctr_setup()

    middleware(app)

    return app


def events(app: Flask) -> None:
    """Add events."""

    warp_app: Flask = app

    @warp_app.teardown_appcontext
    def called_on_teardown(error=None):
        """Called function on app teardown event."""
        if error:
            logger.warning(f"Tearing down with error, {error}")

        # For resource management
        from .extensions import db

        db.session.remove()
        db.engine.dispose()

    # Initial test target database connection
    from psycopg2 import OperationalError as PsycopgOperationalError
    from sqlalchemy import text
    from sqlalchemy.exc import OperationalError

    try:
        from .core.connections.postgresql import generate_engine

        engine = generate_engine()

        # Pre-connect to target database before start application
        with engine.connect() as conn:
            conn.execute(text("select 1"))
    except (OperationalError, PsycopgOperationalError) as err:
        raise RuntimeError(
            "Dose not connect to target database with current connection."
        ) from err

    # TODO: Catch error `psycopg2.OperationalError`
    # With after_request we can handle the CORS response headers
    # avoiding to add extra code to our endpoints
    # docs: (
    #           https://stackoverflow.com/questions/25594893/ -
    #           how-to-enable-cors-in-flask/52875875#52875875
    #       )
    @warp_app.after_request
    def after_request_func(response):
        origin = request.headers.get("Origin")
        # Check method options.
        if request.method == "OPTIONS":
            response = make_response()
            response.headers.add("Access-Control-Allow-Credentials", "true")
            response.headers.add("Access-Control-Allow-Headers", "Content-Type")
            # response.headers.add('Access-Control-Allow-Headers', 'x-csrf-token')
            response.headers.add(
                "Access-Control-Allow-Methods",
                "GET, POST, OPTIONS, PUT, PATCH, DELETE",
            )
        else:
            response.headers.add("Access-Control-Allow-Credentials", "true")

        if origin:
            logger.debug(f"Origin: {origin}")
            # Allow access with domain. If you want ot allow all domain,
            # you can use `*`. The origin that contain 3 contents,
            # {scheme}://{hostname}[:{port}].
            # note: http://example.com and http://example.com:80 is the same
            #       origin because of `http` schema. If it does not have port,
            #       it will be 80.
            #       docs: https://tools.ietf.org/html/rfc6454#section-3.2.1
            # docs: https://acoshift.me/2019/0004-web-cors.html
            response.headers.add("Access-Control-Allow-Origin", origin)
        return response


def filters(app: Flask) -> None:
    """Register 0 or more custom Jinja filters on an element.

    :param app: Flask application instance
    :return: None

    :example:
        - {{ post.content | makeimg | taguser | truncate(200) | urlize(40, true) }}
    """
    import re
    from datetime import datetime
    from functools import partial

    from flask import render_template
    from markupsafe import Markup

    from .core.utils.reusables import to_pascal_case

    warp_app: Flask = app
    logger.debug("Start set up filters to this application ...")

    @warp_app.template_filter("tag-user")
    def tag_user(text):
        return Markup(re.sub(r"@(\w+)", r'<a href="/\1">@\1</a>', text))

    @warp_app.template_filter("make-img")
    def make_image(text):
        return re.sub(
            r"img([a-zA-Z0-9_./:-]+)",
            r'<img width="100px" src="\1">',
            text,
        )

    @warp_app.template_filter("dumps")
    def dumps(text):
        return (
            json.dumps(json.loads(text), indent=4, separators=(",", ": "))
            if text
            else ""
        )

    @warp_app.template_filter("pascal")
    def pascal_case(text, joined: str = ""):
        return to_pascal_case(text, joined)

    def render_partial(
        name: str, renderer: Optional = None, **context_data
    ) -> Markup:
        return Markup(renderer(name, **context_data))

    @warp_app.context_processor
    def inject_render_partial():
        return {
            "render_partial": partial(render_partial, renderer=render_template),
            "now": datetime.utcnow(),
        }

    # Add global function mapping to Jinja template.
    helpers: dict = {
        "len": len,
        "isinstance": isinstance,
        "str": str,
        "type": type,
        "enumerate": enumerate,
    }
    app.jinja_env.globals.update(**helpers)


def extensions(app: Flask) -> None:
    """Register 0 or more extensions (mutates the app passed in).

    :param app: Flask application instance
    :return: None
    """
    logger.debug("Start set up extensions to this application ...")

    from .controls import push_ctr_stop_running

    atexit.register(push_ctr_stop_running)

    from .extensions import (
        assets,
        bcrypt,
        cache,
        cors,
        csrf,
        db,
        jwt_manager,
        limiter,
        login_manager,
        mail,
        scheduler,
    )

    limiter.init_app(app)
    cache.init_app(app)
    csrf.init_app(app)
    jwt_manager.init_app(app)
    # swagger.init_app(app)
    cors.init_app(app)
    login_manager.init_app(app)
    bcrypt.init_app(app)
    db.init_app(app)
    mail.init_app(app)

    # Flask Assets extension
    assets.init_app(app)
    assets.url = app.static_url_path
    assets.directory = app.static_folder
    Path(f"{app.static_folder}/assets/cache").mkdir(parents=True, exist_ok=True)
    assets.cache = f"{app.static_folder}/assets/cache"
    # assets.append_path('assets')

    # Flask APScheduler extension
    scheduler.init_app(app)

    # Register shutdown schedule in not debugging mode.
    if not app.debug:
        from .schedules import add_schedules

        add_schedules(scheduler)
        atexit.register(lambda: scheduler.shutdown(wait=False))
        scheduler.start()

    from .swagger import (
        SWAGGER_UI_BLUEPRINT,
        SWAGGER_URL,
    )

    app.register_blueprint(SWAGGER_UI_BLUEPRINT, url_prefix=SWAGGER_URL)


def middleware(app: Flask):
    """Register 0 or more middleware (mutates the app passed in).

    :param app: Flask application instance
    :return: None
    """
    # Enable the Flask interactive debugger in the browser for development.
    if app.debug:
        app.wsgi_app = DebuggedApplication(app.wsgi_app, evalex=True)

    # Set the real IP address into request.remote_addr when behind a proxy.
    app.wsgi_app = ProxyFix(app.wsgi_app)

    return None


def load_data(
    filename: str,
    target: str,
    truncate: bool = False,
    compress: Optional[str] = None,
) -> None:
    """Load data from local to target database.

    :return: None
    """
    from .controls import push_load_file_to_db

    push_load_file_to_db(filename, target, truncate=truncate, compress=compress)


def migrate_table() -> None:
    from .controls import pull_migrate_tables

    pull_migrate_tables()
