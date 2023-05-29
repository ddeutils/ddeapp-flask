# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

from sqlalchemy import MetaData
import flask_sqlalchemy
from flask_sqlalchemy import SQLAlchemy
from flask_sqlalchemy.model import Model
from flask_login import LoginManager
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_caching import Cache
from flask_bcrypt import Bcrypt
from flask_wtf import CSRFProtect
from flask_jwt_extended import JWTManager
from flasgger import Swagger
from flask_cors import CORS
from flask_assets import Environment
from flask_apscheduler import APScheduler
from flask_mail import Mail

# from flask_migrate import Migrate
# from flask_admin import Admin
# from flask_debugtoolbar import DebugToolbarExtension

from .utils.database import env
from .assets import bundles
from .swagger import swagger_config, swagger_template
from conf import settings


# Flask-SQLAlchemy ===============================================================
conventions = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}
metadata = MetaData(
    naming_convention=conventions,
    schema=env.get('AI_SCHEMA', 'ai')
)


class GlobalQuery(flask_sqlalchemy.BaseQuery):
    """Custom Query Class for all model in this application"""

    def get_or(self, identity, default=None):
        return self.get(identity) or default


class CustomModel(Model):
    """Custom Model Class for all model in this application"""

    @classmethod
    def get_column(cls, column):
        return getattr(cls, column)


db = SQLAlchemy(
    metadata=metadata,
    session_options={'autocommit': False},
    query_class=GlobalQuery,
    model_class=CustomModel
)
# migrate = Migrate()

# Flask-Mail ===============================================================
mail = Mail()
# admin = Admin()
# toolbar = DebugToolbarExtension()

# Flask-Bcrypt ===============================================================
bcrypt = Bcrypt()

# Flask-Login ===============================================================
login_manager = LoginManager()
login_manager.login_view = 'users.login_get'
login_manager.login_message = 'Please log in for access that path.'
login_manager.login_message_category = 'info'
# login_manager.user_loader is registered in main/users.
# login_manager.refresh_view = "auth.reauth"
login_manager.needs_refresh_message = (
    u"To protect your account, please re-authenticate to access this page."
)
login_manager.needs_refresh_message_category = "info"

# Flask-Cache ===============================================================
cache = Cache()

# Flask-Limiter ===============================================================
limiter = Limiter(
    key_func=get_remote_address,
    default_limits=[
        # "200 per day",
        # "50 per hour",
        # "100 per hour",
    ],
    storage_uri="memory://",
)

# Flask-Limiter ===============================================================
jwt_manager = JWTManager()

# Flask-Limiter ===============================================================
csrf = CSRFProtect()

# Flask-Limiter ===============================================================
swagger = Swagger(
    config=swagger_config,
    template=swagger_template,
    # sanitizer=NO_SANITIZER
)

# Flask-Limiter ===============================================================
cors = CORS(
    resources={
        r"/api/*": {"origins": "*"}
    },

    # By default, Flask-CORS does not allow cookies to be submitted across sites,
    # since it has potential security implications. If you wish to enable cross-site
    # cookies, you may wish to add some sort of CSRF protection to keep you and your
    # users safe.
    supports_credentials=False
)

# Flask-Assets ===============================================================
assets = Environment()
assets.register(bundles)

# Flask-APScheduler ===============================================================
scheduler = APScheduler()
