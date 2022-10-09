import os
# from flask_sqlalchemy import SQLAlchemy
# from sqlalchemy import MetaData
# from flask_migrate import Migrate
# from flask_bcrypt import Bcrypt
# from flask_login import LoginManager
# from flask_mail import Mail
# from flask_limiter import Limiter
# from flask_limiter.util import get_remote_address
# from flask_caching import Cache
# from flask_security import Security
# from flask_security import SQLAlchemyUserDatastore
# from flask_admin import Admin
# from flask_debugtoolbar import DebugToolbarExtension
# from .blueprints.controllers.users.models import User, Role

# convention = {
#     "ix": 'ix_%(column_0_label)s',
#     "uq": "uq_%(table_name)s_%(column_0_name)s",
#     "ck": "ck_%(table_name)s_%(constraint_name)s",
#     "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
#     "pk": "pk_%(table_name)s"
# }

# metadata = MetaData(naming_convention=convention)
# db = SQLAlchemy(metadata=metadata)
# migrate = Migrate()
# bcrypt = Bcrypt()
# login_manager = LoginManager()
# login_manager.login_view = 'users.login'
# login_manager.login_message_category = 'info'
# login_manager.user_loader is registered in main/users.
# login_manager.refresh_view = "auth.reauth"
# login_manager.needs_refresh_message = (
#     u"To protect your account, please reauthenticate to access this page."
# )
# login_manager.needs_refresh_message_category = "info"
# mail = Mail()
# limiter = Limiter(
#     key_func=get_remote_address, default_limits=["200 per day", "50 per hour"]
# )
# cache = Cache()
# user_datastore = SQLAlchemyUserDatastore(db, User, Role)
# security = Security()
# admin = Admin()
# toolbar = DebugToolbarExtension()

