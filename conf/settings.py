import os
import pytz
from datetime import timedelta
from pathlib import Path
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from application.utils.database import generate_engine
from functools import lru_cache


class BaseConfig(object):
    """Base configuration"""

    # Main configuration
    BASE_PATH = Path(__file__).parent.parent

    # Flask
    SECRET_KEY: str = '476e90c596a2311335c553599125bf92'
    DEBUG: bool = eval(os.environ.get('DEBUG'))

    # Flask Mail
    MAIL_SERVER = os.environ.get('MAIL_SERVER', 'sandbox.smtp.mailtrap.io')
    MAIL_PORT = int(os.environ.get('MAIL_PORT') or 2525)
    MAIL_USERNAME = os.environ.get('MAIL_USERNAME', '4fef3ab6172e52')
    MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD', '3c1f9a538eada7')
    MAIL_USE_TLS = True
    MAIL_USE_SSL = False
    ADMINS = ['admin@example.com']

    # Flask CORS
    ALLOWED_ORIGINS: list = ['http://localhost:5000', 'https://example.com']

    # Flask WTF Form
    WTF_CSRF_SECRET_KEY = '32510asdf7840b0s0v0s78fhasd'
    WTF_CSRF_ENABLED = True

    RESET_TOKEN_EXPIRE_HOURS = 1

    # Flask SQLAlchemy
    SQLALCHEMY_ENGINE_OPTIONS = {
        'pool_size': 10,
        'pool_recycle': 60,
        'pool_timeout': 7,
        'pool_pre_ping': True,
    }
    SQLALCHEMY_DATABASE_URI: str = str(generate_engine().url)
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Flask Schedule Configuration
    # docs: https://github.com/viniciuschiele/flask-apscheduler/tree/master/examples
    # JOBS = [
    #     {
    #         "id": "job1",
    #         "func": "advanced:job1",
    #         "args": (1, 2),
    #         "trigger": "interval",
    #         "seconds": 10,
    #     }
    # ]
    # SCHEDULER_JOBSTORES = {"default": SQLAlchemyJobStore(url=str(generate_engine().url))}
    SCHEDULER_TIMEZONE = pytz.timezone('Asia/Bangkok'),
    SCHEDULER_EXECUTORS = {
        "default": {
            "type": "threadpool",
            "max_workers": 20
        },
    }
    SCHEDULER_JOB_DEFAULTS = {"coalesce": False, "max_instances": 10}
    SCHEDULER_API_ENABLED = False

    # Flask Cache Configuration
    CACHE_TYPE = "SimpleCache"
    CACHE_DEFAULT_TIMEOUT = 300

    # Flask Celery Configuration
    CELERY_CONFIG = {
        'broker_url': 'redis://localhost:6379/0',
        'result_backend': 'redis://localhost:6379/0',
    }

    # Flask JWT Extension
    JWT_SECRET_KEY = '476e90c596a2311335c553599125bf92'
    JWT_ACCESS_TOKEN_EXPIRES = timedelta(hours=1)
    JWT_REFRESH_TOKEN_EXPIRES = timedelta(days=30)
    JWT_COOKIE_CSRF_PROTECT = False

    # Flask Swagger
    SWAGGER = {
        'title': "AI API",
        'uiversion': 3
    }


class DevConfig(BaseConfig):
    """Development environment configuration"""
    # SQLALCHEMY_ENGINE_OPTIONS: dict = {}


class SitConfig(BaseConfig):
    """SIT environment configuration"""


class PrdConfig(BaseConfig):
    """Production environment configuration"""


class TestingConfig(BaseConfig):
    """Test environment configuration"""


@lru_cache()
def get_settings():
    config_cls_dict: dict = {
        "development": DevConfig,
        "production": PrdConfig,
        "testing": TestingConfig
    }
    config_name = os.environ.get("APP_ENV", "development")
    config_cls = config_cls_dict[config_name]
    return config_cls()


settings: BaseConfig = get_settings()
