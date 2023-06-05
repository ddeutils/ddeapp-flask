import os
from dateutil import tz
from datetime import timedelta
from pathlib import Path
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from application.core.connections.postgresql import generate_engine
from functools import lru_cache


class BaseConfig(object):
    """Base configuration"""

    # Main configuration
    BASE_PATH: Path = Path(__file__).parent.parent
    APP_INGEST_CHUCK: int = 5

    # Flask
    SECRET_KEY: str = '476e90c596a2311335c553599125bf92'
    DEBUG: bool = eval(os.environ.get('DEBUG'))

    # Flask Bcrypt
    BCRYPT_LOG_ROUNDS: int = 12

    # Flask Mail
    MAIL_SERVER: str = os.environ.get('MAIL_SERVER', 'sandbox.smtp.mailtrap.io')
    MAIL_PORT: int = int(os.environ.get('MAIL_PORT') or 2525)
    MAIL_USERNAME: str = os.environ.get('MAIL_USERNAME', '4fef3ab6172e52')
    MAIL_PASSWORD: str = os.environ.get('MAIL_PASSWORD', '3c1f9a538eada7')
    MAIL_USE_TLS: bool = True
    MAIL_USE_SSL: bool = False
    ADMINS = ['admin@example.com']

    # Flask CORS
    ALLOWED_ORIGINS: list = ['http://localhost:5000', 'https://example.com']

    # Flask WTF Form
    WTF_CSRF_SECRET_KEY: str = '32510asdf7840b0s0v0s78fhasd'
    WTF_CSRF_ENABLED: bool = True

    RESET_TOKEN_EXPIRE_HOURS: int = 1

    # Flask SQLAlchemy
    SQLALCHEMY_ENGINE_OPTIONS: dict = {
        'pool_size': 10,
        'pool_recycle': 60,
        'pool_timeout': 7,
        'pool_pre_ping': True,
    }
    SQLALCHEMY_DATABASE_URI: str = (
        generate_engine()
            .url
            .render_as_string(hide_password=False)
    )
    SQLALCHEMY_TRACK_MODIFICATIONS = False

    # Flask ApSchedule Configuration
    # docs: https://github.com/viniciuschiele/flask-apscheduler/tree/master/examples
    JOBS = [
        {
            "id": "default_job",
            "func": "application.schedules:default_job",
            "args": (1, 2),
            "trigger": "interval",
            "seconds": 10,
            "replace_existing": True,
            "jobstore": "sqlite",
        }
    ]
    SCHEDULER_JOBSTORES = {
        # Save jobs and checkpoint of next schedule
        "sqlite": SQLAlchemyJobStore(
            url=f'sqlite:///{BASE_PATH}/schedulers.db',
            tablename="scheduler",
        )
    }
    SCHEDULER_TIMEZONE = tz.gettz('Asia/Bangkok')
    SCHEDULER_EXECUTORS = {
        "sqlite": {
            "type": "threadpool",
            "max_workers": 20
        },
    }
    SCHEDULER_JOB_DEFAULTS = {
        "coalesce": False,
        "max_instances": 10,
        "misfire_grace_time": 100,
    }
    SCHEDULER_API_ENABLED: bool = False
    SCHEDULER_API_PREFIX: str = "/scheduler"
    SCHEDULER_ENDPOINT_PREFIX: str = "scheduler."
    SCHEDULER_ALLOWED_HOSTS: list = ["*"]

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
        'uiversion': 3,
    }

    # Flask Executor
    EXECUTOR_TYPE: str = 'thread'
    EXECUTOR_MAX_WORKERS: int = 4
    EXECUTOR_PROPAGATE_EXCEPTIONS: bool = True

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
    """Return settings object that match with application environment."""
    config_cls_dict: dict = {
        "development": DevConfig,
        "production": PrdConfig,
        "testing": TestingConfig
    }
    config_name = os.environ.get("APP_ENV", "development")
    config_cls = config_cls_dict[config_name]
    return config_cls()


settings: BaseConfig = get_settings()
