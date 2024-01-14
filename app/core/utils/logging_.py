# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------

import datetime
import json
import logging
import logging.handlers
import os
import re
import time
from logging.config import dictConfig
from typing import Optional

import pytz
import requests

from ..utils.reusables import must_bool

if not os.getenv('DEBUG'):
    os.environ['DEBUG']: str = 'True'

ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
)
LOG_DIR = os.path.join(ROOT, 'logs')
DEBUG: bool = must_bool(os.getenv("DEBUG", "False"))
LOG_CONF: bool = must_bool(os.getenv("LOG_CONF", "False"))

if DEBUG and not os.path.exists(LOG_DIR):
    os.mkdir(LOG_DIR)


class Message:
    def __init__(self, fmt, args):
        self.fmt = fmt
        self.args = args

    def __str__(self):
        return self.fmt.format(*self.args)


class StyleAdapter(logging.LoggerAdapter):
    def __init__(self, logger, extra=None):
        super().__init__(logger, extra or {})

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            msg, kwargs = self.process(msg, kwargs)
            self.logger._log(level, Message(msg, args), (), **kwargs)


class UTCFormatter(logging.Formatter):
    """override logging.Formatter to use an aware datetime object"""
    # converter = time.gmtime

    def converter(self, timestamp):
        # Create datetime in UTC
        dt = datetime.datetime.fromtimestamp(timestamp, tz=pytz.UTC)
        # Change timezone of datetime
        return dt.astimezone(pytz.timezone('Asia/Bangkok'))

    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.isoformat(timespec='milliseconds')
            except TypeError:
                s = dt.isoformat()
        return s


class HTTPSlackHandler(logging.Handler):
    """
    Send logging to slack application
    """
    def emit(self, record):
        log_entry = self.format(record)
        json_text = json.dumps({"text": log_entry})
        url = 'https://hooks.slack.com/services/<org_id>/<api_key>'
        return requests.post(
            url,
            json_text,
            headers={"Content-type": "application/json"}
        ).content


class NoConsoleFilter(logging.Filter):
    def __init__(self):
        super().__init__()

    def filter(self, record):
        return (
            not (record.levelname == logging.INFO) & ('no-console' in record.msg)
        )


class MyTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    """
    change time-rotating file format
    """
    def __init__(self, *args, **kwargs):
        self.baseFilename = None
        self.backupCount = None
        super().__init__(*args, **kwargs)
        self.suffix = "%Y%m%d"
        self.extMatch = re.compile(r"^\d{4}\d{2}\d{2}$")

    def getFilesToDelete(self):
        """
        CUT, PASTE AND .... HACK
        """
        _dirname, basename = os.path.split(self.baseFilename)
        file_names = os.listdir(_dirname)
        result: list = []
        ends = f"_{basename}.log"
        elen = len(ends)
        for file_name in file_names:
            if file_name[-elen:] == ends:
                date = file_name[-elen:]
                if self.extMatch.match(date):
                    result.append(os.path.join(_dirname, file_name))
        result.sort()
        return [] if len(result) < self.backupCount else result[:len(result) - self.backupCount]

    def doRollover(self):
        """
        CUT AND PAST FROM TimedRotatingFileHandler
        customize file name by prefix instead suffix

        scenarios
        ---------
            datetime_now exists := DST kicks in before next rollover, so we need to deduct an hour
            datetime_now not exists := DST bows out before next rollover, so we need to add an hour
        """
        if self.stream:
            self.stream.close()
            self.stream = None
        # get the time that this sequence started at and make it a TimeTuple
        current_time = int(time.time())
        datetime_now = time.localtime(current_time)[-1]
        t = self.rolloverAt - self.interval
        if self.utc:
            timeTuple = time.gmtime(t)
        else:
            timeTuple = time.localtime(t)
            dstThen = timeTuple[-1]
            if datetime_now != dstThen:
                addend = 3600 if datetime_now else -3600
                timeTuple = time.localtime(t + addend)
        dfn = time.strftime("%Y%m%d", timeTuple) + "_" + self.baseFilename + ".log"
        if os.path.exists(dfn):
            os.remove(dfn)
        # Issue 18940: A file may not have been created if delay is True.
        if os.path.exists(self.baseFilename):
            os.rename(self.baseFilename, dfn)
        if self.backupCount > 0:
            for s in self.getFilesToDelete():
                os.remove(s)
        if not self.delay:
            self.stream = self._open()
        _rolloverAt = self.computeRollover(current_time)
        while _rolloverAt <= current_time:
            _rolloverAt = _rolloverAt + self.interval
        # If DST changes and midnight or weekly rollover, adjust for this.
        if (self.when == 'MIDNIGHT' or self.when.startswith('W')) and not self.utc:
            dstAtRollover = time.localtime(_rolloverAt)[-1]
            if datetime_now != dstAtRollover:
                addend = 3600 if datetime_now else -3600
                _rolloverAt += addend
        self.rolloverAt = _rolloverAt


class RefmtTimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    """
    change time-rotating file format
        - fileName.log
        - fileName.20211101_110010.log
        - fileName.20211101_110012.log
        - fileName.20211101_110013.log
    """
    def __init__(self, maxBytes=0, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.suffix = "%Y%m%d_%H%M%S"
        self.extMatch = re.compile(r"^\d{4}\d{2}\d{2}_\d{2}\d{2}\d{2}$")
        self.maxBytes = maxBytes
        self.namer = self.re_namer

    @staticmethod
    def re_namer(default_name):
        """This will be called when doing the log rotation
        default_name is the default filename that would be assigned, e.g. Rotate_Test.log.YYYY-MM-DD
        Do any manipulations to that name here, for example this changes the name to Rotate_Test.YYYY-MM-DD.log
        """
        default_dir_name, default_file_name = os.path.split(default_name)
        base_filename, ext, date = default_file_name.split(".")
        return os.path.join(default_dir_name, f"{base_filename}.{date}.{ext}")

    def shouldRollover(self, record):
        """
        Determine if rollover should occur.

        Basically, see if the supplied record would cause the file to exceed
        the size limit we have.
        """
        if self.stream is None:
            self.stream = self._open()
        if self.maxBytes > 0:
            msg = "%s\n" % self.format(record)
            # due to non-posix-compliant Windows feature
            self.stream.seek(0, 2)
            if self.stream.tell() + len(msg) >= self.maxBytes:
                return 1
        return 1 if int(time.time()) >= self.rolloverAt else 0

    def getFilesToDelete(self):
        """
        rewrite get file name logic for delete if list of files more than backupCount
        """
        _dirname, _base_name = os.path.split(self.baseFilename)
        file_names = os.listdir(_dirname)
        result: list = []
        starts, ends = _base_name.split(".")[0], f'.{_base_name.split(".")[-1]}'
        start_len, end_len = len(starts), len(ends)
        for file_name in file_names:
            if file_name[-end_len:] == ends and file_name[:start_len] == starts:
                logging.info(file_name)
                date = file_name[start_len + 1:-end_len]
                if self.extMatch.match(date):
                    result.append(os.path.join(_dirname, file_name))
        result.sort()
        return [] if len(result) < self.backupCount else result[:len(result) - self.backupCount]


class DailyRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """
    change rotating pattern
        - 2016-10-05.log.alias
        - 2016-10-05.log.alias.1
        - 2016-10-05.log.alias.2
        - 2016-10-06.log.alias
        - 2016-10-06.log.alias.1
        - 2016-10-07.log.alias.1
    """
    def __init__(
            self,
            alias,
            basedir,
            mode='a',
            maxBytes: int = 0,
            backupCount=0,
            encoding=None,
            delay=0
    ):
        """
        @summary:
        Set self.baseFilename to date string of today.
        The handler create logFile named self.baseFilename
        """
        self.today_ = None
        self.maxBytes: Optional[int] = None
        self.basedir_ = basedir
        self.alias_ = alias
        self.baseFilename = self.getBaseFilename()
        logging.handlers.RotatingFileHandler.__init__(
            self,
            self.baseFilename,
            mode,
            maxBytes,
            backupCount,
            encoding,
            delay
        )

    def getBaseFilename(self):
        """
        @summary: Return logFile name string formatted to "today.log.alias"
        """
        self.today_ = datetime.date.today()
        basename_ = self.today_.strftime("%Y-%m-%d") + ".log" + '.' + self.alias_
        return os.path.join(self.basedir_, basename_)

    def shouldRollover(self, record):
        """
        @summary:
        Rollover happen
        1. When the logFile size is get over maxBytes.
        2. When date is changed.

        @see: BaseRotatingHandler.emit
        """

        if self.stream is None:
            self.stream = self._open()

        if self.maxBytes > 0:
            msg = "%s\n" % self.format(record)
            self.stream.seek(0, 2)
            if self.stream.tell() + len(msg) >= self.maxBytes:
                return 1

        if self.today_ != datetime.date.today():
            self.baseFilename = self.getBaseFilename()
            return 1
        return 0


class PickableLoggerAdapter:
    """
    pickle module
    """
    def __init__(self, name):
        self.name = name
        self.logger = _create_logger(name)

    def __getstate__(self):
        """
        Method is called when pickle dumps an object.

        Returns
        -------
        Dictionary, representing the object state to be pickled. Ignores
        the self.logger field and only returns the logger name.
        """
        return {'name': self.name}

    def __setstate__(self, state):
        """
        Method is called when pickle loads an object. Retrieves the name and
        creates a logger.

        Parameters
        ----------
        state - dictionary, containing the logger name.

        """
        self.name = state['name']
        self.logger = _create_logger(self.name)

    def debug(self, msg, *args, **kwargs):
        self.logger.debug(msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self.logger.info(msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self.logger.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self.logger.error(msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self.logger.exception(msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self.logger.critical(msg, *args, **kwargs)

    def log(self, level, msg, *args, **kwargs):
        self.logger.log(level, msg, *args, **kwargs)

    def isEnabledFor(self, level):
        return self.logger.isEnabledFor(level)


def _create_logger(name):
    return StyleAdapter(logging.getLogger(name))


def get_logger(name):
    return PickableLoggerAdapter(name)


dictConfig({
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "default": {
            '()': UTCFormatter,
            # "format": (
            #     "%(asctime)s.%(msecs)04d (%(name)s:%(threadName)s"
            #     "[%(thread)d]) - %(lineno)d %(levelname)-8s: %(message)s"
            # ),
            # "format": (
            #     "%(asctime)s (%(thread)06d) %(levelname)-8s|: "
            #     "%(message)s"
            # ),
            "format": (
                "%(asctime)s (%(thread)06d) %(levelname)-8s|%(name)s: "
                "%(message)s"
            ),
            "datefmt": '%Y-%m-%d %H:%M:%S'
        },
        "access": {
            "format": (
                "%(asctime)s.%(msecs)04d (%(name)s:%(threadName)s"
                "[%(thread)d]) - %(lineno)d %(levelname)-8s: "
                "%(message)s"
            ),
            "datefmt": '%Y-%m-%d %H:%M:%S'
        },
        "fsting": {
            "format": (
                '{asctime}.{msecs:.0f} {levelname:<8s} '
                '({name}:{threadName}) {message}'
            ),
            "datefmt": '%Y-%m-%d %H:%M:%S',
            "style": '{',
            "validate": True
        }
    },

    "filters": {
        "no_console_filter": {
            (): "logging_.NoConsoleFilter"
        }
    },

    "handlers": {

        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "default",
            # Stream default is "ext://sys.stdout"
            "stream": "ext://flask.logging.wsgi_errors_stream",
        },

        # "framework_file": {
        #     "class": "logging_.RefmtTimedRotatingFileHandler",
        #     "level": "DEBUG",
        #     "formatter": "default",
        #     "filename": os.path.join(LOG_DIR, "framework.log"),
        #     # "maxBytes": 1024,
        #     "when": "m",
        #     "interval": 1,
        #     "backupCount": 5,
        #     "encoding": "utf8",
        # },

        # # Example for alert with TimeRotatingFile
        # "app_file": {
        #     "level": "DEBUG",
        #     "class": "logging.handlers.TimedRotatingFileHandler",
        #     "formatter": "default",
        #     "filename": os.path.join(LOG_DIR, "app.log"),
        #     "when": "m",
        #     "interval": 2,
        #     "backupCount": 5,
        #     "encoding": "utf8"
        # },

        # # Example for alert to slack
        # "slack": {
        #     "class": "logging_.HTTPSlackHandler",
        #     "formatter": "default",
        #     "level": "ERROR",
        # },

        # # Example for alert to email
        # "email": {
        #     "class": "logging.handlers.SMTPHandler",
        #     "formatter": "default",
        #     "level": "ERROR",
        #     "mailhost": ("smtp.example.com", 587),
        #     "fromaddr": "devops@example.com",
        #     "toaddrs": ["receiver@example.com", "receiver2@example.com"],
        #     "subject": "Error Logs",
        #     "credentials": ("username", "password"),
        # },

        # # Example for alert with RotatingFile
        # "error_file": {
        #     "class": "logging.handlers.RotatingFileHandler",
        #     "formatter": "default",
        #     "filename": os.path.join(LOG_DIR, "error.log"),
        #     "maxBytes": 10000,
        #     "backupCount": 10,
        #     "delay": "True",
        #     "filters": ["no_console_filter"]
        # },

    },
    "loggers": {

        "apscheduler.scheduler": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False
        },

        "application.components.api.analytic.tasks": {
            "handlers": ["console"],
            "level": "DEBUG" if DEBUG else "INFO",
            "propagate": False
        },

        "application.components.api.framework": {
            "handlers": ["console"],
            "level": "DEBUG" if DEBUG else "INFO",
            "propagate": False
        },

        "application.utils": {
            "handlers": ["console"],
            "level": "INFO",
            "propagate": False
        },

        "paramiko.transport": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False
        },

        "apscheduler.executors.default": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False
        },

        "matplotlib": {
            "handlers": ["console"],
            "level": "ERROR",
            "propagate": False
        },

        "waitress": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": False
        },

        "werkzeug": {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": False
        }

    },
    "root": {
        "level": "DEBUG" if DEBUG else "INFO",
        "handlers": ["console"]
    }
})
