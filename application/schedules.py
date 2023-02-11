# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------
from flask_apscheduler import APScheduler
from apscheduler.events import (
    EVENT_JOB_EXECUTED,
    EVENT_JOB_ERROR,
)
from .utils.logging_ import logging
from .controls import (
    pull_ctr_check_process,
    push_trigger_schedule,
    push_cron_schedule,
    push_retention,
    push_close_ssh,
)

logger = logging.getLogger(__name__)


# @scheduler.task('cron', id='retention_data', day='1st sun', jitter=600)
def schedule_retention_data() -> None:
    """
    Run retention module with schedule
    """
    ps_time: int = push_retention()
    logger.info(f"Success run retention data by schedule with {ps_time} sec.")


# @scheduler.task('cron', id='check_process', minute='*/10', jitter=10, misfire_grace_time=None)
def schedule_check_process() -> None:
    """
    Check process in `ctr_task_process` table and alert if exists 2 status longtime
    """
    ps_false, ps_wait, ps_thread = pull_ctr_check_process()
    logger.info(f"Check process false: {ps_false} waiting: {ps_wait} thread: {ps_thread}")


# @scheduler.task('interval', id='trigger_schedule', minutes=1, jitter=5)
# @scheduler.task('interval', id='trigger_schedule', hours=1, minutes=5, jitter=30)
def schedule_trigger() -> None:
    """
    Run data pipeline trigger from data in `ctr_task_schedule`
    """
    ps_time: int = push_trigger_schedule()
    logger.info(f"End Schedule trigger for run data pipeline with {ps_time} sec.")


# @scheduler.task('cron', id='cron_every_sunday', day_of_week='sun', hour='0', minute='5', jitter=10)
def schedule_cron_every_sunday() -> None:
    """
    Run data pipeline cron job every sunday at 00.05 AM
    """
    ps_time: int = push_cron_schedule(group_name='every_sunday_at_00_05')
    logger.info(f"End Schedule `every_sunday_at_00_05` for run data pipeline with {ps_time} sec.")


# @scheduler.task('cron', id='cron_everyday', hour='20', minute='5', jitter=60)
def schedule_cron_everyday() -> None:
    """
    Run data pipeline cron job everyday at 08.05 PM
    """
    ps_time: int = push_cron_schedule(group_name='everyday_at_08_05')
    logger.info(f"End Schedule `everyday_at_08_05` for run data pipeline with {ps_time} sec.")


# @scheduler.task('cron', id='cron_every_quarter', month='*/3', day='19', hour='0', minute='10', jitter=300)
def schedule_cron_every_quarter() -> None:
    """
    Run data pipeline cron job every quarter at 19th and 00.10 AM
    """
    ps_time: int = push_cron_schedule(group_name='every_quarter_at_19th_00_10')
    logger.info(f"End Schedule `every_quarter_at_19th_00_10` for run data pipeline with {ps_time} sec.")


# @scheduler.task('cron', id='close_ssh', minute='*/5', jitter=10, misfire_grace_time=None)
def schedule_close_ssh() -> None:
    """Close SSH session"""
    import time
    time.sleep(10)
    push_close_ssh()


def my_listener(event):
    if event.exception:
        print('The job crashed :(')
    else:
        print('The job worked :)')


def add_schedules(scheduler: APScheduler):

    scheduler.add_job(
        'retention_data',
        schedule_retention_data,
        trigger='cron',
        day='1st sun',
        jitter=600
    )

    scheduler.add_job(
        'check_process',
        schedule_check_process,
        trigger='cron',
        minute='*/10',
        jitter=10,
        misfire_grace_time=None,
    )

    scheduler.add_job(
        'trigger_schedule',
        schedule_trigger,
        trigger='interval', minutes=1, jitter=5,
    )

    scheduler.add_job(
        'cron_everyday',
        schedule_cron_everyday,
        trigger='cron', hour='20', minute='5', jitter=60
    )

    scheduler.add_job(
        'cron_every_quarter',
        schedule_cron_every_quarter, trigger='cron', month='*/3', day='19', hour='0', minute='10', jitter=300
    )

    scheduler.add_job(
        'close_ssh',
        schedule_close_ssh, trigger='cron', minute='*/5', jitter=10, misfire_grace_time=None
    )

    scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
    return scheduler
