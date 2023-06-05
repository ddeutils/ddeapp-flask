# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# -------------------------------------------------------------------------
from flask_apscheduler import APScheduler
from apscheduler.events import (
    EVENT_JOB_EXECUTED,
    EVENT_JOB_ERROR,
)
from application.core.utils.logging_ import logging
from .controls import (
    pull_ctr_check_process,
    push_trigger_schedule,
    push_cron_schedule,
    push_retention,
    push_close_ssh,
)

logger = logging.getLogger(__name__)


def default_job(x, y) -> None:
    logger.info(f"Default job start with args, {x}, and {y}")


def schedule_check_process() -> None:
    """Check process in `ctr_task_process` table and alert
    if exists 2 status longtime
    """
    ps_false, ps_wait, ps_thread = pull_ctr_check_process()
    logger.info(
        f"Check process false: {ps_false} "
        f"waiting: {ps_wait} thread: {ps_thread}"
    )


def schedule_retention_data() -> None:
    """
    Run retention module with schedule
    """
    ps_time: int = push_retention()
    logger.info(f"Success run retention data by schedule with {ps_time} sec.")


def schedule_trigger() -> None:
    """Run data pipeline trigger from data in `ctr_task_schedule`
    """
    ps_time: int = push_trigger_schedule()
    logger.info(
        f"End Schedule trigger for run data pipeline "
        f"with {ps_time} sec."
    )


def schedule_cron_every_sunday() -> None:
    """Run data pipeline cron job every sunday at 00.05 AM
    """
    ps_time: int = push_cron_schedule(group_name='every_sunday_at_00_05')
    logger.info(
        f"End Schedule `every_sunday_at_00_05` for run data pipeline "
        f"with {ps_time} sec."
    )


def schedule_cron_everyday() -> None:
    """
    Run data pipeline cron job everyday at 08.05 PM
    """
    ps_time: int = push_cron_schedule(group_name='everyday_at_08_05')
    logger.info(
        f"End Schedule `everyday_at_08_05` for run data pipeline "
        f"with {ps_time} sec."
    )


def schedule_cron_every_quarter() -> None:
    """
    Run data pipeline cron job every quarter at 19th and 00.10 AM
    """
    ps_time: int = push_cron_schedule(group_name='every_quarter_at_19th_00_10')
    logger.info(
        f"End Schedule `every_quarter_at_19th_00_10` "
        f"for run data pipeline with {ps_time} sec."
    )


def schedule_close_ssh() -> None:
    """Close SSH session"""
    import time
    time.sleep(10)
    push_close_ssh()


def listener_log(event):
    """Listener"""
    if event.exception:
        logger.warning('The job crashed :(')
    else:
        logger.info('The job worked :)')


def add_schedules(scheduler: APScheduler):
    """Add job schedules without decorator functions"""

    # scheduler.add_job(
    #     'retention_data',
    #     schedule_retention_data,
    #     trigger='cron',
    #     day='1st sun',
    #     jitter=600
    # )

    scheduler.add_job(
        id='check_process',
        func=schedule_check_process,
        trigger='cron',
        minute='*/10',
        jitter=10,
        misfire_grace_time=None,

        # Usage configuration
        max_instances=1,

        # Option for using job store
        jobstore='sqlite',
        replace_existing=True,
    )

    scheduler.add_job(
        id='trigger_schedule',
        func=schedule_trigger,
        trigger='interval',
        minutes=1,
        jitter=5,

        # Option for using job store
        jobstore='sqlite',
        replace_existing=True,
    )

    scheduler.add_job(
        id='cron_everyday',
        func=schedule_cron_everyday,
        trigger='cron',
        hour='20',
        minute='5',
        jitter=60,

        # Option for using job store
        jobstore='sqlite',
        replace_existing=True,
    )

    scheduler.add_job(
        id='cron_every_quarter',
        func=schedule_cron_every_quarter,
        trigger='cron',
        month='*/3',
        day='19',
        hour='0',
        minute='10',
        jitter=300,

        # Option for using job store
        jobstore='sqlite',
        replace_existing=True,
    )

    # scheduler.add_job(
    #     id='close_ssh',
    #     func=schedule_close_ssh,
    #     trigger='cron',
    #     minute='*/5',
    #     jitter=10,
    #     misfire_grace_time=None
    # )

    scheduler.add_listener(
        listener_log, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR
    )
    return scheduler
