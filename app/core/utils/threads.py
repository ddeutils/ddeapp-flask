import asyncio
import concurrent.futures
import ctypes
import inspect
import os
import threading
import time
import typing
from typing import Any

threadList: list = []
MAX_THREADS: int = int(os.getenv("THREAD_MAX", "5"))
# TODO: research from
#  https://stackoverflow.com/questions/62469183/multithreading-inside-multiprocessing-in-python


def worker(_id, sleep: int):
    """Worker function for test background task"""
    print(f"The background process was started with id {_id!r} ...")
    for _ in range(sleep):
        print(f"Sleep with step {_}")
        time.sleep(1)
    return f"process id: {_id!r} run successful with sleep {sleep}"


def _async_raise(tid, exc_type):
    """
    Raises an exception in the threads with id tid
    """
    if not inspect.isclass(exc_type):
        raise TypeError("Only types can be raised (not instances)")
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(tid),
        ctypes.py_object(exc_type),
    )
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        """
        if it returns a number greater than one, you're in trouble,
        and you should call it again with exc=NULL to revert the effect
            >> ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), 0)
        """
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


class ThreadWithControl(threading.Thread):
    """We want to create threading class that can control maximum background
    agent and result after complete
    - Get return output from threading function
    - A thread class that supports raising an exception in the thread from
    another thread.
    :usage:
        >> _thread = ThreadWithControl(target=lambda a: a * 2, args=(2, ))
        >> _thread.daemon = True
        >> _thread.start()
        >> print(_thread.join())
        4
    """

    # TODO: `threadLimiter` per data pipeline use case flow
    LIMITER = threading.BoundedSemaphore(MAX_THREADS)

    def __init__(
        self,
        group=None,
        target=None,
        name=None,
        args=(),
        kwargs=None,
        *,
        daemon=None,
    ):
        # inherit variables from `super()` or `threading.Thread`
        self._return = None
        self._target = None
        self._args = None
        self._kwargs = None
        super().__init__(
            group=group,
            target=target,
            name=name,
            args=args,
            kwargs=kwargs,
            daemon=daemon,
        )
        self._stop_event = threading.Event()
        self.check_count = 0

    def run(self):
        self.LIMITER.acquire()
        try:
            if self._target:
                self._return = self._target(*self._args, **self._kwargs)
        finally:
            del self._target, self._args, self._kwargs
            self.LIMITER.release()

    def join(self, *args) -> Any:
        # same as `threading.Thread.join(self, *args)`
        super().join(*args)
        return self._return

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def _get_my_tid(self):
        """
        determines this (self's) thread id

        CAREFUL: this function is executed in the context of the caller
        thread, to get the identity of the thread represented by this
        instance.
        """
        if not self.is_alive():
            raise threading.ThreadError("the thread is not active")

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # no, look for it in the _active dict
        for thread_id, thread_obj in threading._active.items():
            if thread_obj is self:
                self._thread_id = thread_id
                return thread_id

        raise AssertionError("could not determine the thread's id")

    def raise_exc(self, exc_type):
        """
        Raises the given exception type in the context of this thread.

        If the thread is busy in a system call (time.sleep(),
        socket.accept(), ...), the exception is simply ignored.

        If you are sure that your exception should terminate the thread,
        one way to ensure that it works is:

            t = ThreadWithExc(...)
            ...
            t.raise_exc(SomeException)
            while t.isAlive():
                time.sleep(0.1)
                t.raise_exc(SomeException)

        If the exception is to be caught by the thread, you need a way to
        check that your thread has caught it.

        CAREFUL: this function is executed in the context of the
        caller thread, to raise an exception in the context of the
        thread represented by this instance.
        """
        _async_raise(self._get_my_tid(), exc_type)

    def terminate(self):
        """
        must raise the SystemExit type, instead of a SystemExit() instance
        due to a bug in PyThreadState_SetAsyncExc
        """
        self.raise_exc(SystemExit)


class _WorkItem:
    """concurrent.futures.thread.py"""

    def __init__(self, future, fn, args, kwargs, *, debug=None):
        self._debug = debug
        self.future = future
        self.fn = fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        if self._debug:
            print("ExitThread._WorkItem run")
        if not self.future.set_running_or_notify_cancel():
            return
        try:
            coroutine = None
            if asyncio.iscoroutinefunction(self.fn):
                coroutine = self.fn(*self.args, **self.kwargs)
            elif asyncio.iscoroutine(self.fn):
                coroutine = self.fn
            if coroutine is None:
                result = self.fn(*self.args, **self.kwargs)
            else:
                result = asyncio.run(coroutine)
            if self._debug:
                print("_WorkItem done")
        except BaseException as exc:
            self.future.set_exception(exc)
            # Break a reference cycle with the exception 'exc'
            self = None
        else:
            self.future.set_result(result)


class ExitThread:
    """Like a stoppable thread
    Using coroutine for target then exit before running may cause
    RuntimeWarning.
    """

    def __init__(
        self,
        target: typing.Union[typing.Coroutine, typing.Callable] = None,
        args=(),
        kwargs=None,
        *,
        daemon=None,
        debug=None,
    ):
        if kwargs is None:
            kwargs = {}
        self._debug = debug
        self._parent_thread = threading.Thread(
            target=self._parent_thread_run,
            name="ExitThread_parent_thread",
            daemon=daemon,
        )
        self._child_daemon_thread = None
        self.result_future = concurrent.futures.Future()
        self._workItem = _WorkItem(
            self.result_future, target, args, kwargs, debug=debug
        )
        self._parent_thread_exit_lock = threading.Lock()
        self._parent_thread_exit_lock.acquire()
        # This value will be True if it's done
        self._parent_thread_exit_lock_released = False
        self._started = False
        self._exited = False
        self.result_future.add_done_callback(
            self._release_parent_thread_exit_lock
        )

    def _parent_thread_run(self):
        self._child_daemon_thread = threading.Thread(
            target=self._child_daemon_thread_run,
            name="ExitThread_child_daemon_thread",
            daemon=True,
        )
        self._child_daemon_thread.start()
        # Block manager thread
        self._parent_thread_exit_lock.acquire()
        self._parent_thread_exit_lock.release()
        if self._debug:
            print("ExitThread._parent_thread_run exit")

    def _release_parent_thread_exit_lock(self, _future):
        if self._debug:
            print(
                f"ExitThread._release_parent_thread_exit_lock "
                f"{self._parent_thread_exit_lock_released} {_future}"
            )
        if not self._parent_thread_exit_lock_released:
            self._parent_thread_exit_lock_released = True
            self._parent_thread_exit_lock.release()

    def _child_daemon_thread_run(self):
        self._workItem.run()

    def start(self):
        if self._debug:
            print(f"ExitThread.start {self._started}")
        if not self._started:
            self._started = True
            self._parent_thread.start()

    def exit(self):
        if self._debug:
            print(
                f"ExitThread.exit exited: {self._exited} "
                f"lock_released: {self._parent_thread_exit_lock_released}"
            )
        if self._parent_thread_exit_lock_released:
            return
        if not self._exited:
            self._exited = True
            if not self.result_future.cancel() and self.result_future.running():
                self.result_future.set_exception(
                    concurrent.futures.CancelledError()
                )


class BackgroundTasks(threading.Thread):
    """Class that runs background tasks for a flask application.
    Args:
        threading.Thread: Creates a new thread.
    """

    def __init__(
        self,
        app,
    ):
        """Create a background tasks object that runs periodical tasks in the
        background of a flask application.
        Args:
            app: Flask application object.
        """
        super().__init__()
        self.app = app

    def run(self) -> None:
        # Use the current application context and start the timeloop service.
        with self.app.app_context():
            self.synchronize_applications()

    def synchronize_applications(self) -> None: ...
