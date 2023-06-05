import threading
import uuid
from functools import wraps
from werkzeug.exceptions import (
    HTTPException,
    InternalServerError,
)
from flask import (
    current_app,
    request,
)
from flask_mail import Message
from application.core.errors import AllExceptions
from application.core.utils.logging_ import get_logger


logger = get_logger(__name__)
TASKS: dict = {}


def flask_async(f):
    """This decorator transforms a sync route to asynchronous by running it
    in a background thread.

    :example:

        ..> @app.route('/foo')
        ... @flask_async
        ... def foo():
        ...     time.sleep(10)
        ...     return {'Result': True}


    docs: https://stackoverflow.com/questions/40989671/background-tasks-in-flask
    """
    @wraps(f)
    def wrapped(*args, **kwargs):
        def task(_app, environ):
            # Create a request context similar to that of the original request
            with _app.request_context(environ):
                try:
                    # Run the route function and record the response
                    TASKS[task_id]['result'] = f(*args, **kwargs)
                except HTTPException as e:
                    TASKS[task_id]['result'] = (
                        current_app.handle_http_exception(e)
                    )
                except AllExceptions:
                    # The function raised an exception, so we set a 500 error
                    TASKS[task_id]['result'] = InternalServerError()
                    if current_app.debug:
                        # We want to find out if something happened so reraise
                        raise

        # Assign an id to the asynchronous task
        task_id = uuid.uuid4().hex

        # Record the task, and then launch it
        TASKS[task_id] = {
            'task': threading.Thread(
                target=task,

                # If you need to access the underlying object that is proxied,
                # use the `_get_current_object()` method.
                # docs: https://flask.palletsprojects.com/en/2.2.x/reqcontext/
                args=(
                    getattr(current_app, "_get_current_object")(),
                    request.environ
                )
            )
        }
        TASKS[task_id]['task'].start()
        # Return a 202 response, with an id that the client can use to obtain
        # task status
        return {'TaskId': task_id}, 202
    return wrapped


def executor_callback(future):
    logger.debug("Callback from future:", future)


class BackgroundMail:
    """Background Task for Mail Sender"""

    def __init__(self, mail, executor):
        self.mail = mail
        self.executor = executor

    def send_email(self, recipient, subject, content):
        msg = Message(
            subject,
            sender='me@oluwabukunmi.com',
            recipients=[recipient]
        )
        msg.body = content
        self.mail.send(msg)

    def send_email_async(self, recipient, subject, content):
        # Generate a unique task ID
        task_id = uuid.uuid4().hex
        self.executor.submit_stored(
            task_id,
            self.send_email,
            recipient,
            subject,
            content
        )
        return task_id

    def task_status(self, task_id):
        if not self.executor.futures.done(task_id):
            return "running"
        self.executor.futures.pop(task_id)
        return "completed"
