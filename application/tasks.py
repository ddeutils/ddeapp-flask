import threading
import time
import uuid
from functools import wraps
from flask import Flask, current_app, request, abort
from werkzeug.exceptions import HTTPException, InternalServerError

app = Flask(__name__)
tasks = {}


def flask_async(f):
    """This decorator transforms a sync route to asynchronous by running it
    in a background thread.
    docs: https://stackoverflow.com/questions/40989671/background-tasks-in-flask
    """
    @wraps(f)
    def wrapped(*args, **kwargs):
        def task(app, environ):
            # Create a request context similar to that of the original request
            with app.request_context(environ):
                try:
                    # Run the route function and record the response
                    tasks[task_id]['result'] = f(*args, **kwargs)
                except HTTPException as e:
                    tasks[task_id]['result'] = current_app.handle_http_exception(e)
                except Exception as e:
                    # The function raised an exception, so we set a 500 error
                    tasks[task_id]['result'] = InternalServerError()
                    if current_app.debug:
                        # We want to find out if something happened so reraise
                        raise

        # Assign an id to the asynchronous task
        task_id = uuid.uuid4().hex

        # Record the task, and then launch it
        tasks[task_id] = {
            'task': threading.Thread(
                target=task,

                # If you need to access the underlying object that is proxied,
                # use the `_get_current_object()` method.
                # docs: https://flask.palletsprojects.com/en/2.2.x/reqcontext/
                args=(
                    current_app._get_current_object(), request.environ
                )
            )
        }
        tasks[task_id]['task'].start()
        # Return a 202 response, with an id that the client can use to obtain task status
        return {'TaskId': task_id}, 202
    return wrapped


@app.route('/foo')
@flask_async
def foo():
    time.sleep(10)
    return {'Result': True}


@app.route('/foo/<task_id>', methods=['GET'])
def foo_results(task_id):
    """Return results of asynchronous task.
    If this request returns a 202 status code, it means that task hasn't finished yet.
    """
    task = tasks.get(task_id)
    if task is None:
        abort(404)
    return ({'TaskID': task_id}, 202) if 'result' not in task else task['result']


def test_task():
    import time
    import requests

    task_ids = [requests.get('http://127.0.0.1:5000/foo').json().get('TaskId') for _ in range(2)]
    time.sleep(11)
    results = [requests.get(f'http://127.0.0.1:5000/foo/{task_id}').json() for task_id in task_ids]
    print(results)
    # [{'Result': True}, {'Result': True}]
    return results


if __name__ == '__main__':
    app.run(debug=True)
