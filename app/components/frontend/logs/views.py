import pandas as pd
import numpy as np
from flask import (
    Blueprint,
    render_template,
    request,
    jsonify
)
from .models import TaskLog
from ....extensions import db
from app.core.utils.reusables import must_dict

logs = Blueprint('logs', __name__, template_folder='templates')


@logs.get("/logs")
def all_log():
    return render_template('logs/all_logs_server_side.html')


@logs.get("/logs/chart-data")
def log_data_chart():
    period = request.args.get('period', '', type=str)

    logs_df = pd.read_sql_query(
        TaskLog.query.filter_by().order_by(TaskLog.update_date.asc()).statement,
        db.session.connection()
    )
    logs_df = logs_df.reset_index(drop=True).astype({'process_number_put': int, 'process_time': int})
    logs_df[['process_type_bg', 'process_type']] = logs_df['process_type'].str.split('|', n=1, expand=True)
    logs_df[['process_module', 'process_module_value']] = logs_df['process_module'].str.split('|', n=1, expand=True)
    logs_df['update_date'] = pd.to_datetime(logs_df['update_date']).dt.date

    if True:
        logs_df['update_date'] = pd.to_datetime(logs_df['update_date']) + pd.offsets.MonthBegin(1)

    logs_df = logs_df.groupby(
        # ['process_type_bg', 'process_type', 'process_module', 'process_name_put', 'update_date'],
        ['process_type_bg', 'update_date'],
        sort=False,
        as_index=False
    ).agg({
        'process_number_put': np.sum,
        'process_time': np.mean,
        'process_name_put': 'count',
    })

    logs_df['process_time'] = logs_df['process_time'].round(2)
    print(logs_df)

    data = logs_df.to_dict(orient='records')
    print(data)
    data = list(filter(lambda row: row['process_type_bg'] == 'foreground', data))
    return jsonify({
        'labels': [row['update_date'] for row in data],
        'values': {
            "first": [row['process_number_put'] for row in data],
            "second": [row['process_time'] for row in data]
        }
    })


@logs.get("/logs/data")
def log_data():
    """Generate Task Log data"""
    if search := request.args.get('search', '', type=str):
        searched_task_log = TaskLog.search_all(search)
    elif search := request.args.get('searchText', '', type=str):
        searched_task_log = TaskLog.search_all(search)
    elif multi_search := must_dict(request.args.get('filter', '{}', type=str)):
        searched_task_log = TaskLog.search_by(multi_search)
    else:
        searched_task_log = TaskLog.query

    # TODO: Convert this process to common function
    multi_sort = []
    for _ in range(6):
        # For loop with number of column from the TaskLog model.
        if sn := request.args.get(f'multiSort[{_}][sortName]', '', type=str):
            multi_sort.append({
                'sortName': sn,
                'sortOrder': request.args.get(f'multiSort[{_}][sortOrder]', 'asc', type=str)
            })
        else:
            break

    if multi_sort:
        sorting = [
            getattr(TaskLog.get_column(sorts['sortName']), sorts['sortOrder'])()
            for sorts in multi_sort
        ]
    elif sort := request.args.get('sort', '', type=str):
        order = request.args.get('order', 'asc', type=str)
        sorting = [getattr(getattr(TaskLog, sort), order)()]
    elif sort := request.args.get('sortName', '', type=str):
        order = request.args.get('sortOrder', 'asc', type=str)
        sorting = [getattr(getattr(TaskLog, sort), order)()]
    else:
        sorting = [TaskLog.id.desc()]

    if (limit := request.args.get('limit', 0, type=int)) > 0:
        offset = request.args.get('offset', 0, type=int)
        page: int = int(offset / limit) + 1
        _logs = searched_task_log.order_by(*sorting).paginate(page=page, per_page=limit)
        rows = [_.to_dict() for _ in _logs.items]
        total = _logs.total
    elif (page := request.args.get('pageNumber', 0, type=int)) > 0:
        page_size = request.args.get('pageSize', 10, type=int)
        _logs = searched_task_log.order_by(*sorting).paginate(page=page, per_page=page_size)
        rows = [_.to_dict() for _ in _logs.items]
        total = _logs.total
    else:
        _logs = searched_task_log.order_by(*sorting).all()
        rows = [_.to_dict() for _ in _logs]
        total = len(rows)

    return jsonify({
        'total': total,
        'rows': rows
    })
