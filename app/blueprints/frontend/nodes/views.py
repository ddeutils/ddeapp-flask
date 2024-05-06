import json
import time
from datetime import timedelta
from typing import Optional

import numpy as np
import pandas as pd
import plotly
import plotly.express as px
from flask import Blueprint, jsonify, render_template, request

from ....extensions import db
from .models import Node, NodeLog, Pipeline

nodes = Blueprint("nodes", __name__, template_folder="templates")
PIPELINES_PER_PAGE = 10


@nodes.get("/node")
def all_node():
    page = request.args.get("page", 1, type=int)
    _nodes = Node.query.order_by(Node.update_date.desc()).paginate(
        page=page, per_page=25
    )
    return render_template("nodes/all_nodes.html", nodes=_nodes)


@nodes.get("/node/data")
def all_node_data():
    query = Node.query

    # Search filter
    if search := request.args.get("search[value]"):
        query = query.filter(
            db.or_(Node.name.like(f"%{search}%"), Node.type.like(f"%{search}%"))
        )
    total_filtered = query.count()

    # Sorting
    order = []
    i = 0
    while True:
        col_index = request.args.get(f"order[{i}][column]")
        if col_index is None:
            break
        col_name = request.args.get(f"columns[{col_index}][data]")
        if col_name not in ["name", "age", "email"]:
            col_name = "name"
        descending = request.args.get(f"order[{i}][dir]") == "desc"
        col = getattr(Node, col_name)
        if descending:
            col = col.desc()
        order.append(col)
        i += 1

    if order:
        query = query.order_by(*order)

    # Pagination
    start = request.args.get("start", type=int)
    length = request.args.get("length", type=int)
    query = query.all() if length == -1 else query.offset(start).limit(length)

    return jsonify(
        {
            "data": [
                {**user.to_dict(), **{"email": "unknowns"}} for user in query
            ],
            "recordsFiltered": total_filtered,
            "recordsTotal": Node.query.count(),
            "draw": request.args.get("draw", type=int),
        }
    )


@nodes.get("/node/<string:node_name>")
def node(node_name):
    page = request.args.get("page", 1, type=int)
    _node = Node.query.get_or_404(node_name)

    logs = (
        NodeLog.query.filter_by(name=_node.name)
        .order_by(NodeLog.update_date.desc())
        .paginate(page=page, per_page=25)
    )

    return render_template(
        "nodes/node.html",
        node=_node,
        logs=logs,
        graphJSON=node_log_data(_node.name),
    )


@nodes.get("/node/data/<string:node_name>")
def node_log_graph(node_name):
    selected = request.args.get("selected", "Day", type=str)
    return node_log_data(node_name, selected)


def node_log_data(name: str, selected: Optional[str] = None):
    # Load data from Model and convert to DataFrame.
    logs_df = pd.read_sql_query(
        NodeLog.query.filter_by(name=name)
        .order_by(NodeLog.update_date.desc())
        .statement,
        db.session.connection(),
    )
    # Prepare and Aggregate DataFrame.
    logs_df = logs_df.reset_index(drop=True).astype(
        {"row_record": int, "process_time": int}
    )

    selected: str = selected or "Day"
    if selected == "Week":
        # Create function to calculate Start Week date
        logs_df["run_date"] = pd.to_datetime(logs_df["run_date"]).apply(
            lambda date: date - timedelta(days=date.weekday())
        )
    elif selected == "Month":
        logs_df["run_date"] = pd.to_datetime(
            logs_df["run_date"]
        ) + pd.offsets.MonthBegin(1)
    elif selected == "Year":
        logs_df["run_date"] = pd.to_datetime(
            logs_df["run_date"]
        ) + pd.offsets.YearBegin(1)

    logs_df = logs_df.groupby(
        ["table_name", "run_date"], sort=False, as_index=False
    ).agg(
        {
            "row_record": np.sum,
            "process_time": np.mean,
        }
    )
    # print(logs_df)
    fig = px.line(
        logs_df,
        x="run_date",
        y=["row_record", "process_time"],
        line_group="table_name",
        line_shape="spline",
        markers=True,
        labels={
            "run_date": "Run Date",
            "row_record": "Row Record (row)",
            "process_time": "Process Time (second)",
        },
    )
    fig.update_layout(
        font={"family": "Courier New, monospace", "color": "black", "size": 13},
        legend_title="Metric Measures",
        dragmode="pan",
        yaxis={
            "title": "Number of Value",
            "dtick": 5,
            "tick0": -10.00,
            # "range": [-10, 100],
            # "tickvals": [5.1, 5.9, 6.3, 7.5]
        },
        xaxis={
            "tickmode": "auto",
        },
        margin={
            # "t": 0,
            # "b": 2,
            # "l": 10,
            "pad": 2
        },
        plot_bgcolor="rgb(245, 245, 240)",
    )
    # fig.add_trace(
    #     go.Scatter(
    #         x=logs_df['run_date'],
    #         y=logs_df['process_time'],
    #         name='Process Time',
    #         line={
    #             'color': 'firebrick',
    #             'width': 1,
    #             'dash': 'solid'
    #         }
    #     )
    # )
    fig.add_hline(
        y=1,
        opacity=0.2,
        line_dash="dot",
    )
    fig.update_yaxes(
        # rangemode="tozero",
        # title=None,
        # col=1,
        ticklabelposition="inside top",
        color="crimson",
    )
    return json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)


@nodes.post("/node/run")
def node_run():
    return jsonify({"status": "SUCCESS"})


@nodes.get("/pipeline")
def pipeline():
    """Pipeline route for searching and filtering pipeline
    Arguments
    ---
        - page
        - search
        - filter
        - sort
    """
    # Keep page value.
    page: int = request.args.get("page", 1, type=int)
    search: str = request.args.get("search", "", type=str)
    _filter: str = request.args.get("filter", "", type=str)
    paginate_flag: bool = True
    pipelines = Pipeline.query

    if search:
        pipelines = Pipeline.search_all(search)
        paginate_flag: bool = False

    if _filter:
        _filter: dict = json.loads(_filter)
        pipelines = pipelines.filter_by(**_filter)
        paginate_flag: bool = False

    pipelines = pipelines.order_by(Pipeline.update_date.desc())

    if "Hx-Request" in request.headers:

        # Fix not paginate for search and filter option
        if paginate_flag:
            pipelines = pipelines.paginate(
                page=page, per_page=PIPELINES_PER_PAGE
            )
        else:
            pipelines = pipelines.paginate()

        return render_template(
            "nodes/partials/pipelines.html",
            pipelines=pipelines,
            paginate_flag=paginate_flag,
        )

    return render_template(
        "nodes/all_pipelines.html",
        pipelines=pipelines.paginate(page=page, per_page=PIPELINES_PER_PAGE),
        paginate_flag=True,
    )


@nodes.get("/pipeline/partial/<template>")
def pipeline_partial(template):
    return render_template(f"nodes/partials/pipeline_{template}.html")


@nodes.get("/pipeline/<string:pipeline_name>/nodes")
def pipeline_nodes(pipeline_name):
    _pipeline = Pipeline.query.get_or_404(pipeline_name)
    _nodes = Node.query.filter(Node.name.in_(_pipeline.nodes)).paginate()
    return render_template("nodes/partials/pipeline_nodes.html", nodes=_nodes)


@nodes.post("/pipeline/run")
def pipeline_run():
    # TODO: Implement request api.
    time.sleep(5)
    print("Success run data")
    print(request.data)
    print(request.form)
    return jsonify({"status": "SUCCESS"})
