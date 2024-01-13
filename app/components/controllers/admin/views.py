# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

from flask import (
    Blueprint,
    render_template,
    request,
    abort,
)
from ....extensions import db
from .models import MODEL_VIEWS
from .decorators import admin_required

admin = Blueprint('admin', __name__, template_folder='templates')


@admin.get('/')
@admin_required
def admin_view():
    page: int = request.args.get('page', 1, type=int)
    _model_view: str = request.args.get('model_view', 'user', type=str)

    try:
        model_view = MODEL_VIEWS[_model_view.lower()]
    except KeyError as err:
        return abort(404, {"error": str(err)})

    models = model_view.query.paginate(page=page, per_page=15)
    if 'Hx-Request' in request.headers:
        return render_template(
            "admin/partials/models.html",
            models=models,
            model_view=model_view,
        )

    return render_template(
        "admin/model_view.html",
        models=models,
        model_view=model_view,
        model_lists=list(MODEL_VIEWS.keys())
    )


@admin.post("/view-submit/<string:model_view>")
@admin_required
def view_submit(model_view):
    model_view = MODEL_VIEWS[model_view.lower()]
    _updated_data = {
        _: request.form[_] for _ in model_view.__view_cols_create__.values()
    }

    if any(not _ for _ in request.form.values()):
        return ''

    if (
            model_exists := model_view.query.filter_by(
                **{col: _updated_data[col] for col in model_view.__view_cols_search__}
            ).first()
    ):
        model = model_exists
    else:
        model = model_view(
            **_updated_data
        )
        db.session.add(model)
        db.session.commit()
    return f"""
    <tr>
        {
            ' '.join([
                f'<td>{ model.view_items[col] }</td>'
                for col in model_view.__view_cols__
            ])
        }
        <td>
            <a hx-get="/admin/get-edit/{model_view.__view_title__.lower()}/{model.id}"
               class="edit" title="Edit" data-toggle="tooltip">
                <i class="fa fa-pencil" aria-hidden="true"></i>
            </a>
            <a hx-trigger="delete_confirmed"
               hx-delete="/admin/delete-row/{model_view.__view_title__.lower()}/{model.id}"
               _="on click call
                  Swal.fire({{
                    title: 'Are you sure?',
                    text: 'You won\\'t be able to revert this!',
                    icon: 'warning',
                    showCancelButton: true,
                    confirmButtonColor: '#3085d6',
                    cancelButtonColor: '#d33',
                    confirmButtonText: 'Yes, delete it!'
                  }})
                  if result.isConfirmed trigger delete_confirmed"
               class="delete"
               title="Delete"
               data-toggle="tooltip">
                <i class="fa fa-trash" aria-hidden="true"></i>
            </a>
        </td>
    </tr>
    """


@admin.get('/get-edit/<string:model_view>/<model_id>')
@admin_required
def get_edit_from_view(model_view, model_id):
    model_view = MODEL_VIEWS[model_view.lower()]
    model = model_view.query.get_or_404(model_id)
    return f"""
    <tr hx-trigger='cancel' hx-get="/get-row/{model_view.__view_title__.lower()}/{model.id}">
        {
            ' '.join([
                f'''
                <td class="edit-wrapper">
                    <input type="text" name="{col}" value="{model.view_items[col]}" class=""/>
                </td>
                '''
                if model.can_update(col)
                else f'<td>{ model.view_items[col] }</td>'
                for col in model_view.__view_cols__
            ])
        }
        <td>
            <a hx-get="/admin/get-row/{model_view.__view_title__.lower()}/{model.id}"
               class="cancel" title="Cancel" data-toggle="tooltip">
                <i class="fa fa-ban" aria-hidden="true"></i>
            </a>
            <a hx-put="/admin/update-row/{model_view.__view_title__.lower()}/{model.id}"
               hx-include="closest tr"
               _="on click call
                 Swal.fire({{
                    title: 'SUCCESS',
                    text: 'Save change to backend.',
                    icon: 'success'
                 }})"
               class="save" title="Save" data-toggle="tooltip">
                <i class="fa fa-floppy-o" aria-hidden="true"></i>
            </a>
        </td>
    </tr>
    """


@admin.get('/get-row/<string:model_view>/<model_id>')
@admin_required
def get_row_from_view(model_view, model_id):
    model_view = MODEL_VIEWS[model_view.lower()]
    model = model_view.query.get_or_404(model_id)
    return f"""
    <tr>
        {
            ' '.join([
                f'<td>{ model.view_items[col] }</td>'
                for col in model_view.__view_cols__
            ])
        }
        <td>
            <a hx-get="/admin/get-edit/{model_view.__view_title__.lower()}/{model.id}"
               class="edit" title="Edit" data-toggle="tooltip">
                <i class="fa fa-pencil" aria-hidden="true"></i>
            </a>
            <a hx-trigger="delete_confirmed"
               hx-delete="/admin/delete-row/{model_view.__view_title__.lower()}/{model_id}"
               _="on click call
                  Swal.fire({{
                    title: 'Are you sure?',
                    text: 'You won\\'t be able to revert this!',
                    icon: 'warning',
                    showCancelButton: true,
                    confirmButtonColor: '#3085d6',
                    cancelButtonColor: '#d33',
                    confirmButtonText: 'Yes, delete it!'
                  }})
                  if result.isConfirmed trigger delete_confirmed"
               class="delete"
               title="Delete"
               data-toggle="tooltip">
                <i class="fa fa-trash" aria-hidden="true"></i>
            </a>
        </td>
    </tr>
    """


@admin.delete("/delete-row/<string:model_view>/<model_id>")
@admin_required
def delete_row(model_view, model_id):
    model_view = MODEL_VIEWS[model_view.lower()]
    model = model_view.query.get_or_404(model_id)
    db.session.delete(model)
    db.session.commit()
    return ""


@admin.put('/update-row/<string:model_view>/<model_id>')
@admin_required
def update_row(model_view, model_id):
    model_view = MODEL_VIEWS[model_view.lower()]
    model_obj = model_view.query.filter_by(id=model_id)
    model = model_obj.first()
    model_obj.update({
        value: request.form[col]
        for col, value in model_view.__view_cols_update__.items()
    })
    db.session.commit()
    return f"""
    <tr>
        {
            ' '.join([
                f'<td>{ request.form[col] }</td>'
                if model_view.can_update(col) else
                f'<td>{ model.view_items[col] }</td>'
                for col in model_view.__view_cols__
            ])
        }
        <td>
            <a hx-get="/admin/get-edit/{model_view.__view_title__.lower()}/{model_id}"
               class="edit" title="Edit" data-toggle="tooltip">
                <i class="fa fa-pencil" aria-hidden="true"></i>
            </a>
            <a hx-trigger="delete_confirmed"
               hx-delete="/admin/delete-row/{model_view.__view_title__.lower()}/{model_id}"
               _="on click call
                  Swal.fire({{
                    title: 'Are you sure?',
                    text: 'You won\\'t be able to revert this!',
                    icon: 'warning',
                    showCancelButton: true,
                    confirmButtonColor: '#3085d6',
                    cancelButtonColor: '#d33',
                    confirmButtonText: 'Yes, delete it!'
                  }})
                  if result.isConfirmed trigger delete_confirmed"
               class="delete"
               title="Delete"
               data-toggle="tooltip">
                <i class="fa fa-trash" aria-hidden="true"></i>
            </a>
        </td>
    </tr>
    """


@admin.get('/confirmed')
def confirmed():
    return 'Confirmed'
