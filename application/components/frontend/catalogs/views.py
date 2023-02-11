import time
from flask import (
    Blueprint,
    redirect,
    render_template,
    make_response,
)
from .viewmodels import (
    IndexViewModel,
    CategoryViewModel,
    CatalogViewModel,
    FeedViewModel,
    AddCatalogViewModel,
    SearchViewModel,
)
from .services import add_catalog
from ....infrastructures import response

catalogs = Blueprint('catalogs', __name__, template_folder='templates')
CATALOGS_PER_PAGE = 10


@catalogs.get('/catalog')
@response(template_file='catalogs/all_catalogs.html')
def all_catalogs():
    vm = IndexViewModel()
    return vm.to_dict()


@catalogs.get('/catalog/category/<cat_name>')
@response(template_file='catalogs/category.html')
def category(cat_name: str):
    vm = CategoryViewModel(cat_name)
    return vm.to_dict()


@catalogs.get('/catalog/<catalog_id>')
@response(template_file='catalogs/catalog.html')
def catalog(catalog_id: str):
    vm = CatalogViewModel(catalog_id)
    return vm.to_dict()


@catalogs.get('/catalog/feed')
@response(template_file='catalogs/catalog_feed.html')
def feed_catalogs():
    time.sleep(.5)
    vm = FeedViewModel(page_size=CATALOGS_PER_PAGE, page=1)
    return vm.to_dict()


@catalogs.get('/catalog/feed/more/<int:page>')
@response(template_file='catalogs/partials/catalogs.html')
def more_catalogs(page: int):
    time.sleep(.5)
    vm = FeedViewModel(page_size=CATALOGS_PER_PAGE, page=page)
    return vm.to_dict()


@catalogs.get('/catalog/add/<cat_name>')
@response(template_file='catalogs/partials/add_catalog_form.html')
def add_get(cat_name: str):
    vm = AddCatalogViewModel(cat_name)
    return vm.to_dict()


@catalogs.post('/catalog/add/<cat_name>')
def add_post(cat_name: str):
    vm = AddCatalogViewModel(cat_name)
    vm.restore_from_form()
    add_catalog(cat_name, vm.name, vm.config)
    _response = make_response()
    _response.headers["HX-Redirect"] = f'/catalog/category/{cat_name}'
    _response.status_code = 200
    return _response


@catalogs.get('/catalog/cancel_add/<cat_name>')
@response(template_file='catalogs/partials/show_add_form.html')
def cancel_add(cat_name: str):
    vm = AddCatalogViewModel(cat_name)
    return vm.to_dict()


@catalogs.get('/catalog/search')
def search():
    vm = SearchViewModel()
    print(f"Searching for {vm.search_text}")
    if vm.is_htmx_request and vm.search_text:
        html = render_template('catalogs/partials/catalogs.html', catalogs=vm.catalogs)
        return make_response(html)
    return redirect('/catalog/feed/more/1')
