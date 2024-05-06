from typing import Optional

import more_itertools
from flask import (
    Request,
    request,
)

from ....infrastructures import create as request_dict_create
from .models import Catalog, Category
from .services import (
    all_catalogs,
    all_categories,
    catalog_by_id,
    category_by_name,
    search_catalogs,
)


class ViewModelBase:
    """Base of View Model class"""

    def __init__(self):
        self.request: Request = request
        self.request_dict = request_dict_create("")
        self.is_htmx_request = "HX-Request" in request.headers
        self.error: Optional[str] = None
        self.view_model = self.to_dict()

    def to_dict(self):
        return self.__dict__


class IndexViewModel(ViewModelBase):
    def __init__(self):
        super().__init__()
        self.categories: list[Category] = all_categories()
        self.rows = [
            list(row) for row in more_itertools.chunked(self.categories, 3)
        ]


class FeedViewModel(ViewModelBase):
    def __init__(self, page_size: int, page: int = 1):
        super().__init__()
        self.page_size = page_size
        self.page = page
        _all_catalogs = all_catalogs()
        start = (page - 1) * page_size
        end = start + page_size
        self.catalogs: list[Catalog] = _all_catalogs[start:end]
        self.has_more_catalogs = len(_all_catalogs) > end
        print("Has more: ", self.has_more_catalogs)


class CategoryViewModel(ViewModelBase):
    def __init__(self, cat_name: str):
        super().__init__()
        self.cat_name = cat_name
        self.category: Optional[Category] = category_by_name(cat_name)
        self.rows = [
            list(row) for row in more_itertools.chunked(self.category.data, 3)
        ]
        self.Catalog = Catalog


class CatalogViewModel(ViewModelBase):
    def __init__(self, catalog_id: str):
        super().__init__()

        self.catalog_id = catalog_id
        self.catalog: Optional[Catalog] = catalog_by_id(catalog_id)


class AddCatalogViewModel(ViewModelBase):
    def __init__(self, cat_name: str):
        super().__init__()
        self.cat_name = cat_name
        self.name: Optional[str] = None
        self.config: Optional[str] = None

    def restore_from_form(self):
        d = self.request_dict
        self.name = d.get("name")
        self.config = d.get("config")


class SearchViewModel(ViewModelBase):
    def __init__(self):
        super().__init__()
        self.search_text: str = self.request_dict.get("search_text")
        self.catalogs: list[Catalog] = []
        if self.search_text and self.search_text.strip():
            self.catalogs = search_catalogs(self.search_text)
