import pickle
from pathlib import Path
from ....core.legacy.base import (
    get_catalog_all,
    TblCatalog,
    PipeCatalog,
    FuncCatalog,
)
from typing import (
    List,
    Dict,
    Optional,
)
from ...frontend.catalogs.models import (
    Category,
    Catalog,
)


__categories: Dict['str', Category] = {}
__all_catalogs_list: List[Catalog] = []


def load_catalogs(cache: bool = False):
    global __categories, __all_catalogs_list

    file = Path(__file__).parent / 'cache' / 'categories.pickle'

    if cache:
        with open(file, mode='rb') as f:
            __categories = pickle.load(f)
    else:
        cat_mapping = {
            'catalog': TblCatalog,
            'pipeline': PipeCatalog,
            'function': FuncCatalog,
        }
        for cat, obj in cat_mapping.items():
            raw_data = get_catalog_all(cat)
            data: List = [
                obj(name).catalog
                for name in raw_data
            ]
            __categories[cat] = Category(category=cat, data=data)

    with open(file, mode='wb') as f:
        pickle.dump(__categories, f)

    rebuild_flat_file_list()


def rebuild_flat_file_list() -> None:
    global __all_catalogs_list

    flat_set = {
        v.id: v
        for cat_name, cat in __categories.items()
        for v in cat.data
    }
    __all_catalogs_list = list(flat_set.values())
    __all_catalogs_list.sort(key=lambda vid: vid.id, reverse=True)


def category_by_name(category: str) -> Optional[Category]:
    """Get Catalog data from name.
    """
    if not category or not category.strip() or category not in {
        'catalog',
        'pipeline',
        'function'
    }:
        return None

    category = category.strip().lower()
    cat = __categories.get(category)
    if not cat:
        return None
    return cat


def all_catalogs(page: int = 1, page_size: Optional[int] = None) -> List[Catalog]:
    catalogs = __all_catalogs_list
    if page_size:
        start = page_size * (page - 1)
        end = start + page_size
        catalogs = catalogs[start: end]
    return catalogs


def all_categories() -> List[Category]:
    categories = list(__categories.values())
    categories.sort(key=lambda c: c.category.lower().strip())
    return categories


def catalog_by_id(catalog_id: str) -> Optional[Catalog]:
    return next((catalog for catalog in all_catalogs() if catalog.id == catalog_id), None)


def catalog_by_name(catalog_name: str) -> Optional[Catalog]:
    return next((catalog for catalog in all_catalogs() if catalog.name == catalog_name), None)


def search_catalogs(search_text: str) -> List[Catalog]:
    results: List[Catalog] = []
    if not search_text or not search_text.strip():
        return results

    search_text = search_text.lower().strip()
    for catalog in all_catalogs():
        text = f"{catalog.id} {catalog.name}".lower()
        if search_text in text:
            results.append(catalog)
    return results


def add_catalog(cat_name: str, name: str, config: str):
    global __all_catalogs_list

    if catalog_by_name(name):
        return None

    cat = category_by_name(cat_name)
    if not cat:
        return None
    # v = Catalog(name=name)
    # cat.data.append(v)
    print(f"Add catalog to {cat_name} by value ({name}, {config})")
    rebuild_flat_file_list()
    return None


def catalog_count() -> int:
    return len(__all_catalogs_list)


load_catalogs(cache=True)
