import pickle
from pathlib import Path
from typing import (
    List,
    Dict,
    Optional,
)
from ....core.validators import (
    TableFrontend,
    FunctionFrontend,
    PiplineFrontend,
)
from ....core.base import (
    get_catalogs,
    CATALOGS,
)
from application.core.utils.logging_ import get_logger
from ...frontend.catalogs.models import (
    Category,
    Catalog,
)


logger = get_logger(__name__)
__categories: Dict['str', Category] = {}
__all_catalogs_list: List[Catalog] = []


def load_catalogs(cache: bool = False):
    global __categories, __all_catalogs_list

    filepath = Path(__file__).parent / 'cache' / 'categories.pickle'

    if cache:
        try:
            with open(filepath, mode='rb') as f:
                __categories = pickle.load(f)
        except FileNotFoundError as err:
            print(f"Error: {err}")
            cache: bool = False

    if not cache:
        cat_mapping = {
            # 'catalog': TableFrontend,
            # 'pipeline': PiplineFrontend,
            'function': FunctionFrontend,
        }
        for cat, obj in cat_mapping.items():
            raw_data = get_catalogs(cat)
            data: List = [
                obj.parse_name(name).catalog
                for name in raw_data
            ]
            __categories[cat] = Category(category=cat, data=data)

    with open(filepath, mode='wb') as f:
        pickle.dump(__categories, f)

    logger.debug("Success load Catalogs from config.")
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
    logger.debug("Success rebuild flat file list.")


def category_by_name(category: str) -> Optional[Category]:
    """Get Catalog data from name.
    """
    if (
            not category
            or not category.strip()
            or category not in CATALOGS
    ):
        return None

    category = category.strip().lower()
    cat = __categories.get(category)
    return cat or None


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
    return next(
        (catalog for catalog in all_catalogs() if catalog.id == catalog_id),
        None
    )


def catalog_by_name(catalog_name: str) -> Optional[Catalog]:
    return next(
        (catalog for catalog in all_catalogs() if catalog.name == catalog_name),
        None
    )


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


load_catalogs(cache=False)
