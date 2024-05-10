from textwrap import dedent, indent

from .config import (
    AI_APP_PATH,
    Environs,
    Params,
)
from .logging_ import get_logger, logging
from .reusables import (
    hash_string,
    must_list,
    split_iterable,
)


def ptext(text: str, _indent=None) -> str:
    return indent(dedent(text), " | (WARNING) ... ")
