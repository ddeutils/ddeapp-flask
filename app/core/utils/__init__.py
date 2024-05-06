from textwrap import dedent, indent

from .config import (
    Environs,
    Params,
)


def ptext(text: str, _indent=None) -> str:
    return indent(dedent(text), " | (WARNING) ... ")
