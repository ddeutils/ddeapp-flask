# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------

import ast
import hashlib
import math
import os
import random
import re
import string
from collections import defaultdict
from collections.abc import Iterable
from typing import (
    AnyStr,
    Optional,
    Union,
)

import pandas as pd

__all__ = (
    "split_iterable",
    "merge_dicts",
    "hash_string",
    "path_join",
    "only_one",
    "chunks",
    "random_sting",
    "convert_str_list",
    "convert_str_bool",
    "must_list",
    "must_dict",
    "must_bool",
    "rows",
    "to_snake_case",
    "to_camel_case",
    "to_pascal_case",
)


# utility function -------------------------------------------------------------
def split_str(strings, sep: str = r"\s+"):
    """
    warning: does not yet work if sep is a lookahead like `(?=b)`
    usage:
        >> split_str('.......A...b...c....', sep='...')
        <generator object split_str.<locals>.<genexpr> at 0x7fe8530fb5e8>

        >> list(split_str('A,b,c.', sep=','))
        ['A', 'b', 'c.']

        >> list(split_str(',,A,b,c.,', sep=','))
        ['', '', 'A', 'b', 'c.', '']

        >> list(split_str('.......A...b...c....', '...'))
        ['', '', '.A', 'b', 'c', '.']

        >> list(split_str('   A  b  c. '))
        ['', 'A', 'b', 'c.', '']
    """
    if not sep:
        return iter(strings)

    # Alternatively, more verbosely:
    regex = f"(?:^|{sep})((?:(?!{sep}).)*)"
    for match in re.finditer(regex, strings):
        yield match.group(1)


# utility function -------------------------------------------------------------
def isplit(source, sep=None, regex=False):
    """generator version of str.split()
    :param source: source string (unicode or bytes)
    :param sep: separator to split on.
    :param regex: if True, will treat sep as regular expression.
    :returns:
        generator yielding elements of string.

    usage:
        >> print list(isplit("abcb","b"))
        ['a','c','']
    """
    if sep is None:
        # mimic default python behavior
        source = source.strip()
        sep = "\\s+"
        if isinstance(source, bytes):
            sep = sep.encode("ascii")
        regex = True
    start = 0
    if regex:
        # Version using re.finditer()
        if not hasattr(sep, "finditer"):
            sep = re.compile(sep)
        for m in sep.finditer(source):
            idx = m.start()
            assert idx >= start
            yield source[start:idx]
            start = m.end()
        yield source[start:]
    else:
        # Version using str.find(), less overhead than re.finditer()
        sep_size = len(sep)
        while True:
            idx = source.find(sep, start)
            if idx == -1:
                yield source[start:]
                return
            yield source[start:idx]
            start = idx + sep_size


# utility function -------------------------------------------------------------
def split_iterable(iterable, chunk_size=None, generator_flag: bool = True):
    """
    Split an iterable into mini batch with batch length of batch_number
    supports batch of a pandas dataframe
    usage:
        >> for i in split_iterable([1,2,3,4,5], chunk_size=2):
        >>    print(i)
        [1, 2]
        [3, 4]
        [5]

        for idx, mini_data in split_iterable(batch(df, chunk_size=10)):
            print(idx)
            print(mini_data)
    """
    chunk_size: int = chunk_size or 25000
    num_chunks = math.ceil(len(iterable) / chunk_size)
    if generator_flag:
        for _ in range(num_chunks):
            if isinstance(iterable, pd.DataFrame):
                yield iterable.iloc[_ * chunk_size : (_ + 1) * chunk_size]
            else:
                yield iterable[_ * chunk_size : (_ + 1) * chunk_size]
    else:
        _chunks: list = []
        for _ in range(num_chunks):
            if isinstance(iterable, pd.DataFrame):
                _chunks.append(
                    iterable.iloc[_ * chunk_size : (_ + 1) * chunk_size]
                )
            else:
                _chunks.append(iterable[_ * chunk_size : (_ + 1) * chunk_size])
        return _chunks


# utility function -------------------------------------------------------------
def chunks(dataframe, n):
    """Yield successive n-sized chunks from dataframe."""
    for i in range(0, len(dataframe), n):
        yield dataframe.iloc[i : i + n]


# utility function -------------------------------------------------------------
def rows(f, chunk_size=1024, sep="|"):
    """
    Read a file where the row separator is '|' lazily
    usage:
        >> with open('big.csv') as f:
        >>     for r in rows(f):
        >>         process(r)
    """
    row = ""
    while (chunk := f.read(chunk_size)) != "":  # End of file
        while (index := chunk.find(sep)) != -1:  # No separator found
            yield row + chunk[:index]
            chunk = chunk[index + 1 :]
            row = ""
        row += chunk
    yield row


# utility function -------------------------------------------------------------
def merge_dicts(*dict_args) -> dict:
    """
    Given any number of dictionaries, shallow copy and merge into a new dict,
    precedence goes to key-value pairs in latter dictionaries.
    usage:
            >> merge_dicts({1: 'one',2: 'two',3: 'three'}, {3: 'Three',4: 'Four'})
            {1: 'one', 2: 'two', 3: 'Three', 4: 'Four'}
    """
    result: dict = {}
    for dictionary in dict_args:
        result.update(dictionary)
    return result


# utility function -------------------------------------------------------------
def merge_lists(*list_args) -> list:
    """
    usage:
            >> merge_lists([1, 2, 3, 4], ['one', 'two', three'])
            [1, 2, 3, 4, 'one', 'two', 'three']
    """
    result: list = []
    for _list in list_args:
        result.extend(_list)
    return result


# utility function -------------------------------------------------------------
def hash_string(input_value: str, num_length: int = 8) -> str:
    """
    hash str input to number with SHA256 algorithm
    """
    return str(
        int(hashlib.sha256(input_value.encode("utf-8")).hexdigest(), 16)
    )[-num_length:]


# utility function -------------------------------------------------------------
def random_sting(num_length: int = 8) -> str:
    """
    random string from uppercase ASCII and number 0-9
    """
    return "".join(
        random.choices(string.ascii_uppercase + string.digits, k=num_length)
    )


# utility function -------------------------------------------------------------
def path_join(full_path: AnyStr, full_join_path: str) -> AnyStr:
    """
    join path with multi pardir value if set `full_join_path` be '../../<path>'
    """
    _abspath: AnyStr = full_path
    _join_split: list = os.path.normpath(full_join_path).split(os.sep)
    for path in _join_split:
        _abspath: AnyStr = (
            os.path.abspath(os.path.join(_abspath, os.pardir))
            if path == ".."
            else os.path.abspath(os.path.join(_abspath, path))
        )
    return _abspath


# utility function -------------------------------------------------------------
def convert_str_list(str_list: str) -> list:
    """
    Get list of run_date from list string of run_date
    usage
    -----
        >> print(convert_str_list("['2021-01-02', '2021-01-03']"))
        ['2021-01-02', '2021-01-03']
        >> print(convert_str_list("2022-04-01")
        ['2022-04-01']
    """
    return (
        ast.literal_eval(str_list)
        if str_list.startswith("[") and str_list.endswith("]")
        else [str_list]
    )


# utility function -------------------------------------------------------------
def convert_str_dict(str_dict: str) -> dict:
    """
    Get list of run_date from list string of run_date
    usage
    -----
        >>> print(convert_str_dict("{'1': '2021-01-02', '2':'2021-01-03'}"))
        {'1': '2021-01-02', '2': '2021-01-03'}
        >>> print(convert_str_dict("2022-04-01"))
        {0: '2022-04-01'}
    """
    return (
        ast.literal_eval(str_dict)
        if str_dict.startswith("{") and str_dict.endswith("}")
        else {0: str_dict}
    )


# utility function -------------------------------------------------------------
def convert_str_bool(str_bool: str, force_raise: bool = False) -> bool:
    """
    Get boolean of input string
    """
    if str_bool.lower() in {"yes", "true", "t", "1", "y", "1.0"}:
        return True
    elif str_bool.lower() in {"no", "false", "f", "0", "n", "0.0"}:
        return False
    if force_raise:
        raise ValueError(f"value {str_bool!r} does not convert to boolean type")
    return False


# utility function -------------------------------------------------------------
def sort_by_priority_list(values: Iterable, priority: list) -> list:
    """
    Sorts an iterable according to a list of priority items.
    Usage
    -----
        >> sort_by_priority_list(values=[1,2,2,3], priority=[2,3,1])
        [2, 2, 3, 1]
        >> sort_by_priority_list(values=set([1,2,3]), priority=[2,3])
        [2, 3, 1]
    """
    # priority_dict = {k: i for i, k in enumerate(priority)}
    #
    # def priority_getter(value):
    #     return priority_dict.get(value, len(values))
    #
    # return sorted(values, key=priority_getter)
    priority_dict = defaultdict(
        lambda: len(priority),
        zip(
            priority,
            range(len(priority)),
        ),
    )
    priority_getter = priority_dict.__getitem__  # dict.get(key)
    return sorted(values, key=priority_getter)


# utility function -------------------------------------------------------------
def only_one(
    check_list: list, match_list: list, default: bool = True
) -> Optional:
    """
    Usage
    -----
        >> list_a = ['a', 'a', 'b']
        >> list_b = ['a', 'b', 'c']
        >> list_c = ['d', 'f']
        >> only_one(list_a, list_b):

        >> only_one(list_c, list_b):

    """
    if len(exist := set(check_list).intersection(set(match_list))) == 1:
        return list(exist)[0]
    return next(
        (_ for _ in match_list if _ in check_list),
        (match_list[0] if default else None),
    )


# utility function -------------------------------------------------------------
def must_list(value: Optional[Union[str, list]] = None) -> list:
    if value:
        return convert_str_list(value) if isinstance(value, str) else value
    return []


# utility function -------------------------------------------------------------
def must_dict(value: Optional[Union[str, dict]] = None) -> dict:
    if value:
        return convert_str_dict(value) if isinstance(value, str) else value
    return {}


# utility function -------------------------------------------------------------
def must_bool(
    value: Optional[Union[str, int, bool]] = None, force_raise: bool = False
) -> bool:
    """
    Usage
    -----
        >>
    """
    if value:
        return (
            value
            if isinstance(value, bool)
            else convert_str_bool(str(value), force_raise=force_raise)
        )
    return False


# utility function -------------------------------------------------------------
def to_snake_case(value: str):
    """
    Usage
    -----
        >>
    """
    name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", value)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).lower()


# utility function -------------------------------------------------------------
def to_pascal_case(value: str, joined: str = ""):
    return joined.join(word.title() for word in value.split("_"))


# utility function -------------------------------------------------------------
def to_camel_case(value: str):
    return "".join(
        word.title() if index_word > 0 else word
        for index_word, word in enumerate(value.split("_"))
    )
