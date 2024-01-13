# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import csv
import json
from typing import (
    Optional,
    Union,
)
from app.core.utils.reusables import (
    path_join,
    rows,
)
from app.core.utils.config import Params
from app.core.utils.logging_ import logging
from app.core.errors import WriteCSVError

AI_APP_PATH: str = os.getenv(
    'AI_APP_PATH', path_join(os.path.dirname(__file__), '../../..')
)

registers = Params(param_name='registers.yaml')

logger = logging.getLogger(__name__)


def load_json_to_values(
        filepath: Union[str, list],
        schema: Optional[list] = None
):
    _filepath: list = [filepath] if isinstance(filepath, str) else filepath
    _results: list = []

    def _repr_value(content: str):
        return (
            'null'
            if content == 'null'
            else f"""'{content.replace("'", "''")}'"""
        )

    for _path in _filepath:
        with open(
                path_join(AI_APP_PATH, f'{registers.path.data}/{_path}'),
                mode='r',
                encoding='utf-8'
        ) as read_file:
            data: Union[dict, list] = json.load(read_file)
            _data: list = [data] if isinstance(data, dict) else data
        _fix_cols: list = schema or list(_data[0].copy().keys())
        _results.extend([
            ', '.join(
                [
                    _repr_value(_content)
                    if isinstance((_content := row.get(col, 'null')), str)
                    else str(_content)
                    for col in _fix_cols
                ]
            )
            for row in _data
        ])
    return _results


def zip_(folder: str, filename: str, filter_: callable):
    from zipfile import ZipFile
    # create a ZipFile object
    with ZipFile(filename, 'w') as zipObj:
        # Iterate over all the files in directory
        for folder_, sub_folder_, filenames in os.walk(folder):
            for filename in filenames:
                if filter_(filename):
                    # create complete filepath of file in directory
                    filepath = os.path.join(folder_, filename)
                    # Add file to zip
                    zipObj.write(filepath, os.path.basename(filepath))


def prepare_data(row: str, delimiter: str) -> list:
    """Prepare data logic with row by row
    """
    data: list = []
    append = data.append
    for col in iter(row.split(delimiter)):
        _col: str = (
            col.strip()
                .replace('"', '')
                .replace("'", "'")
                .replace(",", "_")
        )
        # append(''.join(_ for _ in col if _.isalnum()))
        append(_col)
    return data


def prepare_csv(
        filename: str,
        delimiter: str = "|",
        chunk_size: int = 1024,
        compress: Optional[str] = None
):
    """Prepare csv file form landing folder and move to success folder

    :other (writer):
        writer = csv.writer(write_file, delimiter="|", quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        writer = csv.writer(write_file, delimiter="|", quoting=csv.QUOTE_ALL)
    """
    with \
            open(
                path_join(AI_APP_PATH, f'{registers.path.data_landing}/{filename}'),
                mode="r",
                encoding="utf-8-sig"
            ) as read_file,\
            open(
                path_join(AI_APP_PATH, f'{registers.path.data_success}/{filename}'),
                mode="w",
                encoding="utf-8-sig",
                newline=''
            ) as write_file:
        writer = csv.writer(write_file, delimiter="|", quoting=csv.QUOTE_NONE)
        _row_count: int = 0
        for row in rows(read_file, chunk_size=chunk_size, sep='\n'):
            try:
                if row == '':
                    continue
                writer.writerow(prepare_data(row, delimiter=delimiter))
                _row_count += 1
            except csv.Error as err_stt:
                # TODO: move error record to error folder
                raise WriteCSVError(
                    f'csv.Error: {err_stt}. with row value: {row!r}'
                ) from err_stt
        if compress:
            logger.warning("Compress option does not support yet")
        logger.info(f"Success prepare file {filename!r} with row number {_row_count} rows")
    os.remove(path_join(AI_APP_PATH, f'{registers.path.data_landing}/{filename}'))


def load_file_from_landing():
    for file_name in os.listdir(path_join(AI_APP_PATH, registers.path.data_landing)):
        prepare_csv(file_name)


def convert_to_gzip(
        filename: str
):
    import gzip
    _filenames = filename.rsplit('.', maxsplit=1)
    _filenames.insert(-1, 'gz')
    filename_out = '.'.join(_filenames)

    with \
            open(
                path_join(AI_APP_PATH, f'{registers.path.data_landing}/{filename}'),
                mode="rb",
            ) as read_file, \
            gzip.open(
                path_join(AI_APP_PATH, f'{registers.path.data_success}/{filename_out}'),
                mode="wb",
            ) as write_file:
        write_file.writelines(read_file)
        logger.info(f"Success convert file {filename!r} to gzip compression")
    os.remove(path_join(AI_APP_PATH, f'{registers.path.data_landing}/{filename}'))


def convert_file_from_landing(compress: str):
    for file_name in os.listdir(path_join(AI_APP_PATH, registers.path.data_landing)):
        if compress in {'gzip', 'gz'}:
            convert_to_gzip(file_name)


if __name__ == '__main__':
    # load_file_from_landing()
    convert_file_from_landing(compress='gz')
