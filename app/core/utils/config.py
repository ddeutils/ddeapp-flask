# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import os
import yaml
from typing import Optional
from ..utils.reusables import (
    path_join,
    merge_dicts,
)

AI_APP_PATH: str = os.getenv(
    'AI_APP_PATH',
    os.path.abspath(path_join(os.path.dirname(__file__), '../../..'))
)

CONF_PATH: str = os.path.join(AI_APP_PATH, 'conf')


class Params:
    """Parameter variables object keeping from parameters.yaml file"""

    def __init__(
            self,
            parameters: Optional[dict] = None,
            param_name: Optional[str] = None
    ):
        __param_name: str = param_name or 'parameters.yaml'
        if not parameters:
            with open(
                    os.path.join(CONF_PATH, __param_name), encoding='utf8'
            ) as f:
                parameters = yaml.load(f, Loader=yaml.Loader)

        if parameters:
            self.__dict__.update(**{
                k: v for k, v in self.__class__.__dict__.items()
                if '__' not in k and not callable(v)
            })
            self.__dict__.update(**parameters)
        self.__dict__ = self.__handle_inner_structures()

    def __str__(self):
        return str(
            {
                key: value
                for key, value in self.__dict__.items()
                if not key.startswith('_')
            }
        )

    def __handle_inner_structures(self):
        for k, v in self.__dict__.items():
            if isinstance(v, dict):
                self.__dict__[k] = Params(v)
        return self.__dict__

    def __getattr__(self, item):
        return getattr(self.__dict__, item)

    def __getitem__(self, item):
        return self.__dict__[item]


_registers = Params(param_name='registers.yaml')


class Environs:
    """Environment variables object keeping from .env file"""

    def __init__(self, env_name: Optional[str] = None, reload: bool = True):
        __env_name: str = env_name or '.env'
        if reload:
            result: dict = {}
            with open(
                    os.path.join(AI_APP_PATH, __env_name), encoding='utf8'
            ) as file:
                # TODO: fix reading logic of .env file
                for line in file:
                    if line.startswith('#'):
                        continue
                    if len(line_split := line.replace('\n', '').split('=')) == 2:
                        result[line_split[0]]: str = str(eval(line_split[1]))
            _result: dict = merge_dicts(
                result, {
                    key: value
                    for key, value in os.environ.items()
                    if key in _registers.env_variables
                }
            )
            self.__dict__.update(**_result)
            os.environ.update(**_result)

    def __getattr__(self, item: str):
        try:
            return getattr(self.__dict__, item)
        except AttributeError:
            return None

    def __getitem__(self, item: str):
        try:
            return self.__dict__[item]
        except KeyError:
            return None
