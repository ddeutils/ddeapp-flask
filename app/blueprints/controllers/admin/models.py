# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------

from typing import Dict

from sqlalchemy.orm import synonym

from app.core.utils.reusables import to_snake_case

from ...frontend.nodes.models import Node
from ..users.models import (
    Group,
    GroupRole,
    GroupToGroup,
    Policy,
    Role,
    RolePolicy,
    User,
    UserGroup,
    UserRole,
)


class MetaView(type):

    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        cls._var = 5
        cls.__view_title__ = ''
        cls.__view_cols__ = []
        cls.__view_cols_update__ = {}
        cls.__view_cols_create__ = {}


class BaseView:
    __view_title__ = 'BaseView'
    __view_cols__ = []
    __view_cols_update__ = {}
    __view_cols_create__ = {}

    @classmethod
    def v_title(cls):
        return cls.__view_title__

    @classmethod
    def v_name(cls):
        return to_snake_case(cls.v_title())

    @classmethod
    def v_columns(cls):
        return cls.__view_cols__

    @classmethod
    def v_columns_create(cls):
        return cls.__view_cols_create__

    @classmethod
    def can_update(cls, col: str):
        return col in cls.__view_cols_update__

    @property
    def view_items(self):
        return NotImplementedError


class UserView(User, BaseView):
    __view_title__ = 'User'
    __view_cols__ = [
        'ID',
        'Username',
        'Email',
        'Image File',
        'Active Flag',
        'Register Date',
        'Update Date',
    ]
    __view_cols_update__ = {
        'Username': 'username',
        'Email': 'email',
        'Image File': 'image_file',
    }

    __view_cols_create__ = {
        # 'Username': 'user_name',
        'Username': 'username',
        'Email': 'user_email',
        'Password': 'user_pass',
    }

    __view_cols_search__ = ['username']

    @property
    def view_items(self):
        return {
            'ID': self.user_id,
            'Username': self.username,
            'Email': self.email,
            'Image File': self.image_file,
            'Active Flag': self.active,
            'Register Date': self.register_date.strftime('%Y/%m/%d %H:%M:%S'),
            'Update Date': self.update_date.strftime('%Y/%m/%d %H:%M:%S'),
        }


class RoleView(Role, BaseView):
    __view_title__ = 'Role'
    __view_cols__ = [
        'ID',
        'Role Name',
        'Description',
        'Register Date',
        'Update Date',
    ]

    __view_cols_update__ = {
        'Role Name': 'role_name',
        'Description': 'description',
    }

    __view_cols_create__ = {
        'Role Name': 'role_name',
        'Description': 'description'
    }

    __view_cols_search__ = ['role_name']

    @property
    def view_items(self):
        return {
            'ID': self.id,
            'Role Name': self.role_name,
            'Description': self.description,
            'Register Date': self.register_date.strftime('%Y/%m/%d %H:%M:%S'),
            'Update Date': self.update_date.strftime('%Y/%m/%d %H:%M:%S'),
        }


class UserRoleView(UserRole, BaseView):
    __view_title__ = 'UserRole'
    __view_cols__ = [
        'ID',
        'User ID',
        'Role ID',
        'Update Date',
    ]

    __view_cols_update__ = {
        'User ID': 'user_id',
        'Role ID': 'role_id',
    }

    __view_cols_create__ = {
        'User ID': 'user_id',
        'Role ID': 'role_id',
    }
    __view_cols_search__ = ['user_id', 'role_id']

    @property
    def view_items(self):
        return {
            'ID': self.map_id,
            'User ID': self.user_id,
            'Role ID': self.role_id,
            'Update Date': self.update_date.strftime('%Y/%m/%d'),
        }


class UserGroupView(UserGroup, BaseView):
    __view_title__ = 'UserGroup'
    __view_cols__ = [
        'ID',
        'User ID',
        'Group ID',
        'Update Date',
    ]

    __view_cols_update__ = {
        'User ID': 'user_id',
        'Group ID': 'group_id',
    }

    __view_cols_create__ = {
        'User ID': 'user_id',
        'Group ID': 'group_id',
    }

    __view_cols_search__ = ['user_id', 'group_id']

    @property
    def view_items(self):
        return {
            'ID': self.map_id,
            'User ID': self.user_id,
            'Group ID': self.group_id,
            'Update Date': self.update_date.strftime('%Y/%m/%d'),
        }


class GroupView(Group, BaseView):
    __view_title__ = 'Group'
    __view_cols__ = [
        'ID',
        'Group Name',
        'Description',
        'Register Date',
        'Update Date',
    ]

    __view_cols_update__ = {
        'Group Name': 'group_name',
        'Description': 'description',
    }

    __view_cols_create__ = {
        'Group Name': 'group_name',
        'Description': 'description',
    }

    __view_cols_search__ = ['group_name']

    @property
    def view_items(self):
        return {
            'ID': self.group_id,
            'Group Name': self.group_name,
            'Description': self.description,
            'Register Date': self.register_date.strftime('%Y/%m/%d'),
            'Update Date': self.update_date.strftime('%Y/%m/%d'),
        }


class GroupToGroupView(GroupToGroup, BaseView):
    __view_title__ = 'GroupToGroup'
    __view_cols__ = [
        'ID',
        'Parent ID',
        'Child ID',
        'Update Date',
    ]

    __view_cols_update__ = {
        'Parent ID': 'parent_id',
        'Child ID': 'child_id',
    }

    __view_cols_create__ = {
        'Parent ID': 'parent_id',
        'Child ID': 'child_id',
    }

    __view_cols_search__ = ['parent_id', 'child_id']

    @property
    def view_items(self):
        return {
            'ID': self.map_id,
            'Parent ID': self.parent_id,
            'Child ID': self.child_id,
            'Update Date': self.update_date.strftime('%Y/%m/%d'),
        }


class PolicyView(Policy, BaseView):
    __view_title__ = 'Policy'
    __view_cols__ = [
        'ID',
        'Policy Name',
        'Allowed Routes',
        'Description',
        'Register Date',
        'Update Date',
    ]

    __view_cols_update__ = {
        'Policy Name': 'policy_name',
        'Allowed Routes': 'allowed_routes',
        'Description': 'description',
    }

    __view_cols_create__ = {
        'Policy Name': 'policy_name',
        'Allowed Routes': 'allowed_routes',
        'Description': 'description',
    }

    __view_cols_search__ = ['policy_name']

    @property
    def view_items(self):
        return {
            'ID': self.policy_id,
            'Policy Name': self.policy_name,
            'Allowed Routes': self.allowed_routes,
            'Description': self.description,
            'Register Date': self.register_date.strftime('%Y/%m/%d'),
            'Update Date': self.update_date.strftime('%Y/%m/%d'),
        }


class RolePolicyView(RolePolicy, BaseView):
    __view_title__ = 'RolePolicy'
    __view_cols__ = [
        'ID',
        'Role ID',
        'Policy ID',
        'Update Date',
    ]

    __view_cols_update__ = {
        'Role ID': 'role_id',
        'Policy ID': 'policy_id',
    }

    __view_cols_create__ = {
        'Role ID': 'role_id',
        'Policy ID': 'policy_id',
    }

    __view_cols_search__ = ['role_id', 'policy_id']

    @property
    def view_items(self):
        return {
            'ID': self.map_id,
            'Role ID': self.role_id,
            'Policy ID': self.policy_id,
            'Update Date': self.update_date.strftime('%Y/%m/%d'),
        }


class GroupRoleView(GroupRole, BaseView):
    __view_title__ = 'GroupRole'
    __view_cols__ = [
        'ID',
        'Group ID',
        'Role ID',
        'Update Date',
    ]

    __view_cols_update__ = {
        'Group ID': 'group_id',
        'Role ID': 'role_id',
    }

    __view_cols_create__ = {
        'Group ID': 'group_id',
        'Role ID': 'role_id',
    }

    __view_cols_search__ = ['role_id', 'group_id']

    @property
    def view_items(self):
        return {
            'ID': self.map_id,
            'Group ID': self.group_id,
            'Role ID': self.role_id,
            'Update Date': self.update_date.strftime('%Y/%m/%d'),
        }


class NodeView(Node, BaseView):
    __view_title__ = 'Node'
    __view_cols__ = [
        'System Type',
        'Name',
        'Type',
        'Data Date',
        'Run Date',
        'Run Type',
        'Run Count',
        'Retention Value',
        'Retention Column',
        'Active Flag',
    ]

    __view_cols_update__ = {
        'Type': 'type',
        'Data Date': 'data_date',
        'Run Date': 'run_date',
        'Run Type': 'run_type',
        'Run Count': 'run_count_now',
        'Retention Value': 'rtt_value',
        'Retention Column': 'rtt_column',
    }

    __view_cols_create__ = {
        'Node name': 'table_name',
    }

    __view_cols_search__ = ['table_name']

    id = synonym('name')

    @property
    def view_items(self):
        return {
            'System Type': self.sys_type,
            'Name': self.name,
            'Type': self.type,
            'Data Date': self.data_date.strftime('%Y/%m/%d'),
            'Run Date': self.run_date.strftime('%Y/%m/%d'),
            'Run Type': self.run_type,
            'Run Count': self.run_count_now,
            'Retention Value': self.rtt_value,
            'Retention Column': self.rtt_column,
            'Active Flag': self.active,
        }


MODEL_VIEWS: Dict = {
    model.v_name(): model
    for model in [
        UserView,
        RoleView,
        UserRoleView,
        UserGroupView,
        PolicyView,
        RolePolicyView,
        GroupView,
        GroupToGroupView,
        GroupRoleView,
        NodeView,
    ]
}
