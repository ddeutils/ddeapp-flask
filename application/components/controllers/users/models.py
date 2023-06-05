# -------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# --------------------------------------------------------------------------

import jwt
import string
import random
import numpy as np
from datetime import (
    datetime,
    timedelta,
)
from sqlalchemy import Table
from sqlalchemy.orm import synonym
from ....extensions import (
    db,
    login_manager,
    bcrypt
)
from flask import current_app
from flask_login import AnonymousUserMixin
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql import func


@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


user_role = Table(
    'ctr_web_user_role',
    db.metadata,
    db.Column('map_id', db.Integer, primary_key=True),
    db.Column(
        'user_id',
        db.Integer,
        db.ForeignKey('ctr_web_user.user_id', ondelete="CASCADE"),
        nullable=False,
    ),
    db.Column(
        'role_id',
        db.Integer,
        db.ForeignKey('ctr_web_role.role_id', ondelete="CASCADE"),
        nullable=False,
    ),
    db.Column(
        'update_date',
        db.DateTime(timezone=True),
        default=datetime.now(),
    ),
    extend_existing=True,
    # autoload=True,
    autoload_with=db.engine,
)


class UserRole(db.Model):
    """User and Role mapping Model"""
    __table__ = user_role

    id = synonym('map_id')


class User(db.Model):
    """User Model represent table `ctr_web_user`
    """

    # Set model properties
    __tablename__ = 'ctr_web_user'
    __table_args__ = {
        'autoload': True,
        'extend_existing': True,
        'autoload_with': db.engine
    }

    user_id = db.Column('user_id', db.BigInteger, primary_key=True)
    user_public_id = db.Column(
        'public_id',
        # db.BigInteger,  # We want to convert timestamp value to base32
        db.String(128),
        unique=True,
        nullable=False,
        default=lambda: np.base_repr(
            int(datetime.now().timestamp() * 1000000), base=32
        )
    )
    username = db.Column('user_name', db.String(128), nullable=False)
    email = db.Column('user_email', db.String(256), nullable=False)
    password = db.Column('user_pass', db.String(256), nullable=False)
    image_file = db.Column(
        'image_file',
        db.String(256),
        nullable=False,
        default='default.jpg'
    )
    active = db.Column('active_flag', db.Boolean, nullable=False, default=True)
    register_date = db.Column('register_date',
                              db.DateTime(timezone=True),
                              default=datetime.now
                              )
    update_date = db.Column('update_date',
                            db.DateTime(timezone=True),
                            default=datetime.now,
                            onupdate=datetime.now
                            )

    # Synonym List
    id = synonym('user_id')
    public_id = synonym('user_public_id')

    # Define the relationship to Role via UserRole
    roles = db.relationship(
        'Role',
        order_by="asc(Role.role_id)",
        secondary=user_role,
        backref=db.backref('users', lazy='dynamic'),
        lazy='dynamic',  # select, joined, subquery
        uselist=True,
    )

    # Define the relationship to Post
    # posts = db.relationship(
    #     'Post',
    #     backref='author',
    #     lazy=True,
    #     cascade='all, delete-orphan',
    #     passive_deletes = True
    # )

    def __init__(self, **kwargs):
        super(User, self).__init__(**kwargs)
        # self.short_url = self.generate_short_characters()

    def __repr__(self):
        return f"User('{self.username}', '{self.email}', '{self.image_file}')"

    def __eq__(self, other):
        """Checks the equality of two `User` objects using `get_id`.
        """
        if isinstance(other, User):
            return self.get_id() == other.get_id()
        return NotImplemented

    def __ne__(self, other):
        """Checks the inequality of two `User` objects using `get_id`.
        """
        equal = self.__eq__(other)
        if equal is NotImplemented:
            return NotImplemented
        return not equal

    @property
    def is_anonymous(self):
        return isinstance(self, AnonymousUserMixin)

    def get_id(self):
        return str(self.user_id)

    @property
    def is_active(self):
        return self.active

    @property
    def is_authenticated(self) -> bool:
        return self.is_active

    @property
    def is_admin(self) -> bool:
        return (
                self.is_authenticated and any("Administrator" in role.name for role in self.roles)
        )

    def get_reset_token(self, expires_hour: int = 1):
        expires_hour: int = current_app.config.get('RESET_TOKEN_EXPIRE_HOURS', expires_hour)
        return jwt.encode(
            {
                'user_id': self.public_id,
                'exp': datetime.utcnow() + timedelta(hours=expires_hour)
            },
            current_app.secret_key,
            algorithm="HS256",
        )

    def generate_short_characters(self):
        characters = (string.digits + string.ascii_letters)
        picked_chars = ''.join(random.choices(characters, k=3))
        link = self.query.filter_by(short_url=picked_chars).first()

        if link:
            self.generate_short_characters()
        else:
            return picked_chars

    @staticmethod
    def verify_reset_token(token):
        try:
            data = jwt.decode(
                token,
                current_app.secret_key,
                algorithms=["HS256"]
            )['user_id']
        except jwt.ExpiredSignatureError:
            return None
        return User.query.get_or_none(data)

    def save(self):
        db.session.add(self)
        db.session.commit()

    def has_permission(self, permission):
        for role in self.roles:
            print(f"Role: {role}")
            policies = role.policies
            print(f"Policy: {policies}")
            if policies and any(permission in policy.allowed_routes for policy in policies):
                return True
        return False


role_policy = Table(
    'ctr_web_role_policy',
    db.metadata,
    db.Column('map_id', db.Integer, primary_key=True),
    db.Column('role_id', db.Integer, db.ForeignKey('ctr_web_role.role_id'), nullable=False),
    db.Column('policy_id', db.Integer, db.ForeignKey('ctr_web_policy.policy_id'), nullable=False),
    db.Column('update_date', db.DateTime(timezone=True), default=datetime.now()),
    # autoload=True,
    extend_existing=True,
    autoload_with=db.engine,
)


class RolePolicy(db.Model):
    """Role and Policy mapping Model"""
    __table__ = role_policy

    id = synonym('map_id')


class Role(db.Model):
    """Role Model represent table `ctr_web_role`

    For role-based access control (RBAC)
        Some common roles that you might include in your application are:

        - Administrator: a user with full access to all features and functions of the application
        - Moderator: a user with the ability to manage and moderate content within the application
        - User: a regular user of the application with limited permissions

        Example:
            - Administrator
                - View content
                - Edit content
                - Manage users
                - Manage roles and policies
            - Moderator
                - View content
                - Edit content
            - User
                - View content
    """

    __tablename__ = 'ctr_web_role'
    __table_args__ = {
        'autoload': True,
        'extend_existing': True,
        'autoload_with': db.engine
    }

    role_id = db.Column('role_id', db.BigInteger, primary_key=True)
    role_name = db.Column('role_name', db.String(128), nullable=False, unique=True)
    description = db.Column('description', db.Text, nullable=True)
    register_date = db.Column('register_date', db.DateTime(timezone=True), default=datetime.now())
    update_date = db.Column('update_date', db.DateTime(timezone=True), default=datetime.now(), onupdate=datetime.now)

    id = synonym('role_id')
    name = synonym('role_name')
    desc = synonym('description')

    # Define the relationship to Policy via RolePolicy
    policies = db.relationship(
        'Policy',
        order_by="asc(Policy.policy_id)",
        # Using this, we can define the role_policy at a later point, as long as it’s available
        # to the callable after all module initialization is complete.
        secondary=lambda: role_policy,
        backref=db.backref('roles', lazy=True),
        lazy='subquery',
        uselist=True,
    )

    def __init__(self, **kwargs):
        super(Role, self).__init__(**kwargs)

    def __repr__(self):
        return f"Role('{self.role_name}', '{self.description}')"


class Policy(db.Model):
    """Some common policies that you might include in your application are:

        - View content: allows a user to view content within the application
        - Edit content: allows a user to create, update, and delete content within the application
        - Manage users: allows a user to create, update, and delete users within the application
        - Manage roles and policies: allows a user to create, update, and delete roles and policies
            within the application

        Example:
            - viewer, *, View content policy
            - editor, *, Edit content policy
    """

    __tablename__ = 'ctr_web_policy'
    __table_args__ = {
        'autoload': True,
        'extend_existing': True,
        'autoload_with': db.engine
    }

    policy_id = db.Column('policy_id', db.BigInteger, primary_key=True)
    policy_name = db.Column('policy_name', db.String(128), nullable=False)
    allowed_routes = db.Column(db.Text, nullable=False)
    description = db.Column('description', db.Text, nullable=True)
    register_date = db.Column('register_date', db.DateTime(timezone=True), default=datetime.now())
    update_date = db.Column('update_date', db.DateTime(timezone=True), default=datetime.now(), onupdate=datetime.now)

    id = synonym('policy_id')
    name = synonym('policy_name')


user_group = db.Table(
    'ctr_web_user_group',
    db.metadata,
    db.Column('map_id', db.BigInteger, primary_key=True),
    db.Column('user_id', db.BigInteger, db.ForeignKey('ctr_web_user.user_id')),
    db.Column('group_id', db.BigInteger, db.ForeignKey('ctr_web_group.group_id')),
    db.Column('update_date', db.DateTime(timezone=True), default=datetime.now(), onupdate=datetime.now),
    # autoload=True,
    extend_existing=True,
    autoload_with=db.engine,
)

group_role = db.Table(
    'ctr_web_group_role',
    db.metadata,
    db.Column('map_id', db.BigInteger, primary_key=True),
    db.Column('group_id', db.BigInteger, db.ForeignKey('ctr_web_group.group_id')),
    db.Column('role_id', db.BigInteger, db.ForeignKey('ctr_web_role.role_id')),
    db.Column('update_date', db.DateTime(timezone=True), default=datetime.now(), onupdate=datetime.now),
    # autoload=True,
    extend_existing=True,
    autoload_with=db.engine,
)

group_to_group = db.Table(
    'ctr_web_group_to_group',
    db.metadata,
    db.Column('map_id', db.BigInteger, primary_key=True),
    db.Column('parent_id', db.BigInteger, db.ForeignKey('ctr_web_group.group_id')),
    db.Column('child_id', db.BigInteger, db.ForeignKey('ctr_web_group.group_id')),
    db.Column('update_date', db.DateTime(timezone=True), default=datetime.now(), onupdate=datetime.now),
    # autoload=True,
    extend_existing=True,
    autoload_with=db.engine,
)


class UserGroup(db.Model):
    __table__ = user_group

    id = synonym('map_id')


class GroupRole(db.Model):
    __table__ = group_role

    id = synonym('map_id')


class GroupToGroup(db.Model):
    __table__ = group_to_group

    id = synonym('map_id')


class Group(db.Model):
    """Group Model represent table `ctr_web_group`

        Example:
            - app_owner
            - app_tester
            - app_user
    """
    __tablename__ = 'ctr_web_group'

    group_id = db.Column('group_id', db.BigInteger, primary_key=True)
    # Can set: db.Column('group_name', db.String(128), index=True, unique=True, nullable=False)
    group_name = db.Column('group_name', db.String(128), unique=True, nullable=False)
    description = db.Column('description', db.Text, nullable=True)
    register_date = db.Column('register_date', db.DateTime(timezone=True), default=datetime.now())
    update_date = db.Column('update_date', db.DateTime(timezone=True), default=datetime.now(), onupdate=datetime.now)

    id = synonym('group_id')

    __table_args__ = {
        'autoload': True,
        'extend_existing': True,
        'autoload_with': db.engine,
        # **db.Index(
        #     'idx_name', group_name, mysql_length=255, mysql_collate='utf8_unicode_ci', mysql_using='btree',
        #     mysql_desc=True
        # ),
    }

    # # Define the relationship to User via UserGroups
    users = db.relationship(
        'User',
        secondary=lambda: user_group,
        order_by='asc(User.user_id)',
        lazy='select',
        uselist=True,
        backref=db.backref(
            'groups',
            lazy='select',
            uselist=True
        ),
    )

    # Define the relationship to Group via GroupToGroups
    children = db.relationship(
        'Group',
        secondary=group_to_group,
        primaryjoin=(group_id == group_to_group.c.parent_id),
        secondaryjoin=(group_id == group_to_group.c.child_id),
        remote_side=[group_to_group.c.parent_id],
        # "all, delete-orphan, delete, merge"
        # cascade="all, delete-orphan",
        # single_parent=True,
        lazy='joined',
        backref=db.backref(
            "parents",
            order_by=group_id,
            lazy=True,
            # cascade="all, delete-orphan",
        ),
    )

    # Define the relationship to Role via RoleGroup
    roles = db.relationship(
        'Role',
        secondary=lambda: group_role,
        lazy='subquery',
        uselist=True,
        backref=db.backref('groups', lazy=True),
    )

    def __init__(self, **kwargs):
        super(Group, self).__init__(**kwargs)

    def __repr__(self):
        return f"Group('{self.group_name}', '{self.description}')"


def initial_data():
    """Initialize Data to target database with any Model classes"""
    # Create Demo Users

    for t in db.metadata.sorted_tables:
        print(t.name)
