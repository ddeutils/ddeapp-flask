from flask_wtf import FlaskForm
from flask_wtf.file import FileField, FileAllowed
from flask_login import current_user
from wtforms import (
    StringField,
    BooleanField,
    PasswordField,
    SubmitField
)
from wtforms.fields import EmailField
from wtforms.validators import (
    DataRequired,
    Length,
    Email,
    EqualTo,
    ValidationError
)
from .models import User
from .validations import (
    Unique,
    NotExists,
    SameCurrentUsername,
    SameCurrentEmail,
)


class RegistrationForm(FlaskForm):
    username = StringField(
        name='Username',
        validators=[
            DataRequired(),
            Length(min=2, max=20),
            Unique(
                User,
                User.username,
                'That username is taken. Please choose a different one.'
            )
        ]
    )
    # email = StringField(
    email = EmailField(
        name='Email',
        validators=[
            DataRequired(),
            Email(),
            Unique(
                User,
                User.email,
                'That email is taken. Please choose a different one.'
            )
        ]
    )
    password = PasswordField(name='Password', validators=[DataRequired()])
    confirm_password = PasswordField(name='Confirm Password', validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField(name='Sign Up')


class LoginForm(FlaskForm):
    email = StringField('Email', validators=[DataRequired(), Email()])
    password = PasswordField('Password', validators=[DataRequired()])
    remember = BooleanField('Remember Me')
    submit = SubmitField('Login')


class UpdateAccountForm(FlaskForm):
    username = StringField(
        name='Username',
        validators=[
            DataRequired(),
            Length(min=2, max=20),
            SameCurrentUsername(
                User,
                User.username,
                current_user,
                'That username is taken. Please choose a different one.'
            )
        ]
    )
    email = StringField(
        name='Email',
        validators=[
            DataRequired(),
            Email(),
            SameCurrentEmail(
                User,
                User.email,
                current_user,
                'That email is taken. Please choose a different one.'
            )
        ]
    )
    picture = FileField('Update Profile Picture', validators=[FileAllowed(['jpg', 'png'])])
    submit = SubmitField('Update')


class RequestResetForm(FlaskForm):
    email = StringField(
        'Email',
        validators=[
            DataRequired(),
            Email(),
            NotExists(
                User,
                User.email,
                'There is no account with that email. You must register first.'
            )
        ]
    )
    submit = SubmitField('Request Password Reset')


class ResetPasswordForm(FlaskForm):
    password = PasswordField('Password', validators=[DataRequired()])
    confirm_password = PasswordField('Confirm Password', validators=[DataRequired(), EqualTo('password')])
    submit = SubmitField('Reset Password')
