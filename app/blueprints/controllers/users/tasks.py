from flask import current_app, url_for
from flask_mail import Message

from conf import settings

from ....app import make_celery
from ....extensions import mail

celery = make_celery(current_app)


@celery.task(name="create_task")
def create_task(task_type):
    print("Success task")
    return True


def send_reset_email(user) -> None:
    token = user.get_reset_token()
    msg = Message(
        "Password Reset Request",
        sender=tuple(settings.ADMINS),
        recipients=[user.email],
    )
    msg.body = f"""To reset your password, visit the following link:
{ url_for('users.reset_token', token=token, _external=True) } \n
If you did not make this request then simply ignore this email and no changes  will be made."""
    mail.send(msg)


def send_reset_token(user) -> None:
    token = user.get_reset_token()
    print(token)


# @celery.task()
# def deliver_contact_email(email, message):
#     """
#     Send a contact e-mail.
#     :param email: E-mail address of the visitor
#     :type user_id: str
#     :param message: E-mail message
#     :type user_id: str
#     :return: None
#     """
#     ctx = {'email': email, 'message': message}
#
#     send_template_message(subject='[Snake Eyes] Contact',
#                           sender=email,
#                           recipients=[current_app.config['MAIL_USERNAME']],
#                           reply_to=email,
#                           template='contact/mail/index', ctx=ctx)
#
#     return None


# ========= from lib.flask_mailplus import send_template_message =========
# def send_template_message(template=None, ctx=None, *args, **kwargs):
#     """
#     Send a templated e-mail using a similar signature as Flask-Mail:
#     http://pythonhosted.org/Flask-Mail/
#     Except, it also supports template rendering. If you want to use a template
#     then just omit the body and html kwargs to Flask-Mail and instead supply
#     a path to a template. It will auto-lookup and render text/html messages.
#     Example:
#         ctx = {'user': current_user, 'reset_token': token}
#         send_template_message('Password reset from Foo', ['you@example.com'],
#                               template='user/mail/password_reset', ctx=ctx)
#     :param subject:
#     :param recipients:
#     :param body:
#     :param html:
#     :param sender:
#     :param cc:
#     :param bcc:
#     :param attachments:
#     :param reply_to:
#     :param date:
#     :param charset:
#     :param extra_headers:
#     :param mail_options:
#     :param rcpt_options:
#     :param template: Path to a template without the extension
#     :param context: Dictionary of anything you want in the template context
#     :return: None
#     """
#     if ctx is None:
#         ctx = {}
#
#     if template is not None:
#         if 'body' in kwargs:
#             raise Exception('You cannot have both a template and body arg.')
#         elif 'html' in kwargs:
#             raise Exception('You cannot have both a template and body arg.')
#
#         kwargs['body'] = _try_renderer_template(template, **ctx)
#         kwargs['html'] = _try_renderer_template(template, ext='html', **ctx)
#
#     mail.send_message(*args, **kwargs)
#
#     return None
#
#
# def _try_renderer_template(template_path, ext='txt', **kwargs):
#     """
#     Attempt to render a template. We use a try/catch here to avoid having to
#     do a path exists based on a relative path to the template.
#     :param template_path: Template path
#     :type template_path: str
#     :param ext: File extension
#     :type ext: str
#     :return: str
#     """
#     try:
#         return render_template('{0}.{1}'.format(template_path, ext), **kwargs)
#     except IOError:
#         pass
