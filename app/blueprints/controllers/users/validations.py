from wtforms.validators import ValidationError


class Unique:
    def __init__(self, model, field, message="This element already exists."):
        self.model = model
        self.field = field
        self.message = message

    def __call__(self, form, field):
        check = self.model.query.filter(self.field == field.data).first()
        if check:
            raise ValidationError(self.message)


class NotExists:
    def __init__(self, model, field, message="This element does not exists."):
        self.model = model
        self.field = field
        self.message = message

    def __call__(self, form, field):
        check = self.model.query.filter(self.field == field.data).first()
        if not check:
            raise ValidationError(self.message)


class SameCurrent:
    __field__ = ""

    def __init__(
        self,
        model,
        field,
        field_current,
        message="This element is taken. Please choose a different one.",
    ):
        self.model = model
        self.field = field
        self.field_current = field_current
        self.message = message

    def __call__(self, form, field):
        print(field.data)
        if field.data != getattr(self.field_current, self.__field__):
            check = self.model.query.filter(self.field == field.data).first()
            if check:
                raise ValidationError(self.message)


class SameCurrentUsername(SameCurrent):
    __field__ = "username"


class SameCurrentEmail(SameCurrent):
    __field__ = "email"
