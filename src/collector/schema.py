from jsonschema import Draft7Validator, draft7_format_checker


class ValidationError(Exception):
    def __init__(self, errors):
        super().__init__()
        self.errors = errors


def validate(schema, value):
    validator = Draft7Validator(schema, format_checker=draft7_format_checker)
    errors = list(validator.iter_errors(value))
    if errors:
        raise ValidationError(errors)


def string():
    return {"type": "string"}


def obj(**properties):
    return {"type": "object", "properties": properties, "required": properties.keys()}
