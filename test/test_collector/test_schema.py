import pytest

from collector.schema import obj, string, validate, ValidationError


class TestValidateSchema:
    def test_validate_schema_valid_input(self):
        schema = obj(key=string())
        inputs = {"key": "value"}

        validate(schema, inputs)

    @pytest.mark.parametrize(
        "schema,inputs,messages",
        [
            (obj(key=string()), {}, ["'key' is a required property"]),
            (obj(key=string()), None, ["None is not of type 'object'"]),
            (obj(key=string()), {"key": 1}, ["1 is not of type 'string'"]),
            (obj(key=string()), {"key-test": 1}, ["'key' is a required property"]),
        ],
    )
    def test_validate_schema_invalid_input(self, schema, inputs, messages):
        with pytest.raises(ValidationError) as error:
            validate(schema, inputs)

        for (idx, error) in enumerate(error.value.errors):
            assert error.message == messages[idx]
