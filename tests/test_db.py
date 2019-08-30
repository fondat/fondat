import roax.db as db
import roax.schema as s


class CustomType:
    def __init__(self, value):
        self.value = value

    def __eq__(self, value):
        return self.value == value


class CustomTypeSchema(s.type):
    def __init__(self, **kwargs):
        super().__init__(python_type=CustomType, **kwargs)

    def json_encode(self, value):
        return self.str_encode(value)

    def json_decode(self, value):
        return self.str_decode(value)

    def str_encode(self, value):
        self.validate(value)
        return s.int().str_encode(value.value)

    def str_decode(self, value):
        result = CustomType(s.int().str_decode(value))
        self.validate(result)
        return result

    def validate(self, value):
        return s.int().validate(value.value)


def test_custom_codec():
    schema = s.dict({"id": s.str(), "custom": CustomTypeSchema()})
    table = db.Table("custom", schema, "id", {})
    assert table.codec("custom").encode(schema["custom"], CustomType(123)) == "123"
    assert table.codec("custom").decode(schema["custom"], "456") == CustomType(456)
