
import roax.schema as s
import unittest

from datetime import datetime
from roax.resource import Resource, operation
from roax.wsgi import App
from webob import Request

_r1_schema = s.dict({
    "id": s.str(),
    "foo": s.int(),
    "bar": s.bool(),
    "dt": s.datetime(),
})

class _Resource1(Resource):
    
    schema = _r1_schema
    
    @operation(
        params = {"id": _r1_schema.properties["id"], "_body": _r1_schema},
        returns = s.dict({"id": _r1_schema.properties["id"]}),
    )
    def create(self, id, _body):
        return {"id": id}

app = App("/", "Title", "1.0")
app.register("/r1", _Resource1())


class TestWSGI(unittest.TestCase):

    def test_create(self):
        request = Request.blank("/r1?id=id1")
        request.method = "POST"
        request.json = {"id": "id1", "foo": 1, "bar": True, "dt": _r1_schema.properties["dt"].json_encode(datetime.now())}
        response = request.get_response(app)
        result = response.json
        self.assertEqual(result, {"id": "id1"})
        self.assertEqual(response.status_code, 200)

if __name__ == "__main__":
    unittest.main()
