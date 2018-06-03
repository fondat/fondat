# Quick Start

This document walks through installing Roax and creating a minimal application.

## Installation

```
pip3 install roax
```

## Hello world

Here is a minimal application that responds with `"Hello world!"` when the
client accesses `http://localhost:8000/hello`.

```python
import roax.schema as schema

from roax.resource import Resource, operation
from roax.wsgi import App
from wsgiref.simple_server import make_server

class HelloResource(Resource):

    @operation(returns=schema.str(), security=[])
    def read(self):
        return "Hello world!"

app = App("/", "Hello", "1.0")
app.register_resource("/hello", HelloResource())

if __name__== "__main__":
    make_server("", 8000, app).serve_forever()
```

