# Hello World

Here is a minimal Roax application that responds with `"Hello world!"` when
the user-agent accesses [http://localhost:8000/hello](http://localhost:8000/hello).

## Code

```python
import roax.schema as s
import roax.wsgi
import wsgiref.simple_server

from roax.resource import Resource, operation

class Hello(Resource):

    security = []

    @operation
    def read(self) -> s.str():
        return "Hello world!"

app = roax.wsgi.App("/", "HelloWorld", "1.0")
app.register_resource("/hello", Hello())

if __name__== "__main__":
    wsgiref.simple_server.make_server("", 8000, app).serve_forever()
```

## Breaking it down

Roax enforces schema of data classes, data structures, parameters and return
values, using the Schema module:
```python
import roax.schema as s
```

Roax can expose resources and associated operations as a REST API, using the
WSGI module:
```python
import roax.wsgi
```

Roax organizes an application into resources, each containing one or more
operations:
```python
from roax.resource import Resource, operation
```

An application resource is defined by subclassing `roax.resource.Resource`:
```python
class Hello(Resource):
```

A resource or operation can specify a list of security requirements, which are
enforced when an operation is called. The code below establishes that
operations in the `Hello` resource have no security requirements. By
specifying and empty list, user-agents can access resource operations without
any required authentication or authorization:
```python
    security = []
```

In the `Hello` resource, a single `read` operation is defined. The `read`
operation is one of `create`, `read`, `update`, `delete`, `query`, `action`,
and `patch` operation types. This `read` operation takes no parameters, and
returns a `str` value. The schema of parameters and return types of
operations are enforced when the method is called:
```python
    @operation
    def read(self) -> s.str():
        return "Hello world!"
```

A WSGI application is defined, where registered resources are served off of
the HTTP server's document root `"/"`. The name of the application is
`"HelloWorld"` and has version `"1.0"`:
```python
app = roax.wsgi.App("/", "HelloWorld", "1.0")
```

The `Hello` resource is registered in the WSGI application, and is accessed
at the path `/hello` relative to the application root.
```python
app.register_resource("/hello", Hello())
```
