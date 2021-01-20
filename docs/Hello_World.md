# Hello World

Here is a minimal Fondat application that responds with `"Hello world!"` when
the user-agent accesses [http://localhost:8000/](http://localhost:8000/).

## example.py

```python
from fondat.asgi import asgi_app
from fondat.http import Application
from fondat.resource import resource, operation

@resource
class Hello:

    @operation
    async def get(self) -> str:
        return "Hello world!"

app = asgi_app(Application(Hello()))
```

## Testing with Uvicorn

```
uvicorn example:app
```

## Breaking it down

Fondat organizes an application into resources, each containing a set of
operations. A resource is defined by decorating a class with `@resource`:

```python
@resource
class Hello:
```

Each resource operation is decorated with `@operation`. In the `Hello`
resource, a single `get` operation is defined: 

```python
    @operation
    async def get(self) -> str:
        return "Hello world!"
```

Resource operations correlate with HTTP methods: `get`, `put`, `post`,
`delete`, and `patch`. The `get` operation above takes no parameters and
returns a `str` value. The type of parameters is enforced when the operation
is called:

An ASGI application is defined, which wraps an HTTP application, which wraps
a root resource:
```python
app = asgi_app(Application(Hello()))
```
