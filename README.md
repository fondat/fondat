# Roax

[![PyPI](https://badge.fury.io/py/roax.svg)](https://badge.fury.io/py/roax)
[![License](https://img.shields.io/github/license/roax/roax.svg)](https://github.com/roax/roax/blob/master/LICENSE)
[![GitHub](https://img.shields.io/badge/github-master-blue.svg)](https://github.com/roax/roax/)
[![Travis CI](https://travis-ci.org/roax/roax.svg?branch=master)](https://travis-ci.org/roax/roax)
[![Codecov](https://codecov.io/gh/roax/roax/branch/master/graph/badge.svg)](https://codecov.io/gh/roax/roax)
[![Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/psf/black)

Ro·ax /ˈɹoʊ.æks/  
A lightweight Python resource-oriented framework. 

## Introduction

Roax is a lightweight framework for building resource-oriented applications in Python.
By composing your application as a set of resources that expose operations through a uniform
interface, they can be automatically exposed through a REST and/or command line interface.

## Features

* Resource operations accessed through WSGI based REST API.
* Command-line interface to resource operations.
* Generates [OpenAPI](https://www.openapis.org/) interface description, compatible with [Swagger UI](https://swagger.io/tools/swagger-ui/).
* Schema enforcement of resource operation parameters and return values.
* Authorization to resource operations enforced through imperative security policies.

## Quick start

### Installation

```
pip install roax
```

### Hello world

Here is a minimal application that responds with `"Hello world!"` when the
client accesses [http://localhost:8000/hello](http://localhost:8000/hello).

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
