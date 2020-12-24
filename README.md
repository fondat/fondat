# fondat-core

[![PyPI](https://badge.fury.io/py/fondat-core.svg)](https://badge.fury.io/py/fondat-core)
[![License](https://img.shields.io/github/license/fondat/fondat-core.svg)](https://github.com/fondat/fondat-core/blob/main/LICENSE)
[![GitHub](https://img.shields.io/badge/github-main-blue.svg)](https://github.com/fondat/fondat-core/)
[![Test](https://github.com/fondat/fondat-core/workflows/test/badge.svg)](https://github.com/fondat/fondat-core/actions?query=workflow/test)
[![Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/psf/black)

A foundation for Python resource-oriented applications. 

## Introduction

Fondat is a foundation for building resource-oriented applications in Python.
By composing your application as a set of resources that expose operations,
they can be automatically exposed through an HTTP API.

## Features

* Resource operations accessed through ASGI-based HTTP API.
* Type encoding and validation of resource operation parameters and return values.
* Authorization to resource operations enforced through security policies.
* Lightweight abstraction of SQL tables, indexes and queries.
* Monitoring of resource operations and elapsed time in time series databases.
* Generates [OpenAPI](https://www.openapis.org/) interface description, compatible with [Swagger UI](https://swagger.io/tools/swagger-ui/).
* Asynchronous uniform resource interface.

## Install

```
pip install fondat-core
```

## Documentation

See the [docs](https://github.com/fondat/fondat/tree/main/docs) folder for documentation.

## Develop

```
poetry install
poetry run pre-commit install
```

## Test

```
poetry run pytest
```
