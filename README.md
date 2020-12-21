# Fondat

[![PyPI](https://badge.fury.io/py/fondat.svg)](https://badge.fury.io/py/fondat)
[![License](https://img.shields.io/github/license/fondat/fondat.svg)](https://github.com/fondat/fondat/blob/main/LICENSE)
[![GitHub](https://img.shields.io/badge/github-main-blue.svg)](https://github.com/fondat/fondat/)
[![Test](https://github.com/fondat/fondat/workflows/test/badge.svg)](https://github.com/fondat/fondat/actions?query=workflow/test)
[![Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/psf/black)

Fon·dat: A foundation for Python resource-oriented applications. 

## Introduction

Fondat is a foundation for building resource-oriented applications in Python.
By composing your application as a set of resources that expose operations,
they can be automatically exposed through a HTTP API, GraphQL and/or command
line interface.

## Features

* Resource operations accessed through ASGI-based API.
* Schema enforcement of resource operation parameters and return values.
* Authorization to resource operations enforced through security policies.
* Schema-enforced data structure definition using Python data classes. 
* Python annotations define schema of data classes, operation parameters and return values. 
* Representation of SQL tables as resources, with SQL query builder.
* Monitoring of resource operations and elapsed time in time series databases.
* Generates [OpenAPI](https://www.openapis.org/) interface description, compatible with [Swagger UI](https://swagger.io/tools/swagger-ui/).
* Access to resource operations through command-line interface.
* Asynchronous uniform resource interface.

## Install

```
pip install fondat
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
