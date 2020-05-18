# Roax

[![PyPI](https://badge.fury.io/py/roax.svg)](https://badge.fury.io/py/roax)
[![License](https://img.shields.io/github/license/roax/roax.svg)](https://github.com/roax/roax/blob/master/LICENSE)
[![GitHub](https://img.shields.io/badge/github-master-blue.svg)](https://github.com/roax/roax/)
[![Test](https://github.com/roax/roax/workflows/Test/badge.svg)](https://github.com/roax/roax/actions?query=workflow/test)
[![Black](https://img.shields.io/badge/code%20style-black-black.svg)](https://github.com/psf/black)

Ro·ax /ˈɹoʊ.æks/: A lightweight Python resource-oriented framework. 

## Introduction

Roax is a lightweight framework for building resource-oriented applications in Python.
By composing your application as a set of resources that expose operations through a uniform
interface, they can be automatically exposed through a REST and/or command line interface.

## Features

* Resource operations accessed through WSGI-based REST API.
* Schema enforcement of resource operation parameters and return values.
* Authorization to resource operations enforced through security policies.
* Schema-enforced data structure definition using Python data classes. 
* Python annotations define schema of data classes, operation parameters and return values. 
* Representation of SQL tables as resources, with SQL query builder.
* Monitoring of resource operations and elapsed time in time series databases.
* Generates [OpenAPI](https://www.openapis.org/) interface description, compatible with [Swagger UI](https://swagger.io/tools/swagger-ui/).
* Access to resource operations through command-line interface.

## Install

```
pip install roax
```

## Documentation

See the [docs](https://github.com/roax/roax/tree/master/docs) folder for documentation.

## Develop

```
poetry install
poetry run pre-commit install
```

## Test

```
poetry run pytest
```
