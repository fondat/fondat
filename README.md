# Roax

[![Travis CI](https://travis-ci.org/pbryan/roax.svg?branch=master)](https://travis-ci.org/pbryan/roax)
[![Codecov](https://codecov.io/gh/pbryan/roax/branch/master/graph/badge.svg)](https://codecov.io/gh/pbryan/roax)
[![PyPI](https://img.shields.io/pypi/v/roax.svg)](https://pypi.org/project/roax/)
![Status](https://img.shields.io/pypi/status/roax.svg)
[![License](https://img.shields.io/github/license/pbryan/roax.svg)](https://github.com/pbryan/roax/blob/master/LICENSE)

Ro·ax /ˈɹoʊ.æks/  
A lightweight Python resource-oriented framework. 

## Introduction

Roax is a resource-oriented framework for building applications in Python.
By composing your application of resources that expose operations in a uniform
interface, they can be exposed through a REST and/or command line interface.

## Features

* Resource operations accessed through WSGI based REST API.
* Command-line interface to resource operations.
* Generates [OpenAPI](https://www.openapis.org/) interface description, compatible with [Swagger UI](https://swagger.io/tools/swagger-ui/).
* Schema enforcement of resource operation parameters and return values.
* Authorization to resource operations enforced through imperative security policies.

## Links

* [Quick start](docs/quick_start.md)
* [GitHub](https://github.com/pbryan/roax)
