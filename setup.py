#!/usr/bin/env python3

import os
import re
import sys

from setuptools import setup, find_packages
from setuptools.command.test import test as _test

class test(_test):
    def finalize_options(self):
        _test.finalize_options(self)
        self.test_args.insert(0, 'discover')

def read(fname):
    with open(os.path.join(os.path.dirname(__file__), fname)) as f:
        return f.read()

def version():
    match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]", read("src/roax/__init__.py"), re.M)
    if not match:
        raise RuntimeError("failed to parse version")
    return match.group(1)

install_requires = [
    "isodate >= 0.6.0",
    "WebOb >= 1.8.1",
    "wrapt >= 1.10.11"
]

classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Topic :: Internet :: WWW/HTTP :: WSGI :: Application"
]

setup(
    name = "roax",
    version = version(),
    description = "Lightweight framework for building resource-oriented applications.",
    long_description = read("README.rst"),
    author = "Paul Bryan",
    author_email = "pbryan@anode.ca",
    license = "Mozilla Public License 2.0",
    classifiers = classifiers,
    url = "https://github.com/pbryan/roax",
    packages = ["roax"],
    package_dir = {"": "src"},
    python_requires = ">= 3.5",
    install_requires = install_requires,
    keywords = "wsgi http framework resource roa",
    test_suite = "tests",
    cmdclass = {"test": test}
)
