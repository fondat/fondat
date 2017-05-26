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

if sys.version_info[:2] < (3, 4):
    raise RuntimeError("Roax requires Python 3.4 or greater")

install_requires = [
	"isodate >= 0.5.4",
    "WebOb >= 1.7.2",
    "wrapt >= 1.10.10"
]

classifiers = [
    "Development Status :: 2 - Pre-Alpha",
    "Intended Audience :: Developers",
    "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
    "Programming Language :: Python :: 3.4",
    "Topic :: Internet :: WWW/HTTP :: WSGI :: Application"
]

long_description = """
Roax (pronounced /ɹoʊ.æks/) is a lightweight framework for building
resource-oriented Python applications.
"""

setup(
    name = "roax",
    version = version(),
    description = "Framework for building resource-oriented Python applications",
    long_description = long_description,
    author = "Paul Bryan",
    author_email = "pbryan@anode.ca",
    license = "Mozilla Public License 2.0",
    classifiers = classifiers,
    url = "https://github.com/pbryan/roax",
    packages = ["roax"],
	package_dir={"": "src"},
    install_requires = install_requires,
    keywords = "wsgi http framework resource roa",
    test_suite = "tests",
    cmdclass = {'test': test}
)
