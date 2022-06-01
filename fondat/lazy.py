"""Module to support lazy evaluation of values."""

import importlib
import logging
import threading

from collections.abc import Callable, Mapping, MutableMapping
from typing import Any


_logger = logging.getLogger(__name__)


class LazyMap(MutableMapping):
    """
    A mapping of key-value pairs, in which a value can be lazily initialized at the time of
    first access. This is useful to allow resources to access other resources, preventing
    circular imports.

    Parameters:
    • init: mapping of key-value pairs to initialize the lazy map

    To lazily initialize a value, set it to a no-argument function callback that has been
    decorated with the @lazy decorator. When the value is then first accessed, the callback
    will be called to initialize the value. The resulting value will then be stored in the
    mapping.
    """

    def __init__(self, init: Mapping | None = None):
        super().__init__()
        self._lock = threading.Lock()
        self._store = {}
        if init is not None:
            for key, value in init.items():
                self[key] = value

    def __getitem__(self, key):
        value = self._store[key]
        if is_lazy(value):
            with self._lock:
                value = self._store[key]
                if is_lazy(value):  # prevent race
                    self._store[key] = value = value()
        return value

    def __setitem__(self, key, value):
        self._store[key] = value

    def __delitem__(self, key):
        del self._store[key]

    def __iter__(self):
        return iter(self._store)

    def __len__(self):
        return len(self._store)

    def __contains__(self, item):
        return item in self._store


def lazy(function: Any) -> Any:
    """Decorate a function to tag it as a lazy initializer."""
    if not callable(function):
        raise TypeError("function must be callable")
    setattr(function, "_fondat_lazy", True)
    return function


def lazy_import(module_name: str) -> Callable:
    """
    Return a lazy callback function that imports a module and returns it.

    Parameters:
    • module_name: the name of the module to import
    """

    def callback():
        return importlib.import_module(module_name)

    return lazy(callback)


def lazy_import_attr(module_name: str, attr_name: str) -> Callable:
    """
    Return a lazy callback function that imports a module and returns an attribute from it.

    Parameters:
    • module_name: the name of the module to import
    • attr_name: the name of the attribute in the module to be returned
    """

    def callback():
        return getattr(importlib.import_module(module_name), attr_name)

    return lazy(callback)


def is_lazy(obj: Any) -> bool:
    """Return True if an object is a callable is tagged as a lazy callback function."""
    return callable(obj) and getattr(obj, "_fondat_lazy", None)
