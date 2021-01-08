"""Fondat lazy evaluation module."""

import importlib
import threading

from collections.abc import Callable
from typing import Any


class LazyMap:
    """
    A map-like object of key-value pairs, in which a value can be lazily
    initialized at the time of first access. This is useful to allow resources
    to access other resources, preventing circular dependencies.

    Parameters:
    • init: Mapping of key-value pairs to initialize the lazy map.

    To lazily initialize a value, set it to a no-argument function callback
    that has been decorated with the @lazy decorator. When the value is then
    first accessed, the callback will be called to initialize the value.
    The resulting value will then be stored in the mapping.

    All map values can be set, retrieved and deleted as items, like: map["key"]
    and attributes, like: map.key.
    """

    def __init__(self, init=None):
        super().__init__()
        self._fondat_lock = threading.Lock()
        self._fondat_map = {}
        if init is not None:
            for name, value in init.items():
                self[name] = value

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(
                f"{self.__class__.__name__} object has no attribute '{name}'"
            )

    def __setattr__(self, name, value):
        if name.startswith("_fondat_") or name in super().__dir__():
            super().__setattr__(name, value)
        else:
            self[name] = value

    def __delattr__(self, name):
        if name.startswith("_fondat_") or name in super().__dir__():
            super().__delattr__(name)
        else:
            try:
                del self[name]
            except KeyError:
                raise AttributeError(name)

    def __dir__(self):
        return [*super().__dir__(), *iter(self._fondat_map)]

    def __len__(self):
        return len(self._fondat_map)

    def __getitem__(self, key):
        return self._fondat_resolve(key)

    def __setitem__(self, key, value):
        self._fondat_map[key] = value

    def __delitem__(self, key):
        del self._fondat_map[key]

    def __iter__(self):
        return iter(self._fondat_map)

    def __contains__(self, item):
        return item in self._fondat_map

    def _fondat_resolve(self, key):
        value = self._fondat_map[key]
        if is_lazy(value):
            with self._fondat_lock:
                if is_lazy(value):  # prevent race
                    self._fondat_map[key] = value()
            value = self._fondat_map[key]
        return value


def lazy(function: Callable) -> Callable:
    """
    Decorate a function to tag it as a lazy initializer.
    """
    if not callable(function):
        raise TypeError("function must be callable")
    setattr(function, "_fondat_lazy", True)
    return function


def lazy_import(module_name: str) -> Callable:
    """
    Return a lazy callback function that imports a module and returns it.

    Parameters:
    • module_name: The name of the module to import.
    """

    def callback():
        return importlib.import_module(module_name)

    return lazy(callback)


def lazy_import_attr(module_name: str, attr_name: str) -> Callable:
    """
    Return a lazy callback function that imports a module and returns an
    attribute from it.

    Parameters:
    • module_name: The name of the module to import.
    • attr_name: The name of the attribute in the module to be returned.
    """

    def callback():
        return getattr(importlib.import_module(module_name), attr_name)

    return lazy(callback)


def is_lazy(obj: Any) -> bool:
    """
    Return True if an object is a callable is tagged as a lazy callback function.
    """
    return callable(obj) and getattr(obj, "_fondat_lazy", None)
