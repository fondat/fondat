"""Roax module to perform lazy evaluation."""

import importlib
import threading


class LazyMap:
    """
    A map of key-value pairs, in which value can be lazily initialized at time
    of first access. This is useful to allow resources to access other
    resources and break any circular dependencies. 

    Parameters:
    • init: Mapping of key-value pairs to initialize the lazy map.

    To have a map value be lazily initialized, set it to a no-argument function
    callback that has been decorated with the @lazy decorator. When the value
    is then accessed, this causes the callback to be called to initialize the
    value. The resulting value will then be stored in the mapping.

    All map values can be set, retrieved and deleted as items, like: map["key"]
    and attributes, like: map.key.
    """

    def __init__(self, init={}):
        super().__init__()
        self._roax_lock = threading.Lock()
        self._roax_map = {}
        for name, value in init.items():
            setattr(self, name, value)

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(
                f"{self.__class__.__name__} object has no attribute '{name}'"
            )

    def __setattr__(self, name, value):
        if name.startswith("_roax_") or name in super().__dir__():
            super().__setattr__(name, value)
        else:
            self[name] = value

    def __delattr__(self, name):
        if name.startswith("_roax_") or name in super().__dir__():
            super().__delattr__(name)
        else:
            try:
                del self[name]
            except KeyError:
                raise AttributeError(name)

    def __dir__(self):
        return [*super().__dir__(), *iter(self._roax_map)]

    def __len__(self):
        return len(self._roax_map)

    def __getitem__(self, key):
        return self._roax_resolve(key)

    def __setitem__(self, key, value):
        self._roax_map[key] = value

    def __delitem__(self, key):
        del self._roax_map[key]

    def __iter__(self):
        return iter(self._roax_map)

    def __contains__(self, item):
        return item in self._roax_map

    def _roax_resolve(self, key):
        value = self._roax_map[key]
        if is_lazy(value):
            with self._roax_lock:
                if is_lazy(value):  # prevent race
                    self._roax_map[key] = value()
            value = self._roax_map[key]
        return value


def lazy(function):
    """Decorator that tags a callable to be used for lazy initialization."""
    if not callable(function):
        raise TypeError
    setattr(function, "_roax_lazy", True)
    return function


def lazy_class(spec):
    """
    Return a lazy callback function to import and load a class.

    Parameters:
    • spec: String specifying "module:class" to import module and resolve class.
    """
    def callback():
        module, class_ = spec.split(":", 1)
        return getattr(importlib.import_module(module), class_)

    return lazy(callback)


def is_lazy(function):
    """Return True if the function is a tagged as a lazy callback function."""
    return callable(function) and getattr(function, "_roax_lazy", None)
