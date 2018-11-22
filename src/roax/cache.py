"""Module to cache resource items."""

# Copyright © 2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import wrapt

from datetime import datetime, timedelta
from inspect import signature


def cache(max_size=None, max_age=None, id="id"):
    """
    Class decorator that augments a resource class to cache resource items. If
    `max_size` is specified, then when the cache is full, the oldest entry is
    evicted to make room for latest entry. If `max_age` is specified, then any
    entry exceeding that age is evicted.

    This decorator decorates `read`, `update` and `delete` methods (if defined).
    It also adds an `_invalidate` method, which invalidates item(s) in the cache.

    :param max_size: Maximum number of cache entries.
    :param max_age: Maximum age to retain cache entry, as `datetime.timedelta`.
    :param id: Identity parameter used in `read`, `update` and `delete` methods. ["id"]
    """
    if not max_size and not max_age:
        raise ValueError("one of max_size or max_age must be specified")

    if max_age and not isinstance(max_age, timedelta):
        raise ValueError("max_age must be of type datetime.timedelta")

    def get_id_arg(wrapped, instance, args, kwargs):
        return signature(wrapped).bind(*args, **kwargs).arguments[id]

    def evict_expired(instance):
        if max_age:
            now = datetime.utcnow()
            for key in list(instance._roax_cache_):  # avoid iteration over modified dict
                entry = instance._roax_cache_.get(key)
                if entry:
                    time, value = entry
                    if now > time + max_age:
                        instance._invalidate(key) 

    def evict_oldest(instance):
        oldest_key, oldest_time = None, None
        for key in list(instance._roax_cache_):  # avoid iteration over modified dict
            entry = instance._roax_cache_.get(key)
            if entry:
                entry_time, entry_value = entry
                if entry and (oldest_time == None or entry_time < oldest_time):
                    oldest_key = key
                    oldest_time = entry_time
        if oldest_key:
            instance._invalidate(oldest_key)

    def get_cache_value(instance, id):
        entry = instance._roax_cache_.get(id)
        if entry:
            time, value = entry
            if max_age and datetime.utcnow() <= time + max_age:
                return value
            evict_expired(instance)

    def set_cache_value(instance, id, value):
        if max_size and len(instance._roax_cache_) >= max_size:  # too many entries
            evict_expired(instance)  # first try to evict all expired entries
            if len(instance._roax_cache_) >= max_size:  # still too many entries
                evict_oldest(instance)  # evict the oldest entry
        instance._roax_cache_[id] = (datetime.utcnow(), value)

    def _invalidate(self, id=None):
        """
        Invalidate a single cache entry, or all entries.

        :param id: Identity of cache entry to invalidate, or `None` for all.
        """
        if id:
            self._roax_cache_.pop(id, None)
        else:
            self._roax_cache_.clear()

    @wrapt.decorator
    def read(wrapped, instance, args, kwargs):
        id = get_id_arg(wrapped, instance, args, kwargs)
        result = get_cache_value(instance, id)
        if not result:
            result = wrapped(*args, **kwargs)
            set_cache_value(instance, id, result)
        return result

    @wrapt.decorator
    def update(wrapped, instance, args, kwargs):
        instance._invalidate(get_id_arg(wrapped, instance, args, kwargs))
        return wrapped(*args, **kwargs)

    @wrapt.decorator
    def delete(wrapped, instance, args, kwargs):
        instance._invalidate(get_id_arg(wrapped, instance, args, kwargs))
        return wrapped(*args, **kwargs)

    @wrapt.decorator
    def __init__(wrapped, instance, args, kwargs):
        instance._roax_cache_ = {}
        return wrapped(*args, **kwargs)

    def decorate(cls, name, wrapper):
        method = getattr(cls, name, None)
        if method:
            setattr(cls, name, wrapper(method))

    def decorator(cls):
        cls._invalidate = _invalidate
        cls.__init__ = __init__(cls.__init__)
        decorate(cls, "read", read)
        decorate(cls, "update", update)
        decorate(cls, "delete", delete)
        return cls

    return decorator
