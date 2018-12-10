"""TBD."""

# Copyright © 2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this

import math
import re
import roax.schema as s
import time

from collections import deque
from datetime import datetime, timezone
from roax.resource import BadRequest, Conflict, Resource, operation


class _DataPoint:
    """TODO: Description."""

    def __init__(self, timestamp):
        self.timestamp = timestamp


class _Counter(_DataPoint):
    """TODO: Description."""

    def __init__(self, timestamp):
        super().__init__(timestamp)
        self.counter = 0

    def process(self, value):
        self.counter = max(self.counter, value)


class _Gauge(_DataPoint):
    """TODO: Description."""

    def __init__(self, timestamp):
        super().__init__(timestamp)
        self.min = None
        self.max = None
        self.count = 0
        self.sum = 0

    def process(self, value):
        if self.min is None or value < self.min:
            self.min = value
        if self.max is None or value > self.max:
            self.max = value
        self.count += 1
        self.sum += value


class _Absolute(_DataPoint):
    """TODO: Description."""

    def __init__(self, timestamp):
        super().__init__(timestamp)
        self.accumulator = 0

    def process(self, value):
        self.accumulator += value


_datum_data_types = {
    "counter": _Counter,
    "gauge": _Gauge,
    "absolute": _Absolute,
}


_datum_type_schema = s.str(
    enum = set(_datum_data_types),
    description = "Datum type.",
)


_datum_value_schema = s.any_of(
    schemas = (s.int(), s.float()),
    description = "Datum value.",
)


class _Series:

    def __init__(self, type, patterns, points, interval):
        self.type = type
        self.patterns = [re.compile(pattern) for pattern in patterns]
        self.points = points
        self.interval = interval
        self.data = deque()

    def _tags_match(self, tags):
        """True if all tag patterns match submitted data point tags."""
        for pattern in self.patterns:
            if not sum([1 for tag in tags if pattern.fullmatch(tag)]):
                return False
        return True

    def _get_datum(self, timestamp):
        timestamp = math.ceil(timestamp)
        modulus = timestamp % self.interval
        timestamp += self.interval - modulus if modulus else 0
        index = 0
        for index in range(0, len(self.data)):
            if self.data[index].timestamp == timestamp:
                return self.data[index]
            if self.data[index].timestamp > timestamp:
                break
        if index < len(self.data) and self.data[index].timestamp < timestamp:
            index += 1
        datum = _datum_data_types[self.type](timestamp)
        self.data.insert(index, datum)
        while len(self.data) > self.points:
            self.data.popleft()
        return datum

    def submit(self, tags, timestamp, type, value):
        if not self._tags_match(tags):
            return  # ignore submission
        if type != self.type:
            raise BadRequest("expecting data point type of {}}".format(self.type))
        if timestamp > time.time():
            raise BadRequest("cannot submit datum with timestamp in the future")
        self._get_datum(timestamp).submit(value)

    def query(self, start, end):


        #datetime.fromtimestamp(self.timestamp, tz=timezone.utc)
        pass

class SimpleMonitorResource(Resource):
    """
    A simple memory round-robin monitoring resource, capable of maintaining
    multiple time series. This resource is appropriate for collecting hundreds or
    possibly thousands of data points; beyond that it's advisable to use a
    dedicated time series database.

    In this resource, a time series is composed of a set of data points and time
    intervals of fixed duration. A data point records data submitted at that exact
    point in time and the interval leading up to it.
    
    This resource handles the following types of submitted data: "counter",
    "gauge" and "absolute".

    A counter datum is an integer value that monotonicaly increases. On query,
    multiple counter data points yield rate over time. If a data point has no
    previous data point, or has a counter value less than the previous data point
    (e.g. rolled over), the derived rate over time at that data point will be
    unknown (`None`).

    A gauge datum is a measured integer or floating point value. On query a gauge
    data point yields the minimum, maximum and mean (average) measured values for
    the time interval leading up to and including the data point timestamp.

    An absolute datum is an integer value. Like counters, on query, multiple
    absolute data points yield rate over time. Unlike counters, absolute data
    points can be negative, yielding negative rate over time. If multiple absolute
    data are submitted in the same interval, they are accumulated.

    If no data is recorded for a given data point, the data point will not be
    stored in the time series. On query, rates for counter and absolute data will
    be correctly extrapolated by comparing the times of two adjacent data points;
    the consumer of the time series must perform any required interpolation
    (e.g. for graphic representation).
    """

    def __init__(self):
        """Initialize the simple monitoring resource."""
        super().__init__()
        self.series = {}

    # ----- track -----
    @operation(
        type = "action",
        params = {
            "name": s.str(description="Name of the new time series."),
            "type": _datum_type_schema,
            "tags": s.set(items=s.str(), description="Tags to match; data with matching tags are tracked in the time series."),
            "points": s.int(minimum=1, description="Maximum number of data points to maintain in the time series."),
            "interval": s.int(minimum=1, description="Duration of interval between data points, in seconds."),
        }
    )
    def track(self, name, type, patterns, points, interval):
        """
        Track data points for a specfied set of tags in a new time series.

        Patterns are a set of regular expression strings to match against submitted
        data tags. For example, a pattern of `"name=foo"` would track data where a tag
        is literally `"name=foo"`, while `"name=foo.\\.+"` would track data with tags
        that begin with `"name=foo."`, for example: `"name=foo.bar"` and
        `"name=foo.qux"`.
       """
        if name in self.series:
            raise Conflict("time series already exists: {}".format(name))
        self.series[name] = _Series(type, patterns, points, interval)

    # ----- submit -----
    @operation(
        type = "action",
        params = {
            "tags": s.set(items=s.str()),
            "timestamp": s.datetime(),
            "type": _datum_type_schema,
            "value": _datum_value_schema,
        }
    )
    def submit(self, tags, timestamp, type, value):
        """
        Submit datum to be monitored.

        Tags are lists of strings. By convention, each string should contain a key
        and value separated by an equal sign. At least one tag should have a key of
        `name`. For example, `"name=foo"`.
        """
        for series in self.series.values():
            series.submit(tags, timestamp.timestamp(), type, value) 

    # ----- query -----
    def query(self, name, start=None, end=None):
        """
        TBD
        """
        pass


monitor = SimpleMonitorResource()
