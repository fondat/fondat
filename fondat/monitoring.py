"""
Module for monitoring measurements.

This module defines a "monitors" variable, which is an instance of the Monitors class. Your
application monitor(s) can be added to/deleted from this object.
"""

import collections
import fondat.validation
import logging
import re
import time

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Optional, Union


_logger = logging.getLogger(__name__)


_now = lambda: datetime.now(tz=timezone.utc)


@dataclass
class Measurement:
    """
    An individual measurement.

    Attributes:
    • tags: tags associated with the measurement; should contain a "name" key
    • timestamp: date and time of the measurement to record
    • type: type of measurement to record
    • value: value of measurement
    """

    tags: dict[str, str]
    timestamp: datetime
    type: Literal["counter", "gauge", "absolute"]
    value: Union[int, float]

    def __post_init__(self):
        fondat.validation.validate(self, self.__class__)


class Counter:
    """
    A counter data point. A counter measurement is an integer value that should monotonicaly
    increase (unless being reset). The counter data point stores the highest counter value
    measured.

    Parameter and attribute:
    • timestamp: time of the data point

    Attributes:
    • value: highest counter value measured
    """

    name = "counter"

    def __init__(self, timestamp: datetime):
        super().__init__()
        self.timestamp = timestamp
        self.value = 0

    def record(self, value: Union[int, float]):
        self.value = max(self.value, value)


class Gauge:
    """
    A gauge data point. A gauge measurement is an integer or floating point value. The gauge
    data point stores the minimum, maximum, sum and count of measured values.

    Parameter and attribute:
    • timestamp: date and time of the data point

    Attributes:
    • min: minimum measured value
    • max: maximum measured value
    • sum: sum of all measured values
    • count: count of measured values
    """

    name = "gauge"

    def __init__(self, timestamp: int):
        super().__init__()
        self.timestamp = timestamp
        self.min = None
        self.max = None
        self.count = 0
        self.sum = 0

    def record(self, value: Union[int, float]):
        if self.min is None or value < self.min:
            self.min = value
        if self.max is None or value > self.max:
            self.max = value
        self.count += 1
        self.sum += value


class Absolute:
    """
    An absolute data point. An absolute measurement is an integer value. The absolute data
    point stores the sum of measured values.

    Parameter and attribute:
    • timestamp: date and time of the data point

    Attributes:
    • value: sum of measured values
    """

    name = "absolute"

    def __init__(self, timestamp: datetime):
        super().__init__()
        self.timestamp = timestamp
        self.value = 0

    def record(self, value: Union[int, float]):
        self.value += value


_types = {t.name: t for t in {Counter, Gauge, Absolute}}


class Series:
    """
    Parameters and attributes:
    • type: type of the data point being tracked
    • patterns: dictionary of tag names to regular expression patterns to match
    • points: number of data points to maintain in the time series
    • interval: interval between data points, in seconds

    Attributes:
    • data: deque of timestamp-ordered data points

    The patterns parameter is a dictionary that maps tag names to regular expression
    strings to match against recorded measurement tags. For example, {"name": "foo"} would
    track data where a tag includes {"name": "foo"}, while {"name": "foo\\..+"} would track
    measurements with tags that include {"name": "foo.bar"} and {"name": "foo.qux"}.
    """

    def __init__(
        self,
        type: Literal["counter", "gauge", "absolute"],
        patterns: dict[str, str],
        points: int,
        interval: int,
    ):
        self.type = type
        self.patterns = {k: re.compile(v) for k, v in patterns.items()}
        self.points = points
        self.interval = interval
        self.data = collections.deque()

    def _tags_match(self, tags):
        for pk, pv in self.patterns.items():
            if not sum([1 for tk, tv in tags.items() if tk == pk and pv.fullmatch(tv)]):
                return False
        return True

    def _round_down(self, timestamp):
        ts = int(timestamp.timestamp())  # truncate milliseconds
        ts -= ts % self.interval  # round to beginning of interval
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    def _get_data_point(self, timestamp):
        timestamp = self._round_down(timestamp)
        index = 0
        for index in range(0, len(self.data)):
            if self.data[index].timestamp == timestamp:
                return self.data[index]
            if self.data[index].timestamp > timestamp:
                break
        if index < len(self.data) and self.data[index].timestamp < timestamp:
            index += 1
        result = _types[self.type](timestamp)
        self.data.insert(index, result)
        while len(self.data) > self.points:
            self.data.popleft()
        return result

    def record(self, measurement):
        """Record a measurement."""
        if not self._tags_match(measurement.tags):
            return  # ignore submission
        if measurement.type != self.type:
            raise ValueError(f"expecting data point type of: {self.type}")
        if measurement.timestamp > _now():
            raise ValueError("cannot record measurement in the future")
        self._get_data_point(measurement.timestamp).record(measurement.value)


class SimpleMonitor:
    """
    A simple in-memory round-robin monitor, capable of maintaining multiple time series. This
    class is appropriate for collecting thousands of data points (e.g. unit testing). Beyond
    that, it’s advisable to use an external, real time series database.

    In this monitor, a time series is a set of data points and time intervals of fixed
    duration. A data point records data measured at that exact point in time and the
    subsequent interval.

    This monitor handles the following types of recorded measurements in data points:
    "counter", "gauge" and "absolute". For more information on these types, see their class
    documentation.

    If no measurement is recorded for a given data point, the data point will not be stored in
    the time series. Consumers of the time series should perform interpolation if required
    (e.g. for graphic representation).

    The simple monitor contains a series attribute, which is a dictionary mapping time series
    names to associated Series objects.
    """

    def __init__(self):
        super().__init__()
        self.series = {}

    def track(
        self,
        name: str,
        type: Literal["counter", "gauge", "absolute"],
        patterns: dict[str, str],
        points: int,
        interval: int,
    ):
        """
        Track data points for a specfied set of tags in a new time series.

        Parameters:
        • name: name of the new time series
        • type: type of data point to track  {"counter", "gauge", "absolute"}
        • patterns: measurements with tags matching regular expressions are tracked
        • points: number of data points to maintain in the time series
        • interval: interval between data points, in seconds

        For patterns parameter, see the Series class initializer documentation.
        """
        if name in self.series:
            raise ValueError(f"time series already exists: {name}")
        if type not in _types:
            raise ValueError(f"unsupported data point type: {type}")
        self.series[name] = Series(type, patterns, points, interval)

    async def record(self, measurement: Measurement):
        """Record a measurement."""
        for series in self.series.values():
            series.record(measurement)


class DequeMonitor:
    """
        A monitor that stores all recorded measurements in a deque object.

        Parameters:
        • size: maximum number of recorded measurements to enqueue  [unlimited]
        • deque: deque to store measurements in  [new deque]
    dict[str, str]
        When a measurement is recorded, if the maximum queue size is reached, the oldest
        measurement is expunged.
    """

    def __init__(self, size: int = None, deque: collections.deque = None):
        super().__init__()
        self.deque = deque if deque is not None else collections.deque()
        self.size = size

    async def record(self, measurement: Measurement):
        """Record a measurement."""
        if self.size is not None:
            while len(self.deque) >= self.size:
                self.deque.popleft()
        self.deque.append(measurement)

    async def pop(self, monitor, cap: int = None):
        """
        Remove oldest measurements from the deque and record them into another monitor.

        Parameters:
        • monitor: monitor to record measurements into
        • cap: maximum number of measurements to pop from deque

        If no cap is specified, all queued items will be popped.
        """
        count = 0
        while (count < cap) if cap else True:
            try:
                measurement = self.deque.popleft()
            except IndexError:
                break
            await monitor.record(measurement)
            count += 1


class Monitors(dict):
    """
    A monitor that is a dict of key-monitor pairs. A call to the record method in this monitor
    records the measurement in all contained monitors. The key to associate with a monitor is
    at the discretion of its creator.
    """

    async def record(self, measurement: Measurement):
        """Record a measurement."""
        exception = None
        for monitor in self.values():
            try:
                await monitor.record(measurement)
            except Exception as e:
                if not exception:
                    exception = e
        if exception:
            raise e


class timer:
    """
    A context manager that times the execution of work and records its duration in seconds as
    a gauge measurement in the monitor.

    Parameters:
    • tags: tags to record upon completion of the timer
    • monitors: monitors to record measurement in  [global monitors]
    • status: name of tag to record status in measurement; None excludes status

    If no exception is encounted during execution, recotagsrded status is "success", otherwise
    "failure".
    """

    def __init__(
        self,
        tags: dict[str, str],
        *,
        monitors: Optional[Iterable[Any]] = None,
        status: str = "status",
    ):
        self.tags = tags
        self.monitors = monitors
        self.status = status

    async def __aenter__(self):
        self.begin = time.perf_counter()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        duration = time.perf_counter() - self.begin
        tags = {**self.tags}
        if self.status:
            tags[self.status] = "failure" if exc_type else "success"
        try:
            await record(Measurement(tags, _now(), "gauge", duration), self.monitors)
        except:
            _logger.warning("Exception recording measurement", exc_info=True)


class counter:
    """
    A context manager that records the excecution of work as a counter measurement in the
    monitor.

    Parameters:
    • tags: tags to record upon completion of the timer
    • monitors: monitors to record measurement in  [global monitors]
    • status: name of tag to record status in measurement; None excludes status

    If no exception is encounted during execution, recorded status is "success", otherwise
    "failure".
    """

    def __init__(
        self,
        tags: dict[str, str],
        *,
        monitors: Optional[Iterable[Any]] = None,
        status: str = "status",
    ):
        self.tags = tags
        self.monitors = monitors
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        tags = {**self.tags}
        if self.status:
            tags[self.status] = "failure" if exc_type else "success"
        try:
            await record(Measurement(tags, _now(), "counter", 1), self.monitors)
        except:
            _logger.warning("Exception recording measurement", exc_info=True)


monitors = []


async def record(measurement: Measurement, monitors: Optional[Iterable[Any]] = None):
    """
    Record a measurement.

    Parameters:
    • measurement: measurement to record
    • monitors: monitors to record measurement in  [global monitors]
    """
    for monitor in monitors or globals()["monitors"]:
        await monitor.record(measurement)
