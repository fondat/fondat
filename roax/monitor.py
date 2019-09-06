"""
Module for monitoring measurements.

This module defines a `monitors` variable, which is an instance of the `Monitors`
class. Your application monitor(s) can be set in/deleted from this object.
"""

import collections
import datetime
import logging
import math
import re
import time


_logger = logging.getLogger(__name__)


_now = lambda: datetime.datetime.now(tz=datetime.timezone.utc)


class Counter:
    """
    A counter data point. A counter measurement is an integer value that should
    monotonicaly increase (unless being reset). The counter data point stores the
    highest counter value measured.

    The counter data point contains the following instance variables:
      • timestamp: The time of the data point, in seconds since Epoch.
      • value: The highest counter value measured.
    """

    name = "counter"

    def __init__(self, timestamp):
        super().__init__()
        self.timestamp = timestamp
        self.value = 0

    def record(self, value):
        self.value = max(self.value, value)


class Gauge:
    """
    A gauge data point. A gauge measurement is an integer or floating point
    value. The gauge data point stores the minimum, maximum, sum and count of
    measured values.

    The gauge data point contains the following instance variables:
      • timestamp: The date and time of the data point.
      • min: The minimum measured value.
      • max: The maximum measured value.
      • sum: The sum of all measured values
      • count: The count of measured values.
    """

    name = "gauge"

    def __init__(self, timestamp):
        super().__init__()
        self.timestamp = timestamp
        self.min = None
        self.max = None
        self.count = 0
        self.sum = 0

    def record(self, value):
        if self.min is None or value < self.min:
            self.min = value
        if self.max is None or value > self.max:
            self.max = value
        self.count += 1
        self.sum += value


class Absolute:
    """
    An absolute data point. An absolute measurement is an integer value. The
    absolute data point stores the sum of measured values.

    The absolute data point contains the following instance variables:
      • timestamp: The date and time of the data point.
      • value: The sum of measured values.    
    """

    name = "absolute"

    def __init__(self, timestamp):
        super().__init__()
        self.timestamp = timestamp
        self.value = 0

    def record(self, value):
        self.value += value


_types = {Counter.name: Counter, Gauge.name: Gauge, Absolute.name: Absolute}


class Series:
    """
    TODO: Description.

    The series contains the following instance variables:
      • type: The type of the data point being tracked.
      • patterns: Dictionary of tag names to regular expression patterns to match.
      • points: Number of data points to maintain in the time series.
      • interval: Interval between data points, in seconds.
      • data: A deque of timestamp-ordered data points.
    """

    def __init__(self, type, patterns, points, interval):
        """
        The `patterns` argument is a dictionary that maps tag names to regular
        expressions (compiled or strings) to match against recorded measurement tags.
        For example, `{"name": "foo"}` would track data where a tag includes
        `{"name": "foo"}`, while `{"name": "foo\\..+"}` would track measurements with
        tags that include `{"name": "foo.bar"}` and `{"name": "foo.qux"}`.
        """
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
        ts = math.ceil(timestamp.timestamp())  # truncate milliseconds
        ts -= ts % self.interval  # round to beginning of interval
        return datetime.datetime.fromtimestamp(ts, tz=datetime.timezone.utc)

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

    def record(self, tags, timestamp, type, value):
        if not self._tags_match(tags):
            return  # ignore submission
        if type != self.type:
            raise ValueError(f"expecting data point type of: {self.type}")
        if timestamp > _now():
            raise ValueError("cannot record measurement in the future")
        self._get_data_point(timestamp).record(value)


class SimpleMonitor:
    """
    A simple memory round-robin monitor, capable of maintaining multiple time
    series. This resource is appropriate for collecting thousands of data points;
    beyond that it’s advisable to use a time series database.

    In this monitor, a time series is a set of data points and time intervals of
    fixed duration. A data point records data measured at that exact point in time
    and the subsequent interval.
    
    This monitor handles the following types of recorded measurements in data
    points: "counter", "gauge" and "absolute". For more information on these
    types, see their class documentation. 

    If no measurement is recorded for a given data point, the data point will not
    be stored in the time series. Consumers of the time series should perform
    interpolation if required (e.g. for graphic representation).

    The simple monitor contains a `series` instance variable, which is a
    dictionary mapping time series names to associated `Series` objects.
    """

    def __init__(self):
        """Initialize the simple monitoring resource."""
        super().__init__()
        self.series = {}

    def track(self, name, type, patterns, points, interval):
        """
        Track data points for a specfied set of tags in a new time series.

        :param name: Name of the new time series.
        :param type: Type of data point to track.  {"counter", "gauge", "absolute"}
        :param patterns: Measurements with tags matching regular expressions are tracked.
        :param points: Number of data points to maintain in the time series.
        :param interval: Interval between data points, in seconds.

        For `patterns` argument, see the `Series` class initializer documentation.
       """
        if name in self.series:
            raise ValueError(f"time series already exists: {name}")
        if type not in _types:
            raise ValueError(f"unsupported data point type: {type}")
        self.series[name] = Series(type, patterns, points, interval)

    def record(self, tags, timestamp, type, value):
        """
        Record a measurement.

        :param tags: Tags associated with the measurement.
        :param timestamp: Date and time of the measurement to record.
        :param type: Type of measurement to record.  {"counter", "gauge", "absolute"}
        :param value: Value of measurement to record.

        The `tags` argument is a dictionary that maps string key to string value. At
        least one tag should have a key of `name`.
        """
        for series in self.series.values():
            series.record(tags, timestamp, type, value)


class QueueMonitor:
    """
    A monitor that queues all recorded measurements in a `deque` object.

    The queue size can be specified; if reached, oldest measurements will be
    truncated.

    Each measurements is stored in this form.
    `{"tags": dict, "timestamp": datetime, "type": str, "value": object}`.
    """

    def __init__(self, size=None, deque=None):
        """
        Initialize the queue monitor.

        :param size: Maximum number of recorded measurements to queue.  [None]
        :param deque: Deque to store measurements in.  [new deque]
        """
        super().__init__()
        self.deque = deque or collections.deque()
        self.size = size

    def record(self, tags, timestamp, type, value):
        """
        Record a measurement.

        :param tags: Tags associated with the measurement.
        :param timestamp: Date and time of the measurement to record.
        :param type: Type of measurement to record.  {"counter", "gauge", "absolute"}
        :param value: Value of measurement to record.

        The `tags` argument is a dictionary that maps string key to string value. At
        least one tag should have a key of `name`.
        """
        if self.size and len(self.deque) >= self.size:
            _logger.warning(
                f"QueueMonitor reached maximum size of {self.size}; truncating"
            )
            while len(self.deque) >= self.size:
                self.deque.popleft()
        self.deque.append(dict(tags=tags, timestamp=timestamp, type=type, value=value))

    def pop(self):
        """
        Remove and return all recorded measurements from the deque as a list.
        """
        result = []
        while True:
            try:
                result.append(self.deque.popleft())
            except IndexError:
                break
        return result


class Monitors(dict):
    """
    A monitor that is itself a dict of keys-to-monitors. A call to the `record`
    method in this class records the measurement in all of its monitors. The
    key to associate with a monitor is at the discretion of its creator.
    """

    def record(self, tags, timestamp, type, value):
        """
        Record a measurement.

        :param tags: Tags associated with the measurement.
        :param timestamp: Date and time of the measurement to record.
        :param type: Type of measurement to record.  {"counter", "gauge", "absolute"}
        :param value: Value of measurement to record.

        The `tags` argument is a dictionary that maps string key to string value. At
        least one tag should have a key of `name`.
        """
        exception = None
        for monitor in self.values():
            try:
                monitor.record(tags, timestamp, type, value)
            except Exception as e:
                if not exception:
                    exception = e
        if exception:
            raise e


class timer:
    """
    A context manager that times statement(s) and records the duration measurement
    as a gauge in the monitor.
    """

    def __init__(self, tags, monitor=None):
        """
        Initialize the timer.

        :param tags: Tags to record upon completion of the timer.
        :param monitor: Monitor to record measurement in.  [monitor]
        """
        self.tags = tags
        self.monitor = monitor or monitors

    def __enter__(self):
        self.begin = time.time()
        return self

    def __exit__(self, *args):
        duration = time.time() - self.begin
        try:
            self.monitor.record(self.tags, _now(), "gauge", duration)
        except:
            _logger.warning("Exception recording measurement", exc_info=True)


monitors = Monitors()
