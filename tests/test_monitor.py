import asyncio
import fondat.codec
import fondat.monitor
import pytest

from datetime import datetime
from fondat.monitor import Measurement, Monitor


class MyMonitor(Monitor):
    def __init__(self):
        self.measurements = []

    async def record(self, measurement: Measurement):
        self.measurements.append(measurement)


async def test_timer():
    monitor = MyMonitor()
    tags = {"foo": "bar"}
    async with fondat.monitor.timer(name="baz", tags=tags, monitor=monitor):
        await asyncio.sleep(0.1)
    assert len(monitor.measurements) == 1
    measurement = monitor.measurements[0]
    assert measurement.name == "baz"
    assert measurement.tags == tags
    assert isinstance(measurement.timestamp, datetime)
    assert measurement.type == "gauge"
    assert measurement.value > 0


async def test_counter_success():
    monitor = MyMonitor()
    tags = {"foo": "bar"}
    async with fondat.monitor.counter(name="baz", tags=tags, monitor=monitor, status="status"):
        pass
    assert len(monitor.measurements) == 1
    measurement = monitor.measurements[0]
    assert measurement.name == "baz"
    assert measurement.tags["status"] == "success"
    assert isinstance(measurement.timestamp, datetime)
    assert measurement.type == "counter"
    assert measurement.value == 1
    async with fondat.monitor.counter(name="baz", monitor=monitor, status="status"):
        pass
    assert len(monitor.measurements) == 2
    measurement = monitor.measurements[1]
    assert measurement.tags == {"status": "success"}


async def test_counter_failure():
    monitor = MyMonitor()
    tags = {"foo": "bar"}
    with pytest.raises(TypeError):
        async with fondat.monitor.counter(
            name="baz", tags=tags, monitor=monitor, status="status"
        ):
            raise TypeError
    assert len(monitor.measurements) == 1
    measurement = monitor.measurements[0]
    assert measurement.name == "baz"
    assert measurement.tags["status"] == "failure"
    assert isinstance(measurement.timestamp, datetime)
    assert measurement.type == "counter"
    assert measurement.value == 1
    with pytest.raises(TypeError):
        async with fondat.monitor.counter(name="baz", monitor=monitor, status="status"):
            raise TypeError
    assert len(monitor.measurements) == 2
    measurement = monitor.measurements[1]
    assert measurement.tags == {"status": "failure"}
