"""
Fondat monitor module.

A global "monitors" object is a list of monitors and a monitor itself. Applications are free
to add/remove their own monitors to/from this object.
"""

import asyncio
import time

from contextlib import asynccontextmanager
from dataclasses import field
from datetime import datetime, timezone
from fondat.data import datacls
from fondat.validation import MinLen, validate
from types import NoneType
from typing import Annotated, Literal


# type aliases
Name = Annotated[str, MinLen(1)]
Type = Literal["counter", "gauge"]
Tags = dict[str, str]
Value = int | float


_now = lambda: datetime.now(tz=timezone.utc)


@datacls
class Measurement:
    """
    An individual measurement.

    Parameters and attributes:
    • name: name of the measurement
    • tags: key-value pairs that qualify the measurement
    • timestamp: date and time of the measurement  [now]
    • type: type of measurement
    • value: measured value
    • unit: unit of measure

    Name should be an identifier, expressed in snake_case, which describes the metric being
    measured (e.g. "operation_invocations", "operation_duration").

    Unit should be a standard symbol (e.g. SI symbol "s" for seconds). If unit is a rate,
    symbols separated by a slash "/" character should be used (e.g. "m/s" for metres per
    second).
    """

    name: Name
    tags: Tags | None
    timestamp: datetime = field(default_factory=_now)
    type: Type
    value: Value
    unit: str | None

    def __post_init__(self):
        validate(self, Measurement)


class Monitor:
    """Base class for a monitor that records measurements."""

    async def record(self, measurement: Measurement) -> NoneType:
        """Record a measurement."""
        raise NotImplementedError

    async def flush(self) -> NoneType:
        """Flush all cached measurements. Base class implementation does nothing."""
        return


class Monitors(Monitor, list[Monitor]):
    """A list of monitors, to which all measurements are recorded."""

    async def record(self, measurement: Measurement):
        """Record a measurement in monitors."""
        await asyncio.gather(monitor.record(measurement) for monitor in self)

    async def flush(self):
        """Flush all cached measurements."""
        await asyncio.gather(monitor.flush() for monitor in self)


monitors = Monitors()


@asynccontextmanager
async def timer(
    *,
    name: Name,
    tags: Tags | None = None,
    monitor: Monitor | None = None,
):
    """
    An asynchronous context manager that times the execution of work and records it as a
    gauge measurement of duration in seconds.

    Parameters:
    • name: measurement name
    • tags: key-value pairs that qualify the measurement
    • monitor: monitor to record measurement  [global monitors]

    If an exception is raised during execution, the measurement will not be recorded.
    """
    begin = time.perf_counter()
    yield
    duration = time.perf_counter() - begin
    await record(
        Measurement(name=name, type="gauge", value=duration, unit="s", tags=tags),
        monitor,
    )


@asynccontextmanager
async def counter(
    *,
    name: Name,
    tags: Tags | None = None,
    monitor: Monitor | None = None,
    status: str | None = None,
):
    """
    An asynchronous context manager that counts the number of executions, and optional status.

    Parameters:
    • name: measurement name
    • tags: key-value pairs that qualify the measurement
    • monitor: monitor to record measurement  [global monitors]
    • status: tag to record the status of execution

    If recording status, the value "success" will be added as a tag if execution was
    successful, or "failure" if an exception was raised during execution.
    """
    exception = None
    try:
        yield
    except Exception as e:
        exception = e
    if status:
        tags = {**(tags or {}), "status": "success" if not exception else "failure"}
    await record(
        Measurement(name=name, type="counter", value=1, tags=tags),
        monitor,
    )
    if exception:
        raise exception


async def record(measurement: Measurement, monitor: Monitor | None):
    """
    Record a measurement.

    Parameters:
    • measurement: measurement to record
    • monitor: monitor to record measurement  [global monitors]
    """
    await (monitor if monitor is not None else monitors).record(measurement)
