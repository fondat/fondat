import fondat.codec
import fondat.monitor
import pytest

from datetime import datetime, timedelta, timezone
from fondat.monitor import Measurement

_tags = {"name": "test"}

_dt = lambda s: fondat.codec.get_codec(datetime).str_decode(s)

_now = lambda: datetime.now(tz=timezone.utc)


@pytest.mark.asyncio
async def test_simple_counter_type():
    simple = fondat.monitor.SimpleMonitor()
    _type = "counter"
    simple.track("test", _type, _tags, 60, 60)
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:00:00Z"), _type, 1))
    await simple.record(
        Measurement(_tags, _dt("2018-12-01T00:00:10.100000Z"), _type, 2)
    )
    await simple.record(
        Measurement(_tags, _dt("2018-12-01T00:00:20.200000Z"), _type, 3)
    )
    await simple.record(
        Measurement(_tags, _dt("2018-12-01T00:00:30.300000Z"), _type, 4)
    )
    await simple.record(
        Measurement(_tags, _dt("2018-12-01T00:00:59.999999Z"), _type, 5)
    )
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:01Z"), _type, 10))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:02Z"), _type, 20))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:03Z"), _type, 30))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:04Z"), _type, 40))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:05Z"), _type, 50))
    series = simple.series["test"]
    assert series.type == _type
    data = series.data
    assert len(data) == 2
    dp = data[0]
    assert dp.timestamp == _dt("2018-12-01T00:00:00Z")
    assert dp.value == 5
    dp = data[1]
    assert dp.timestamp == _dt("2018-12-01T00:01:00Z")
    assert dp.value == 50


@pytest.mark.asyncio
async def test_simple_gauge_type():
    simple = fondat.monitor.SimpleMonitor()
    _type = "gauge"
    simple.track("test", _type, _tags, 60, 60)
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:00:00Z"), _type, 1))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:00:01Z"), _type, 2))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:00:02Z"), _type, 3))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:00:03Z"), _type, 4))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:00:04Z"), _type, 5))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:55Z"), _type, 10))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:56Z"), _type, 20))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:57Z"), _type, 30))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:58Z"), _type, 40))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:59Z"), _type, 50))
    series = simple.series["test"]
    assert series.type == _type
    data = series.data
    assert len(data) == 2
    dp = data[0]
    assert dp.timestamp == _dt("2018-12-01T00:00:00Z")
    assert dp.min == 1
    assert dp.max == 5
    assert dp.count == 5
    assert dp.sum == 15
    dp = data[1]
    assert dp.timestamp == _dt("2018-12-01T00:01:00Z")
    assert dp.min == 10
    assert dp.max == 50
    assert dp.count == 5
    assert dp.sum == 150


@pytest.mark.asyncio
async def test_simple_absolute_type():
    simple = fondat.monitor.SimpleMonitor()
    _type = "absolute"
    simple.track("test", _type, _tags, 60, 60)
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:00:00Z"), _type, 1))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:00:01Z"), _type, 2))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:00:02Z"), _type, 3))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:00:03Z"), _type, 4))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:00:04Z"), _type, 5))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:55Z"), _type, 10))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:56Z"), _type, 20))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:57Z"), _type, 30))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:58Z"), _type, 40))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:59Z"), _type, 50))
    series = simple.series["test"]
    assert series.type == _type
    data = series.data
    assert len(data) == 2
    dp = data[0]
    assert dp.timestamp == _dt("2018-12-01T00:00:00Z")
    assert dp.value == 15
    dp = data[1]
    assert dp.timestamp == _dt("2018-12-01T00:01:00Z")
    assert dp.value == 150


@pytest.mark.asyncio
async def test_simple_truncation():
    simple = fondat.monitor.SimpleMonitor()
    _type = "absolute"
    simple.track("test", _type, _tags, 3, 60)
    start = _dt("2018-12-01T00:00:00Z")
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:00:00Z"), _type, 1))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:00Z"), _type, 2))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:02:00Z"), _type, 3))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:03:00Z"), _type, 4))
    data = simple.series["test"].data
    assert len(data) == 3
    assert data[0].timestamp == _dt("2018-12-01T00:01:00Z")


@pytest.mark.asyncio
async def test_simple_untracked_tag_ignore():
    simple = fondat.monitor.SimpleMonitor()
    _type = "absolute"
    simple.track("test", _type, _tags, 60, 60)
    await simple.record(
        Measurement({"another": "tag"}, _dt("2018-12-01T00:00:00Z"), _type, 1)
    )
    assert len(simple.series["test"].data) == 0


@pytest.mark.asyncio
async def test_simple_backdate_preserve_order():
    simple = fondat.monitor.SimpleMonitor()
    _type = "absolute"
    simple.track("test", _type, _tags, 60, 60)
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:00Z"), _type, 1))
    await simple.record(Measurement(_tags, _dt("2018-12-01T00:00:00Z"), _type, 2))
    series = simple.series["test"]
    assert series.type == _type
    data = series.data
    assert len(data) == 2
    assert data[0].timestamp == _dt("2018-12-01T00:00:00Z")


@pytest.mark.asyncio
async def test_simple_pattern_match():
    simple = fondat.monitor.SimpleMonitor()
    _type = "absolute"
    simple.track("test", _type, {"name": "foo\\..+"}, 60, 60)
    await simple.record(
        Measurement({"name": "foo.bar"}, _dt("2018-12-01T00:01:00Z"), _type, 1)
    )
    await simple.record(
        Measurement({"name": "foo"}, _dt("2018-12-01T00:02:00Z"), _type, 1)
    )
    await simple.record(
        Measurement({"name": "foo."}, _dt("2018-12-01T00:03:00Z"), _type, 1)
    )
    await simple.record(
        Measurement({"name": "qux.bar"}, _dt("2018-12-01T00:04:00Z"), _type, 2)
    )
    data = simple.series["test"].data
    assert len(data) == 1


@pytest.mark.asyncio
async def test_simple_type_mismatch_error():
    simple = fondat.monitor.SimpleMonitor()
    simple.track("test", "absolute", _tags, 60, 60)
    with pytest.raises(ValueError):
        await simple.record(Measurement(_tags, _dt("2018-12-01T00:01:00Z"), "gauge", 1))


@pytest.mark.asyncio
async def test_simple_type_future_error():
    simple = fondat.monitor.SimpleMonitor()
    simple.track("test", "absolute", _tags, 60, 60)
    with pytest.raises(ValueError):
        await simple.record(
            Measurement(_tags, _now() + timedelta(seconds=1), "absolute", 1)
        )


def test_simple_duplicate_series_error():
    simple = fondat.monitor.SimpleMonitor()
    simple.track("test", "absolute", _tags, 60, 60)
    with pytest.raises(ValueError):
        simple.track("test", "absolute", _tags, 60, 60)


def test_simple_invalid_data_series_type():
    simple = fondat.monitor.SimpleMonitor()
    with pytest.raises(ValueError):
        simple.track("test", "foobar", _tags, 60, 60)


@pytest.mark.asyncio
async def test_deque_truncate():
    monitor = fondat.monitor.DequeMonitor(size=2)
    # should be truncated
    await monitor.record(Measurement(_tags, _dt("2018-12-01T00:00:00Z"), "absolute", 1))
    await monitor.record(Measurement(_tags, _dt("2018-12-01T00:00:01Z"), "absolute", 2))
    await monitor.record(Measurement(_tags, _dt("2018-12-01T00:00:02Z"), "absolute", 3))
    assert list(monitor.deque) == [
        Measurement(_tags, _dt("2018-12-01T00:00:01Z"), "absolute", 2),
        Measurement(_tags, _dt("2018-12-01T00:00:02Z"), "absolute", 3),
    ]


@pytest.mark.asyncio
async def test_deque_pop_all():
    m1 = fondat.monitor.DequeMonitor()
    m2 = fondat.monitor.DequeMonitor()
    await m1.record(Measurement(_tags, _dt("2018-12-01T00:00:01Z"), "absolute", 1))
    await m1.record(Measurement(_tags, _dt("2018-12-01T00:00:02Z"), "absolute", 2))
    await m1.pop(m2)
    assert list(m2.deque) == [
        Measurement(_tags, _dt("2018-12-01T00:00:01Z"), "absolute", 1),
        Measurement(_tags, _dt("2018-12-01T00:00:02Z"), "absolute", 2),
    ]


@pytest.mark.asyncio
async def test_deque_pop_cap():
    m1 = fondat.monitor.DequeMonitor()
    m2 = fondat.monitor.DequeMonitor()
    await m1.record(Measurement(_tags, _dt("2018-12-01T00:00:01Z"), "absolute", 1))
    await m1.record(Measurement(_tags, _dt("2018-12-01T00:00:02Z"), "absolute", 2))
    await m1.record(Measurement(_tags, _dt("2018-12-01T00:00:03Z"), "absolute", 3))
    await m1.record(Measurement(_tags, _dt("2018-12-01T00:00:04Z"), "absolute", 4))
    await m1.pop(m2, 2)
    assert list(m1.deque) == [
        Measurement(_tags, _dt("2018-12-01T00:00:03Z"), "absolute", 3),
        Measurement(_tags, _dt("2018-12-01T00:00:04Z"), "absolute", 4),
    ]
    assert list(m2.deque) == [
        Measurement(_tags, _dt("2018-12-01T00:00:01Z"), "absolute", 1),
        Measurement(_tags, _dt("2018-12-01T00:00:02Z"), "absolute", 2),
    ]


@pytest.mark.asyncio
async def test_monitors():
    monitors = fondat.monitor.Monitors()
    m1 = fondat.monitor.DequeMonitor()
    m2 = fondat.monitor.DequeMonitor()
    monitors["m1"] = m1
    monitors["m2"] = m2
    await monitors.record(
        Measurement(_tags, _dt("2018-12-01T00:00:01Z"), "absolute", 1)
    )
    assert len(m1.deque) == 1
    assert len(m2.deque) == 1
