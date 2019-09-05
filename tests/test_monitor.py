import pytest
import roax.monitor
import roax.schema as s


from datetime import datetime, timedelta, timezone


_tags = {"name": "test"}

_dt = lambda string: s.datetime().str_decode(string)

_now = lambda: datetime.now(tz=timezone.utc)


def test_simple_counter_type():
    simple = roax.monitor.SimpleMonitor()
    _type = "counter"
    simple.track("test", _type, _tags, 60, 60)
    simple.record(_tags, _dt("2018-12-01T00:00:00Z"), _type, 1)
    simple.record(_tags, _dt("2018-12-01T00:00:10.1Z"), _type, 2)
    simple.record(_tags, _dt("2018-12-01T00:00:20.2Z"), _type, 3)
    simple.record(_tags, _dt("2018-12-01T00:00:30.3Z"), _type, 4)
    simple.record(_tags, _dt("2018-12-01T00:00:59.999Z"), _type, 5)
    simple.record(_tags, _dt("2018-12-01T00:01:01Z"), _type, 10)
    simple.record(_tags, _dt("2018-12-01T00:01:02Z"), _type, 20)
    simple.record(_tags, _dt("2018-12-01T00:01:03Z"), _type, 30)
    simple.record(_tags, _dt("2018-12-01T00:01:04Z"), _type, 40)
    simple.record(_tags, _dt("2018-12-01T00:01:05Z"), _type, 50)
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


def test_simple_gauge_type():
    simple = roax.monitor.SimpleMonitor()
    _type = "gauge"
    simple.track("test", _type, _tags, 60, 60)
    simple.record(_tags, _dt("2018-12-01T00:00:00Z"), _type, 1)
    simple.record(_tags, _dt("2018-12-01T00:00:01Z"), _type, 2)
    simple.record(_tags, _dt("2018-12-01T00:00:02Z"), _type, 3)
    simple.record(_tags, _dt("2018-12-01T00:00:03Z"), _type, 4)
    simple.record(_tags, _dt("2018-12-01T00:00:04Z"), _type, 5)
    simple.record(_tags, _dt("2018-12-01T00:01:55Z"), _type, 10)
    simple.record(_tags, _dt("2018-12-01T00:01:56Z"), _type, 20)
    simple.record(_tags, _dt("2018-12-01T00:01:57Z"), _type, 30)
    simple.record(_tags, _dt("2018-12-01T00:01:58Z"), _type, 40)
    simple.record(_tags, _dt("2018-12-01T00:01:59Z"), _type, 50)
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


def test_simple_absolute_type():
    simple = roax.monitor.SimpleMonitor()
    _type = "absolute"
    simple.track("test", _type, _tags, 60, 60)
    simple.record(_tags, _dt("2018-12-01T00:00:00Z"), _type, 1)
    simple.record(_tags, _dt("2018-12-01T00:00:01Z"), _type, 2)
    simple.record(_tags, _dt("2018-12-01T00:00:02Z"), _type, 3)
    simple.record(_tags, _dt("2018-12-01T00:00:03Z"), _type, 4)
    simple.record(_tags, _dt("2018-12-01T00:00:04Z"), _type, 5)
    simple.record(_tags, _dt("2018-12-01T00:01:55Z"), _type, 10)
    simple.record(_tags, _dt("2018-12-01T00:01:56Z"), _type, 20)
    simple.record(_tags, _dt("2018-12-01T00:01:57Z"), _type, 30)
    simple.record(_tags, _dt("2018-12-01T00:01:58Z"), _type, 40)
    simple.record(_tags, _dt("2018-12-01T00:01:59Z"), _type, 50)
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


def test_simple_truncation():
    simple = roax.monitor.SimpleMonitor()
    _type = "absolute"
    simple.track("test", _type, _tags, 3, 60)
    start = _dt("2018-12-01T00:00:00Z")
    simple.record(_tags, _dt("2018-12-01T00:00:00Z"), _type, 1)
    simple.record(_tags, _dt("2018-12-01T00:01:00Z"), _type, 2)
    simple.record(_tags, _dt("2018-12-01T00:02:00Z"), _type, 3)
    simple.record(_tags, _dt("2018-12-01T00:03:00Z"), _type, 4)
    data = simple.series["test"].data
    assert len(data) == 3
    assert data[0].timestamp == _dt("2018-12-01T00:01:00Z")


def test_simple_untracked_tag_ignore():
    simple = roax.monitor.SimpleMonitor()
    _type = "absolute"
    simple.track("test", _type, _tags, 60, 60)
    simple.record({"another": "tag"}, _dt("2018-12-01T00:00:00Z"), _type, 1)
    assert len(simple.series["test"].data) == 0


def test_simple_backdate_preserve_order():
    simple = roax.monitor.SimpleMonitor()
    _type = "absolute"
    simple.track("test", _type, _tags, 60, 60)
    simple.record(_tags, _dt("2018-12-01T00:01:00Z"), _type, 1)
    simple.record(_tags, _dt("2018-12-01T00:00:00Z"), _type, 2)
    series = simple.series["test"]
    assert series.type == _type
    data = series.data
    assert len(data) == 2
    assert data[0].timestamp == _dt("2018-12-01T00:00:00Z")


def test_simple_pattern_match():
    simple = roax.monitor.SimpleMonitor()
    _type = "absolute"
    simple.track("test", _type, {"name": "foo\\..+"}, 60, 60)
    simple.record({"name": "foo.bar"}, _dt("2018-12-01T00:01:00Z"), _type, 1)
    simple.record({"name": "foo"}, _dt("2018-12-01T00:02:00Z"), _type, 1)
    simple.record({"name": "foo."}, _dt("2018-12-01T00:03:00Z"), _type, 1)
    simple.record({"name": "qux.bar"}, _dt("2018-12-01T00:04:00Z"), _type, 2)
    data = simple.series["test"].data
    assert len(data) == 1


def test_simple_type_mismatch_error():
    simple = roax.monitor.SimpleMonitor()
    simple.track("test", "absolute", _tags, 60, 60)
    with pytest.raises(ValueError):
        simple.record(_tags, _dt("2018-12-01T00:01:00Z"), "gauge", 1)


def test_simple_type_future_error():
    simple = roax.monitor.SimpleMonitor()
    simple.track("test", "absolute", _tags, 60, 60)
    with pytest.raises(ValueError):
        simple.record(_tags, _now() + timedelta(seconds=1), "absolute", 1)


def test_simple_duplicate_series_error():
    simple = roax.monitor.SimpleMonitor()
    simple.track("test", "absolute", _tags, 60, 60)
    with pytest.raises(ValueError):
        simple.track("test", "absolute", _tags, 60, 60)


def test_simple_invalid_data_series_type():
    simple = roax.monitor.SimpleMonitor()
    with pytest.raises(ValueError):
        simple.track("test", "foobar", _tags, 60, 60)


def test_queue_pop():
    queue = roax.monitor.QueueMonitor()
    queue.record(_tags, _dt("2018-12-01T00:00:00Z"), "counter", 1)
    queue.record(_tags, _dt("2018-12-01T00:01:01Z"), "counter", 10)
    assert queue.pop() == [
        dict(
            tags=_tags, timestamp=_dt("2018-12-01T00:00:00Z"), type="counter", value=1
        ),
        dict(
            tags=_tags, timestamp=_dt("2018-12-01T00:01:01Z"), type="counter", value=10
        ),
    ]
    assert len(queue.pop()) == 0


def test_queue_truncate():
    queue = roax.monitor.QueueMonitor(size=2)
    queue.record(
        _tags, _dt("2018-12-01T00:00:00Z"), "absolute", 1
    )  # should be truncated
    queue.record(_tags, _dt("2018-12-01T00:00:01Z"), "absolute", 2)
    queue.record(_tags, _dt("2018-12-01T00:00:02Z"), "absolute", 3)
    assert queue.pop() == [
        dict(
            tags=_tags, timestamp=_dt("2018-12-01T00:00:01Z"), type="absolute", value=2
        ),
        dict(
            tags=_tags, timestamp=_dt("2018-12-01T00:00:02Z"), type="absolute", value=3
        ),
    ]


def test_monitors():
    monitors = roax.monitor.Monitors()
    q1 = roax.monitor.QueueMonitor()
    q2 = roax.monitor.QueueMonitor()
    monitors.append(q1)
    monitors.append(q2)
    monitors.record(_tags, _dt("2018-12-01T00:00:01Z"), "absolute", 1)
    assert len(q1.pop()) == 1
    assert len(q2.pop()) == 1
