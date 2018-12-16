import roax.monitor as m
import roax.schema as s
import unittest

from datetime import datetime, timedelta, timezone


_tags = {"name": "test"}

_dt = lambda string: s.datetime().str_decode(string)

_now = lambda: datetime.now(tz=timezone.utc)


class TestMonitor(unittest.TestCase):

    def setUp(self):
        m.monitor = m.SimpleMonitor()

    def test_counter_type(self):
        _type = "counter"
        m.monitor.track("test", _type, _tags, 60, 60)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:00Z"), _type, 1)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:10.1Z"), _type, 2)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:20.2Z"), _type, 3)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:30.3Z"), _type, 4)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:59.999Z"), _type, 5)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:01Z"), _type, 10)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:02Z"), _type, 20)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:03Z"), _type, 30)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:04Z"), _type, 40)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:05Z"), _type, 50)
        series = m.monitor.series["test"]
        self.assertEqual(series.type, _type)
        data = series.data
        self.assertEqual(len(data), 2)
        dp = data[0]
        self.assertEqual(dp.timestamp, _dt("2018-12-01T00:00:00Z"))
        self.assertEqual(dp.value, 5)
        dp = data[1]
        self.assertEqual(dp.timestamp, _dt("2018-12-01T00:01:00Z"))
        self.assertEqual(dp.value, 50)

    def test_gauge_type(self):
        _type = "gauge"
        m.monitor.track("test", _type, _tags, 60, 60)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:00Z"), _type, 1)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:01Z"), _type, 2)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:02Z"), _type, 3)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:03Z"), _type, 4)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:04Z"), _type, 5)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:55Z"), _type, 10)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:56Z"), _type, 20)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:57Z"), _type, 30)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:58Z"), _type, 40)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:59Z"), _type, 50)
        series = m.monitor.series["test"]
        self.assertEqual(series.type, _type)
        data = series.data
        self.assertEqual(len(data), 2)
        dp = data[0]
        self.assertEqual(dp.timestamp, _dt("2018-12-01T00:00:00Z"))
        self.assertEqual(dp.min, 1)
        self.assertEqual(dp.max, 5)
        self.assertEqual(dp.count, 5)
        self.assertEqual(dp.sum, 15)
        dp = data[1]
        self.assertEqual(dp.timestamp, _dt("2018-12-01T00:01:00Z"))
        self.assertEqual(dp.min, 10)
        self.assertEqual(dp.max, 50)
        self.assertEqual(dp.count, 5)
        self.assertEqual(dp.sum, 150)

    def test_absolute_type(self):
        _type = "absolute"
        m.monitor.track("test", _type, _tags, 60, 60)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:00Z"), _type, 1)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:01Z"), _type, 2)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:02Z"), _type, 3)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:03Z"), _type, 4)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:04Z"), _type, 5)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:55Z"), _type, 10)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:56Z"), _type, 20)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:57Z"), _type, 30)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:58Z"), _type, 40)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:59Z"), _type, 50)
        series = m.monitor.series["test"]
        self.assertEqual(series.type, _type)
        data = series.data
        self.assertEqual(len(data), 2)
        dp = data[0]
        self.assertEqual(dp.timestamp, _dt("2018-12-01T00:00:00Z"))
        self.assertEqual(dp.value, 15)
        dp = data[1]
        self.assertEqual(dp.timestamp, _dt("2018-12-01T00:01:00Z"))
        self.assertEqual(dp.value, 150)

    def test_truncation(self):
        _type = "absolute"
        m.monitor.track("test", _type, _tags, 3, 60)
        start = _dt("2018-12-01T00:00:00Z")
        m.monitor.record(_tags, _dt("2018-12-01T00:00:00Z"), _type, 1)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:00Z"), _type, 2)
        m.monitor.record(_tags, _dt("2018-12-01T00:02:00Z"), _type, 3)
        m.monitor.record(_tags, _dt("2018-12-01T00:03:00Z"), _type, 4)
        data = m.monitor.series["test"].data
        self.assertEqual(len(data), 3)
        self.assertEqual(data[0].timestamp, _dt("2018-12-01T00:01:00Z"))

    def test_untracked_tag_ignore(self):
        _type = "absolute"
        m.monitor.track("test", _type, _tags, 60, 60)
        m.monitor.record({"another": "tag"}, _dt("2018-12-01T00:00:00Z"), _type, 1)
        self.assertEqual(len(m.monitor.series["test"].data), 0)

    def test_backdate_preserve_order(self):
        _type = "absolute"
        m.monitor.track("test", _type, _tags, 60, 60)
        m.monitor.record(_tags, _dt("2018-12-01T00:01:00Z"), _type, 1)
        m.monitor.record(_tags, _dt("2018-12-01T00:00:00Z"), _type, 2)
        series = m.monitor.series["test"]
        self.assertEqual(series.type, _type)
        data = series.data
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0].timestamp, _dt("2018-12-01T00:00:00Z"))

    def test_pattern_match(self):
        _type = "absolute"
        m.monitor.track("test", _type, {"name": "foo\\..+"}, 60, 60)
        m.monitor.record({"name": "foo.bar"}, _dt("2018-12-01T00:01:00Z"), _type, 1)
        m.monitor.record({"name": "foo"}, _dt("2018-12-01T00:02:00Z"), _type, 1)
        m.monitor.record({"name": "foo."}, _dt("2018-12-01T00:03:00Z"), _type, 1)
        m.monitor.record({"name": "qux.bar"}, _dt("2018-12-01T00:04:00Z"), _type, 2)
        data = m.monitor.series["test"].data
        self.assertEqual(len(data), 1)

    def test_type_mismatch_error(self):
        m.monitor.track("test", "absolute", _tags, 60, 60)
        with self.assertRaises(ValueError):
            m.monitor.record(_tags, _dt("2018-12-01T00:01:00Z"), "gauge", 1)
        
    def test_type_future_error(self):
        m.monitor.track("test", "absolute", _tags, 60, 60)
        with self.assertRaises(ValueError):
            m.monitor.record(_tags, _now() + timedelta(seconds=1), "absolute", 1)

    def test_duplicate_series_error(self):
        m.monitor.track("test", "absolute", _tags, 60, 60)
        with self.assertRaises(ValueError):
            m.monitor.track("test", "absolute", _tags, 60, 60)

    def test_invalid_data_series_type(self):
        with self.assertRaises(ValueError):
            m.monitor.track("test", "foobar", _tags, 60, 60)


if __name__ == "__main__":
    unittest.main()
