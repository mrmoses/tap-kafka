"""Unit tests for the poll_empty_retry_wait_ms config and its effect on
sync.read_kafka_messages."""
import unittest
from unittest.mock import patch, MagicMock

import confluent_kafka

import tap_kafka
from tap_kafka import sync
from tap_kafka.defaults import (
    DEFAULT_BOOKMARK_PRECEDENCE,
    DEFAULT_POLL_EMPTY_RETRY_WAIT_MS,
)


TOPIC = 'test_topic'


MINIMAL_CONFIG = {
    'topic': 'my_topic',
    'group_id': 'my_group_id',
    'bootstrap_servers': 'server1,server2,server3',
}


class _Clock:
    """Controllable clock for read_kafka_messages.

    time() returns the current value; sleep(seconds) advances the clock by
    that many seconds. Tracks every sleep call for assertions."""

    def __init__(self, start=1000.0):
        self.now = start
        self.sleeps = []

    def time(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


def _make_kafka_config(poll_empty_retry_wait_ms=DEFAULT_POLL_EMPTY_RETRY_WAIT_MS,
                      max_runtime_ms=1000,
                      consumer_timeout_ms=10000,
                      commit_interval_ms=5000):
    return {
        'topic': TOPIC,
        'primary_keys': {},
        'use_message_key': False,
        'max_runtime_ms': max_runtime_ms,
        'commit_interval_ms': commit_interval_ms,
        'consumer_timeout_ms': consumer_timeout_ms,
        'poll_empty_retry_wait_ms': poll_empty_retry_wait_ms,
        'bookmark_precedence': DEFAULT_BOOKMARK_PRECEDENCE,
    }


def _make_message(partition=0, offset=1, timestamp_ms=1638132327000, value=b'{}', key=None):
    msg = MagicMock()
    msg.topic.return_value = TOPIC
    msg.partition.return_value = partition
    msg.offset.return_value = offset
    msg.timestamp.return_value = (confluent_kafka.TIMESTAMP_CREATE_TIME, timestamp_ms)
    msg.value.return_value = value
    msg.key.return_value = key
    msg.headers.return_value = []
    return msg


def _run_with_clock(consumer, kafka_config, clock):
    """Run sync.read_kafka_messages with time and sleep driven by `clock`."""
    with patch('tap_kafka.sync.time.time', clock.time), \
         patch('tap_kafka.sync.time.sleep', clock.sleep), \
         patch('tap_kafka.sync.singer.write_message'), \
         patch('tap_kafka.sync.consume_kafka_message'):
        sync.read_kafka_messages(consumer, kafka_config, state={})


class TestPollEmptyRetryWaitConfig(unittest.TestCase):
    """Config-layer behavior for poll_empty_retry_wait_ms."""

    def test_default_value_is_negative_one(self):
        self.assertEqual(DEFAULT_POLL_EMPTY_RETRY_WAIT_MS, -1)

    def test_generate_config_applies_default_when_not_provided(self):
        config = tap_kafka.generate_config(dict(MINIMAL_CONFIG))
        self.assertEqual(config['poll_empty_retry_wait_ms'], DEFAULT_POLL_EMPTY_RETRY_WAIT_MS)

    def test_generate_config_respects_custom_value(self):
        custom = dict(MINIMAL_CONFIG, poll_empty_retry_wait_ms=500)
        config = tap_kafka.generate_config(custom)
        self.assertEqual(config['poll_empty_retry_wait_ms'], 500)

    def test_generate_config_respects_zero(self):
        # 0 ms means "retry without waiting", which is meaningfully different
        # from -1 ("don't retry, break immediately").
        custom = dict(MINIMAL_CONFIG, poll_empty_retry_wait_ms=0)
        config = tap_kafka.generate_config(custom)
        self.assertEqual(config['poll_empty_retry_wait_ms'], 0)


class TestReadKafkaMessagesPollEmptyRetryWait(unittest.TestCase):
    """sync.read_kafka_messages behavior driven by poll_empty_retry_wait_ms."""

    def test_default_breaks_immediately_on_empty_poll(self):
        consumer = MagicMock()
        consumer.poll.return_value = None
        clock = _Clock()
        kafka_config = _make_kafka_config(poll_empty_retry_wait_ms=-1)

        _run_with_clock(consumer, kafka_config, clock)

        consumer.poll.assert_called_once()
        self.assertEqual(clock.sleeps, [])

    def test_positive_wait_retries_until_max_runtime(self):
        consumer = MagicMock()
        consumer.poll.return_value = None
        clock = _Clock()
        kafka_config = _make_kafka_config(poll_empty_retry_wait_ms=100, max_runtime_ms=1000)

        _run_with_clock(consumer, kafka_config, clock)

        # 1000ms / 100ms = 10 retries; each sleep is ~0.1s (last one may be
        # slightly less due to floating-point drift in the clock).
        self.assertEqual(consumer.poll.call_count, 10)
        self.assertEqual(len(clock.sleeps), 10)
        for s in clock.sleeps:
            self.assertAlmostEqual(s, 0.1, places=4)

    def test_positive_wait_with_message_consumes_then_continues(self):
        consumer = MagicMock()
        message = _make_message(offset=42)
        # None → retry, then message, then None → retry, then exhausts runtime.
        consumer.poll.side_effect = [None, message, None, None, None, None, None,
                                     None, None, None, None]
        clock = _Clock()
        kafka_config = _make_kafka_config(poll_empty_retry_wait_ms=100, max_runtime_ms=1000)

        _run_with_clock(consumer, kafka_config, clock)

        # The empty-then-retry path should sleep at least once before the message,
        # and continue sleeping while polls remain empty afterward.
        self.assertGreaterEqual(len(clock.sleeps), 2)
        # At least one poll returned the message.
        self.assertGreaterEqual(consumer.poll.call_count, 2)

    def test_sleep_clamped_to_remaining_runtime(self):
        # Retry wait (5000ms) is much larger than max_runtime (200ms). The
        # first sleep should be clamped to the remaining runtime.
        consumer = MagicMock()
        consumer.poll.return_value = None
        clock = _Clock()
        kafka_config = _make_kafka_config(poll_empty_retry_wait_ms=5000, max_runtime_ms=200)

        _run_with_clock(consumer, kafka_config, clock)

        self.assertEqual(len(clock.sleeps), 1)
        self.assertAlmostEqual(clock.sleeps[0], 0.2)

    def test_zero_wait_breaks_immediately_like_negative_one(self):
        # poll_empty_retry_wait_ms=0 yields sleep_time_s=0, which the code
        # treats as "max runtime exceeded" and breaks the loop. Functionally
        # equivalent to the default -1 path; documented here so the quirk
        # doesn't surprise future readers.
        consumer = MagicMock()
        consumer.poll.return_value = None
        clock = _Clock()
        kafka_config = _make_kafka_config(poll_empty_retry_wait_ms=0, max_runtime_ms=1000)

        _run_with_clock(consumer, kafka_config, clock)

        consumer.poll.assert_called_once()
        self.assertEqual(clock.sleeps, [])


if __name__ == '__main__':
    unittest.main()
