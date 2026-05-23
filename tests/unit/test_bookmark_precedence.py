"""Unit tests for the bookmark_precedence config and the precedence-driven
TopicPartition selection in sync.bookmarked_partition_offset."""
import unittest
from unittest.mock import MagicMock

import confluent_kafka

import tap_kafka
from tap_kafka import sync
from tap_kafka.defaults import DEFAULT_BOOKMARK_PRECEDENCE
from tap_kafka.errors import InvalidBookmarkException, InvalidConfigException


MINIMAL_CONFIG = {
    'topic': 'my_topic',
    'group_id': 'my_group_id',
    'bootstrap_servers': 'server1,server2,server3',
}


class TestValidateBookmarkPrecedence(unittest.TestCase):
    """Pure-function tests for tap_kafka.validate_bookmark_precedence."""

    def test_default_value_is_offset_timestamp_start_time(self):
        self.assertEqual(DEFAULT_BOOKMARK_PRECEDENCE, ['offset', 'timestamp', 'start_time'])

    def test_accepts_empty_list(self):
        self.assertIsNone(tap_kafka.validate_bookmark_precedence([]))

    def test_accepts_single_allowed_key(self):
        for key in ('offset', 'timestamp', 'start_time'):
            with self.subTest(key=key):
                self.assertIsNone(tap_kafka.validate_bookmark_precedence([key]))

    def test_accepts_all_allowed_keys_in_any_order(self):
        self.assertIsNone(tap_kafka.validate_bookmark_precedence(['offset', 'timestamp', 'start_time']))
        self.assertIsNone(tap_kafka.validate_bookmark_precedence(['start_time', 'offset', 'timestamp']))
        self.assertIsNone(tap_kafka.validate_bookmark_precedence(['timestamp', 'start_time']))

    def test_rejects_non_list_types(self):
        for bad in (None, 'offset', {'offset': 1}, 42, ('offset',)):
            with self.subTest(value=bad), self.assertRaises(InvalidConfigException):
                tap_kafka.validate_bookmark_precedence(bad)

    def test_rejects_unknown_key(self):
        with self.assertRaises(InvalidConfigException):
            tap_kafka.validate_bookmark_precedence(['offset', 'nope'])

    def test_rejects_mixed_valid_and_unknown_keys(self):
        with self.assertRaises(InvalidConfigException):
            tap_kafka.validate_bookmark_precedence(['offset', 'timestamp', 'unsupported'])


class TestGenerateConfigBookmarkPrecedence(unittest.TestCase):
    """generate_config behavior for the bookmark_precedence field."""

    def test_default_applied_when_not_provided(self):
        config = tap_kafka.generate_config(dict(MINIMAL_CONFIG))
        self.assertEqual(config['bookmark_precedence'], DEFAULT_BOOKMARK_PRECEDENCE)

    def test_custom_value_respected(self):
        custom = dict(MINIMAL_CONFIG, bookmark_precedence=['start_time', 'offset'])
        config = tap_kafka.generate_config(custom)
        self.assertEqual(config['bookmark_precedence'], ['start_time', 'offset'])

    def test_invalid_bookmark_precedence_raises_during_generate(self):
        bad = dict(MINIMAL_CONFIG, bookmark_precedence=['offset', 'bogus'])
        with self.assertRaises(InvalidConfigException):
            tap_kafka.generate_config(bad)

    def test_non_list_bookmark_precedence_raises_during_generate(self):
        bad = dict(MINIMAL_CONFIG, bookmark_precedence='offset')
        with self.assertRaises(InvalidConfigException):
            tap_kafka.generate_config(bad)


class TestBookmarkedPartitionOffsetPrecedence(unittest.TestCase):
    """sync.bookmarked_partition_offset honors the precedence order."""

    TOPIC = 'test_topic'

    def _consumer_returning_offset(self, offset_value):
        """Mock consumer whose offsets_for_times returns a TopicPartition with the given offset."""
        consumer = MagicMock()

        def _offsets_for_times(topic_partitions):
            tp = topic_partitions[0]
            return [confluent_kafka.TopicPartition(tp.topic, tp.partition, offset_value)]

        consumer.offsets_for_times.side_effect = _offsets_for_times
        return consumer

    def test_default_precedence_picks_offset_when_present(self):
        consumer = self._consumer_returning_offset(999)
        bookmark = {'partition': 0, 'offset': 1234, 'timestamp': 1638132327000}

        tp = sync.bookmarked_partition_offset(consumer, self.TOPIC, bookmark)

        # Offset path is taken; consumer.offsets_for_times should NOT be invoked.
        consumer.offsets_for_times.assert_not_called()
        self.assertEqual(tp.partition, 0)

    def test_default_precedence_falls_to_timestamp_when_no_offset(self):
        consumer = self._consumer_returning_offset(555)
        bookmark = {'partition': 0, 'timestamp': 1638132327000}

        tp = sync.bookmarked_partition_offset(consumer, self.TOPIC, bookmark)

        consumer.offsets_for_times.assert_called_once()
        self.assertEqual(tp.offset, 555)

    def test_default_precedence_falls_to_start_time_when_only_start_time(self):
        consumer = self._consumer_returning_offset(777)
        bookmark = {'partition': 0, 'start_time': '2021-11-28T22:05:27.000'}

        tp = sync.bookmarked_partition_offset(consumer, self.TOPIC, bookmark)

        consumer.offsets_for_times.assert_called_once()
        self.assertEqual(tp.offset, 777)

    def test_custom_precedence_prefers_timestamp_over_offset(self):
        consumer = self._consumer_returning_offset(555)
        bookmark = {'partition': 0, 'offset': 1234, 'timestamp': 1638132327000}

        tp = sync.bookmarked_partition_offset(
            consumer, self.TOPIC, bookmark, bookmark_precedence=['timestamp', 'offset'])

        consumer.offsets_for_times.assert_called_once()
        self.assertEqual(tp.offset, 555)

    def test_custom_precedence_skips_keys_missing_from_bookmark(self):
        consumer = self._consumer_returning_offset(555)
        bookmark = {'partition': 0, 'timestamp': 1638132327000}

        tp = sync.bookmarked_partition_offset(
            consumer, self.TOPIC, bookmark, bookmark_precedence=['offset', 'timestamp'])

        # No 'offset' in bookmark, falls through to 'timestamp'.
        consumer.offsets_for_times.assert_called_once()
        self.assertEqual(tp.offset, 555)

    def test_custom_precedence_with_only_offset_when_bookmark_has_only_timestamp(self):
        consumer = self._consumer_returning_offset(555)
        bookmark = {'partition': 0, 'timestamp': 1638132327000}

        with self.assertRaises(InvalidBookmarkException):
            sync.bookmarked_partition_offset(
                consumer, self.TOPIC, bookmark, bookmark_precedence=['offset'])

    def test_empty_precedence_raises_invalid_bookmark(self):
        consumer = self._consumer_returning_offset(555)
        bookmark = {'partition': 0, 'offset': 1234, 'timestamp': 1638132327000}

        with self.assertRaises(InvalidBookmarkException):
            sync.bookmarked_partition_offset(consumer, self.TOPIC, bookmark, bookmark_precedence=[])


class TestGetBookmarkComment(unittest.TestCase):
    """sync.get_bookmark_comment serializes the configured precedence into the comment string."""

    def test_default_precedence_comment(self):
        comment = sync.get_bookmark_comment({'bookmark_precedence': DEFAULT_BOOKMARK_PRECEDENCE})
        self.assertEqual(comment, 'order of precedence : offset, timestamp, start_time; only one will be used')

    def test_custom_precedence_comment(self):
        comment = sync.get_bookmark_comment({'bookmark_precedence': ['timestamp', 'offset']})
        self.assertEqual(comment, 'order of precedence : timestamp, offset; only one will be used')

    def test_single_key_precedence_comment(self):
        comment = sync.get_bookmark_comment({'bookmark_precedence': ['start_time']})
        self.assertEqual(comment, 'order of precedence : start_time; only one will be used')


if __name__ == '__main__':
    unittest.main()
