"""Unit tests for the bookmark-offset+1 behavior and the future
initial_start_time handling in sync.set_partition_offsets."""
import unittest
from unittest.mock import MagicMock

import confluent_kafka

from tap_kafka import sync
from tap_kafka.defaults import DEFAULT_BOOKMARK_PRECEDENCE


TOPIC = 'test_topic'


def _consumer_mock(low=0, high=1000, offsets_for_times_offset=None, committed_offset=None):
    """Build a consumer mock with configurable watermarks and lookup responses."""
    consumer = MagicMock()
    consumer.get_watermark_offsets.return_value = (low, high)

    if offsets_for_times_offset is not None:
        def _offsets_for_times(topic_partitions):
            tp = topic_partitions[0]
            return [confluent_kafka.TopicPartition(tp.topic, tp.partition, offsets_for_times_offset)]
        consumer.offsets_for_times.side_effect = _offsets_for_times

    if committed_offset is not None:
        def _committed(topic_partitions):
            tp = topic_partitions[0]
            return [confluent_kafka.TopicPartition(tp.topic, tp.partition, committed_offset)]
        consumer.committed.side_effect = _committed

    return consumer


def _config(initial_start_time='latest', bookmark_precedence=None, auto_offset_reset='latest'):
    return {
        'topic': TOPIC,
        'initial_start_time': initial_start_time,
        'bookmark_precedence': bookmark_precedence or DEFAULT_BOOKMARK_PRECEDENCE,
        'auto_offset_reset': auto_offset_reset,
    }


def _state_with_bookmark(partition_num, **bookmark_fields):
    return {'bookmarks': {TOPIC: {f'partition_{partition_num}': {'partition': partition_num, **bookmark_fields}}}}


class TestPartitionByOffsetPlusOne(unittest.TestCase):
    """sync.partition_by_offset returns the bookmark offset incremented by one."""

    def test_returns_bookmark_offset_plus_one(self):
        consumer = MagicMock()
        bookmark = {'partition': 0, 'offset': 1234}

        tp = sync.partition_by_offset(consumer, TOPIC, bookmark)

        self.assertEqual(tp.topic, TOPIC)
        self.assertEqual(tp.partition, 0)
        self.assertEqual(tp.offset, 1235)

    def test_does_not_call_offsets_for_times(self):
        consumer = MagicMock()
        bookmark = {'partition': 0, 'offset': 1234}

        sync.partition_by_offset(consumer, TOPIC, bookmark)

        consumer.offsets_for_times.assert_not_called()

    def test_offset_zero_becomes_one(self):
        consumer = MagicMock()
        bookmark = {'partition': 3, 'offset': 0}

        tp = sync.partition_by_offset(consumer, TOPIC, bookmark)

        self.assertEqual(tp.offset, 1)


class TestSetPartitionOffsetsBookmarkValidity(unittest.TestCase):
    """set_partition_offsets bookmark-validity rules (offset >= low_offset)."""

    def test_bookmark_within_range_is_used(self):
        consumer = _consumer_mock(low=100, high=500)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]
        state = _state_with_bookmark(0, offset=200, timestamp=1638132327000)

        result = sync.set_partition_offsets(consumer, partitions, _config(), state)

        # Bookmark offset 200 is valid (>= low 100), offset+1 applied.
        self.assertEqual(result[0].offset, 201)

    def test_bookmark_below_low_offset_falls_through_to_initial_start_time(self):
        # low=500 means bookmark offset 200 is invalid; fall through to 'latest'.
        consumer = _consumer_mock(low=500, high=1000)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]
        state = _state_with_bookmark(0, offset=200, timestamp=1638132327000)

        result = sync.set_partition_offsets(consumer, partitions, _config(initial_start_time='latest'), state)

        # 'latest' sets to high_offset - 1.
        self.assertEqual(result[0].offset, 999)

    def test_bookmark_at_high_offset_is_caught_up_and_kept(self):
        # Bookmark caught up: offset 999 + 1 = 1000 == high_offset. Should be kept.
        consumer = _consumer_mock(low=0, high=1000)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]
        state = _state_with_bookmark(0, offset=999, timestamp=1638132327000)

        result = sync.set_partition_offsets(consumer, partitions, _config(), state)

        self.assertEqual(result[0].offset, 1000)

    def test_bookmark_beyond_high_offset_clamps_to_low(self):
        # Bookmark sits past the high watermark (partition truncated/recreated).
        consumer = _consumer_mock(low=0, high=500)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]
        state = _state_with_bookmark(0, offset=999, timestamp=1638132327000)

        result = sync.set_partition_offsets(consumer, partitions, _config(), state)

        # offset 999 + 1 = 1000 exceeds high 500 → clamped to low_offset to re-read available messages.
        self.assertEqual(result[0].offset, 0)

    def test_bookmark_at_low_offset_is_used(self):
        consumer = _consumer_mock(low=100, high=500)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]
        state = _state_with_bookmark(0, offset=99, timestamp=1638132327000)

        # 99 + 1 = 100 == low_offset → valid.
        result = sync.set_partition_offsets(consumer, partitions, _config(), state)
        self.assertEqual(result[0].offset, 100)


class TestSetPartitionOffsetsFutureStartTime(unittest.TestCase):
    """set_partition_offsets initial_start_time handling when no messages exist after the timestamp."""

    def test_iso_future_with_no_messages_uses_high_offset(self):
        # offsets_for_times returns -1 → no messages at/after timestamp → use high_offset.
        consumer = _consumer_mock(low=0, high=500, offsets_for_times_offset=-1)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]

        result = sync.set_partition_offsets(
            consumer, partitions, _config(initial_start_time='2099-01-01T00:00:00'), state={})

        self.assertEqual(result[0].offset, 500)

    def test_iso_past_with_messages_uses_max_of_returned_and_low(self):
        # offsets_for_times returns 250 (> low_offset 100) → use 250.
        consumer = _consumer_mock(low=100, high=500, offsets_for_times_offset=250)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]

        result = sync.set_partition_offsets(
            consumer, partitions, _config(initial_start_time='2021-11-28T22:05:27.000'), state={})

        self.assertEqual(result[0].offset, 250)

    def test_iso_past_below_low_offset_clamped_to_low(self):
        # offsets_for_times returns 50 (< low_offset 100) → clamped to low.
        consumer = _consumer_mock(low=100, high=500, offsets_for_times_offset=50)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]

        result = sync.set_partition_offsets(
            consumer, partitions, _config(initial_start_time='2021-11-28T22:05:27.000'), state={})

        self.assertEqual(result[0].offset, 100)


if __name__ == '__main__':
    unittest.main()
