"""Unit tests for bookmark range validation against partition watermarks.

When a bookmark's offset falls outside the partition's current low watermark,
set_partition_offsets falls back to the configured initial_start_time. When
the bookmark is in range, it is used regardless of initial_start_time."""
import unittest
from unittest.mock import MagicMock

import confluent_kafka

from tap_kafka import sync
from tap_kafka.defaults import DEFAULT_BOOKMARK_PRECEDENCE


TOPIC = 'test_topic'


def _consumer_mock(low=0, high=1000, committed_offset=None, offsets_for_times_offset=None):
    consumer = MagicMock()
    consumer.get_watermark_offsets.return_value = (low, high)

    if committed_offset is not None:
        def _committed(topic_partitions):
            tp = topic_partitions[0]
            return [confluent_kafka.TopicPartition(tp.topic, tp.partition, committed_offset)]
        consumer.committed.side_effect = _committed

    if offsets_for_times_offset is not None:
        def _offsets_for_times(topic_partitions):
            tp = topic_partitions[0]
            return [confluent_kafka.TopicPartition(tp.topic, tp.partition, offsets_for_times_offset)]
        consumer.offsets_for_times.side_effect = _offsets_for_times

    return consumer


def _config(initial_start_time, auto_offset_reset='latest'):
    return {
        'topic': TOPIC,
        'initial_start_time': initial_start_time,
        'bookmark_precedence': DEFAULT_BOOKMARK_PRECEDENCE,
        'auto_offset_reset': auto_offset_reset,
    }


def _state_with_offset_bookmark(partition_num, offset):
    return {
        'bookmarks': {
            TOPIC: {f'partition_{partition_num}': {'partition': partition_num, 'offset': offset}}
        }
    }


class TestOutOfRangeBookmarkFallsBackByInitialStartTimeMode(unittest.TestCase):
    """Each initial_start_time mode is exercised when the bookmark is below low_offset."""

    # Bookmark offset 50 → partition_by_offset yields 51, which is < low (100) → invalid.
    OUT_OF_RANGE_BOOKMARK_OFFSET = 50

    def test_falls_back_to_beginning(self):
        consumer = _consumer_mock(low=100, high=500)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]
        state = _state_with_offset_bookmark(0, self.OUT_OF_RANGE_BOOKMARK_OFFSET)

        result = sync.set_partition_offsets(consumer, partitions, _config('beginning'), state)

        self.assertEqual(result[0].offset, 100)

    def test_falls_back_to_earliest_using_committed_when_higher_than_low(self):
        consumer = _consumer_mock(low=100, high=500, committed_offset=250)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]
        state = _state_with_offset_bookmark(0, self.OUT_OF_RANGE_BOOKMARK_OFFSET)

        result = sync.set_partition_offsets(consumer, partitions, _config('earliest'), state)

        self.assertEqual(result[0].offset, 250)

    def test_falls_back_to_earliest_clamped_to_low_when_committed_is_lower(self):
        consumer = _consumer_mock(low=100, high=500, committed_offset=30)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]
        state = _state_with_offset_bookmark(0, self.OUT_OF_RANGE_BOOKMARK_OFFSET)

        result = sync.set_partition_offsets(consumer, partitions, _config('earliest'), state)

        self.assertEqual(result[0].offset, 100)

    def test_falls_back_to_iso_past_timestamp(self):
        consumer = _consumer_mock(low=100, high=500, offsets_for_times_offset=300)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]
        state = _state_with_offset_bookmark(0, self.OUT_OF_RANGE_BOOKMARK_OFFSET)

        result = sync.set_partition_offsets(
            consumer, partitions, _config('2021-11-28T22:05:27.000'), state)

        self.assertEqual(result[0].offset, 300)


class TestInRangeBookmarkWinsOverInitialStartTime(unittest.TestCase):
    """When the bookmark is in range, it is used regardless of initial_start_time."""

    def test_bookmark_wins_over_beginning(self):
        consumer = _consumer_mock(low=0, high=500)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]
        state = _state_with_offset_bookmark(0, 200)

        result = sync.set_partition_offsets(consumer, partitions, _config('beginning'), state)

        # offset 200 + 1, not low_offset 0.
        self.assertEqual(result[0].offset, 201)

    def test_bookmark_wins_over_earliest(self):
        consumer = _consumer_mock(low=0, high=500, committed_offset=400)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]
        state = _state_with_offset_bookmark(0, 200)

        result = sync.set_partition_offsets(consumer, partitions, _config('earliest'), state)

        # offset 200 + 1, not committed 400.
        self.assertEqual(result[0].offset, 201)
        consumer.committed.assert_not_called()

    def test_bookmark_wins_over_iso_timestamp(self):
        consumer = _consumer_mock(low=0, high=500, offsets_for_times_offset=400)
        partitions = [confluent_kafka.TopicPartition(TOPIC, 0)]
        state = _state_with_offset_bookmark(0, 200)

        result = sync.set_partition_offsets(
            consumer, partitions, _config('2021-11-28T22:05:27.000'), state)

        # offset 200 + 1, not the timestamp-resolved offset 400.
        self.assertEqual(result[0].offset, 201)
        consumer.offsets_for_times.assert_not_called()


class TestWatermarksEvaluatedPerPartition(unittest.TestCase):
    """Each partition's watermarks are fetched and evaluated independently."""

    def test_one_partition_uses_bookmark_other_falls_back(self):
        # Partition 0 watermarks: low=0, high=500   → bookmark offset 200 in range
        # Partition 1 watermarks: low=400, high=500 → bookmark offset 200 out of range
        def _watermarks(partition):
            return (0, 500) if partition.partition == 0 else (400, 500)

        consumer = MagicMock()
        consumer.get_watermark_offsets.side_effect = _watermarks

        partitions = [
            confluent_kafka.TopicPartition(TOPIC, 0),
            confluent_kafka.TopicPartition(TOPIC, 1),
        ]
        state = {
            'bookmarks': {
                TOPIC: {
                    'partition_0': {'partition': 0, 'offset': 200},
                    'partition_1': {'partition': 1, 'offset': 200},
                }
            }
        }

        result = sync.set_partition_offsets(consumer, partitions, _config('beginning'), state)

        offsets_by_partition = {tp.partition: tp.offset for tp in result}
        self.assertEqual(offsets_by_partition[0], 201)  # bookmark honored
        self.assertEqual(offsets_by_partition[1], 400)  # fell back to 'beginning' (low_offset)


if __name__ == '__main__':
    unittest.main()
