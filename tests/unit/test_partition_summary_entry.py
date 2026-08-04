"""Unit tests for sync.build_partition_summary_entry — the pure helper that builds
one per-partition entry for the per-run offset summary (DIO-8)."""
import unittest

from tap_kafka import sync
from tap_kafka.defaults import DEFAULT_BOOKMARK_PRECEDENCE


class TestBuildPartitionSummaryEntry(unittest.TestCase):
    """The summary entry decouples evidence (bookmark_value) from outcome (resolved_by)."""

    def test_offset_bookmark_used(self):
        """Offset bookmark resolves the partition: resolved_by 'offset', value is the stored offset."""
        bookmark = {'partition': 0, 'offset': 1234, 'timestamp': 1638132327000}
        entry = sync.build_partition_summary_entry(
            low=1000, high=2000, selected_offset=1235,
            matched_bookmark=bookmark, precedence=DEFAULT_BOOKMARK_PRECEDENCE,
            found_in_bookmark=True)

        self.assertEqual(entry['resolved_by'], 'offset')
        self.assertEqual(entry['bookmark_value'], 1234)
        self.assertEqual(entry['selected_offset'], 1235)
        self.assertEqual(entry['watermarks'], [1000, 2000])
        self.assertFalse(entry['caught_up'])

    def test_timestamp_bookmark_used(self):
        """Timestamp bookmark resolves: bookmark_value is the epoch, not null."""
        bookmark = {'partition': 0, 'timestamp': 1638132327000}
        entry = sync.build_partition_summary_entry(
            low=1000, high=2000, selected_offset=1500,
            matched_bookmark=bookmark, precedence=DEFAULT_BOOKMARK_PRECEDENCE,
            found_in_bookmark=True)

        self.assertEqual(entry['resolved_by'], 'timestamp')
        self.assertEqual(entry['bookmark_value'], 1638132327000)

    def test_stale_offset_below_low_watermark_falls_through(self):
        """A bookmark that existed but was unusable reads 'initial_start_time',
        yet still carries its evidence (the stale value)."""
        bookmark = {'partition': 0, 'offset': 200, 'timestamp': 1638132327000}
        entry = sync.build_partition_summary_entry(
            low=1000, high=2000, selected_offset=1999,
            matched_bookmark=bookmark, precedence=DEFAULT_BOOKMARK_PRECEDENCE,
            found_in_bookmark=False)

        self.assertEqual(entry['resolved_by'], 'initial_start_time')
        self.assertEqual(entry['bookmark_value'], 200)

    def test_missing_bookmark_is_distinguishable_from_stale(self):
        """No bookmark at all: bookmark_value is null, resolved_by 'initial_start_time'."""
        entry = sync.build_partition_summary_entry(
            low=1000, high=2000, selected_offset=1999,
            matched_bookmark=None, precedence=DEFAULT_BOOKMARK_PRECEDENCE,
            found_in_bookmark=False)

        self.assertEqual(entry['resolved_by'], 'initial_start_time')
        self.assertIsNone(entry['bookmark_value'])

    def test_caught_up_when_selected_offset_at_high(self):
        entry = sync.build_partition_summary_entry(
            low=1000, high=2000, selected_offset=2000,
            matched_bookmark={'partition': 0, 'offset': 1999},
            precedence=DEFAULT_BOOKMARK_PRECEDENCE, found_in_bookmark=True)

        self.assertTrue(entry['caught_up'])

    def test_not_caught_up_when_selected_offset_above_high(self):
        # A stale offset above the high watermark is not 'caught up'.
        entry = sync.build_partition_summary_entry(
            low=1000, high=2000, selected_offset=2001,
            matched_bookmark={'partition': 0, 'offset': 2000},
            precedence=DEFAULT_BOOKMARK_PRECEDENCE, found_in_bookmark=True)

        self.assertFalse(entry['caught_up'])


if __name__ == '__main__':
    unittest.main()
