"""Unit tests for the auto_offset_reset config option and its wiring into the
sync Kafka consumer's auto.offset.reset setting."""
import unittest
from unittest.mock import patch

from tap_kafka import sync

from tests.unit.helper.config import consumer_config_for_sync as _consumer_config_for_sync


class TestInitKafkaConsumerAutoOffsetReset(unittest.TestCase):
    """auto.offset.reset wiring inside tap_kafka.sync.init_kafka_consumer()."""

    def _run_init(self, config):
        with patch('confluent_kafka.DeserializingConsumer') as consumer_cls:
            sync.init_kafka_consumer(config)

        consumer_cls.assert_called_once()
        return consumer_cls.call_args[0][0]

    def test_default_latest_passed_through(self):
        conf = self._run_init(_consumer_config_for_sync())
        self.assertEqual(conf['auto.offset.reset'], 'latest')

    def test_earliest_override_passed_through(self):
        conf = self._run_init(_consumer_config_for_sync(auto_offset_reset='earliest'))
        self.assertEqual(conf['auto.offset.reset'], 'earliest')


if __name__ == '__main__':
    unittest.main()
