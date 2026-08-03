"""Unit tests for the auto_offset_reset config option and its wiring into the
sync Kafka consumer's auto.offset.reset setting."""
import unittest
from unittest.mock import patch

from tap_kafka import sync


def _consumer_config_for_sync(**overrides):
    """Build a kafka_config dict that init_kafka_consumer accepts."""
    config = {
        'bootstrap_servers': 'server1,server2,server3',
        'group_id': 'my_group_id',
        'session_timeout_ms': 30000,
        'heartbeat_interval_ms': 10000,
        'max_poll_interval_ms': 300000,
        'auto_offset_reset': 'latest',
        'message_format': 'json',
        'debug_contexts': None,
        'client_id': None,
        'security_protocol': 'SASL_SSL',
        'sasl_mechanisms': 'PLAIN',
        'sasl_username': None,
        'sasl_password': None,
    }
    config.update(overrides)
    return config


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
