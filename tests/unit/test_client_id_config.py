"""Unit tests for the client_id config option (also driven by the
TAP_KAFKA_CLIENT_ID env var) and its wiring into Kafka consumer config."""
import os
import unittest
from unittest.mock import patch, MagicMock

import tap_kafka
from tap_kafka import sync


MINIMAL_CONFIG = {
    'topic': 'my_topic',
    'group_id': 'my_group_id',
    'bootstrap_servers': 'server1,server2,server3',
}


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


class TestGenerateConfigClientId(unittest.TestCase):
    """client_id behavior of tap_kafka.generate_config()."""

    def test_client_id_is_none_when_not_provided_and_env_var_unset(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('TAP_KAFKA_CLIENT_ID', None)
            config = tap_kafka.generate_config(dict(MINIMAL_CONFIG))

        self.assertIsNone(config['client_id'])

    def test_client_id_from_env_var(self):
        env = {'TAP_KAFKA_CLIENT_ID': 'env-client-id'}
        with patch.dict(os.environ, env, clear=False):
            config = tap_kafka.generate_config(dict(MINIMAL_CONFIG))

        self.assertEqual(config['client_id'], 'env-client-id')

    def test_explicit_config_value_respected(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('TAP_KAFKA_CLIENT_ID', None)
            custom = dict(MINIMAL_CONFIG, client_id='cfg-client-id')
            config = tap_kafka.generate_config(custom)

        self.assertEqual(config['client_id'], 'cfg-client-id')

    def test_explicit_config_value_takes_precedence_over_env_var(self):
        env = {'TAP_KAFKA_CLIENT_ID': 'env-client-id'}
        custom = dict(MINIMAL_CONFIG, client_id='cfg-client-id')
        with patch.dict(os.environ, env, clear=False):
            config = tap_kafka.generate_config(custom)

        self.assertEqual(config['client_id'], 'cfg-client-id')


class TestDoDiscoveryClientId(unittest.TestCase):
    """client.id wiring inside tap_kafka.do_discovery()."""

    def _base_config(self, **overrides):
        config = {
            'bootstrap_servers': 'server1,server2,server3',
            'group_id': 'my_group_id',
            'topic': 'my_topic',
            'session_timeout_ms': 30000,
            'client_id': None,
            'security_protocol': 'SASL_SSL',
            'sasl_mechanisms': 'PLAIN',
            'sasl_username': None,
            'sasl_password': None,
        }
        config.update(overrides)
        return config

    def _run_discovery(self, config):
        topic_md = MagicMock()
        topic_md.error = None
        cluster_md = MagicMock()
        cluster_md.topics = {config['topic']: topic_md}
        consumer_instance = MagicMock()
        consumer_instance.list_topics.return_value = cluster_md

        with patch('tap_kafka.Consumer', return_value=consumer_instance) as consumer_cls, \
             patch('tap_kafka.common.generate_catalog', return_value=[]), \
             patch('tap_kafka.dump_catalog'):
            tap_kafka.do_discovery(config)

        consumer_cls.assert_called_once()
        return consumer_cls.call_args[0][0]

    def test_client_id_added_when_set(self):
        conf = self._run_discovery(self._base_config(client_id='discovery-id'))
        self.assertEqual(conf['client.id'], 'discovery-id')

    def test_client_id_omitted_when_none(self):
        conf = self._run_discovery(self._base_config(client_id=None))
        self.assertNotIn('client.id', conf)

    def test_client_id_omitted_when_empty_string(self):
        # Empty string is falsy in the `if client_id:` check.
        conf = self._run_discovery(self._base_config(client_id=''))
        self.assertNotIn('client.id', conf)


class TestInitKafkaConsumerClientId(unittest.TestCase):
    """client.id wiring inside tap_kafka.sync.init_kafka_consumer()."""

    def _run_init(self, config):
        with patch('confluent_kafka.DeserializingConsumer') as consumer_cls:
            sync.init_kafka_consumer(config)

        consumer_cls.assert_called_once()
        return consumer_cls.call_args[0][0]

    def test_client_id_added_when_set(self):
        conf = self._run_init(_consumer_config_for_sync(client_id='sync-id'))
        self.assertEqual(conf['client.id'], 'sync-id')

    def test_client_id_omitted_when_none(self):
        conf = self._run_init(_consumer_config_for_sync(client_id=None))
        self.assertNotIn('client.id', conf)

    def test_client_id_omitted_when_empty_string(self):
        conf = self._run_init(_consumer_config_for_sync(client_id=''))
        self.assertNotIn('client.id', conf)


if __name__ == '__main__':
    unittest.main()
