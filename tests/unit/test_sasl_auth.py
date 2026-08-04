"""Unit tests for SASL/SSL authentication config and Kafka consumer wiring."""
import unittest
from unittest.mock import patch, MagicMock

import tap_kafka
from tap_kafka import sync

from tests.unit.helper.config import consumer_config_for_sync as _consumer_config_for_sync


MINIMAL_CONFIG = {
    'topic': 'my_topic',
    'group_id': 'my_group_id',
    'bootstrap_servers': 'server1,server2,server3',
}


class TestGenerateConfigSasl(unittest.TestCase):
    """SASL-related behavior of tap_kafka.generate_config()."""

    def test_sasl_defaults_when_not_provided(self):
        """generate_config sets default security_protocol/sasl_mechanisms and leaves creds as None."""
        with patch.dict('os.environ', {}, clear=False) as _env:
            # Remove any pre-set env vars so the defaults path is exercised.
            import os
            os.environ.pop('TAP_KAFKA_SASL_USERNAME', None)
            os.environ.pop('TAP_KAFKA_SASL_PASSWORD', None)

            config = tap_kafka.generate_config(dict(MINIMAL_CONFIG))

        self.assertEqual(config['security_protocol'], tap_kafka.DEFAULT_SECURITY_PROTOCOL)
        self.assertEqual(config['sasl_mechanisms'], tap_kafka.DEFAULT_SASL_MECHANISMS)
        self.assertIsNone(config['sasl_username'])
        self.assertIsNone(config['sasl_password'])

    def test_sasl_custom_security_protocol_and_mechanism(self):
        """Custom security_protocol and sasl_mechanisms override the defaults."""
        custom = dict(MINIMAL_CONFIG,
                      security_protocol='SASL_PLAINTEXT',
                      sasl_mechanisms='SCRAM-SHA-512',
                      sasl_username='user',
                      sasl_password='pass')

        config = tap_kafka.generate_config(custom)

        self.assertEqual(config['security_protocol'], 'SASL_PLAINTEXT')
        self.assertEqual(config['sasl_mechanisms'], 'SCRAM-SHA-512')
        self.assertEqual(config['sasl_username'], 'user')
        self.assertEqual(config['sasl_password'], 'pass')

    def test_sasl_credentials_from_env_vars(self):
        """sasl_username/password fall back to env vars when not in config."""
        env = {
            'TAP_KAFKA_SASL_USERNAME': 'env_user',
            'TAP_KAFKA_SASL_PASSWORD': 'env_pass',
        }
        with patch.dict('os.environ', env, clear=False):
            config = tap_kafka.generate_config(dict(MINIMAL_CONFIG))

        self.assertEqual(config['sasl_username'], 'env_user')
        self.assertEqual(config['sasl_password'], 'env_pass')

    def test_sasl_config_takes_precedence_over_env_vars(self):
        """Explicit config values win over the env-var fallback."""
        env = {
            'TAP_KAFKA_SASL_USERNAME': 'env_user',
            'TAP_KAFKA_SASL_PASSWORD': 'env_pass',
        }
        custom = dict(MINIMAL_CONFIG, sasl_username='cfg_user', sasl_password='cfg_pass')
        with patch.dict('os.environ', env, clear=False):
            config = tap_kafka.generate_config(custom)

        self.assertEqual(config['sasl_username'], 'cfg_user')
        self.assertEqual(config['sasl_password'], 'cfg_pass')


class TestDoDiscoverySasl(unittest.TestCase):
    """SASL wiring inside tap_kafka.do_discovery()."""

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
        """Run do_discovery against a mocked Consumer and return the consumer conf passed in."""
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

    def test_sasl_keys_added_when_both_credentials_present(self):
        config = self._base_config(sasl_username='user', sasl_password='pass')

        conf = self._run_discovery(config)

        self.assertEqual(conf['security.protocol'], 'SASL_SSL')
        self.assertEqual(conf['sasl.mechanisms'], 'PLAIN')
        self.assertEqual(conf['sasl.username'], 'user')
        self.assertEqual(conf['sasl.password'], 'pass')

    def test_sasl_keys_use_custom_protocol_and_mechanism(self):
        config = self._base_config(
            sasl_username='user',
            sasl_password='pass',
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanisms='SCRAM-SHA-256',
        )

        conf = self._run_discovery(config)

        self.assertEqual(conf['security.protocol'], 'SASL_PLAINTEXT')
        self.assertEqual(conf['sasl.mechanisms'], 'SCRAM-SHA-256')

    def test_sasl_keys_omitted_when_credentials_missing(self):
        config = self._base_config()

        conf = self._run_discovery(config)

        self.assertNotIn('security.protocol', conf)
        self.assertNotIn('sasl.mechanisms', conf)
        self.assertNotIn('sasl.username', conf)
        self.assertNotIn('sasl.password', conf)

    def test_sasl_keys_omitted_when_only_username_present(self):
        config = self._base_config(sasl_username='user', sasl_password=None)

        conf = self._run_discovery(config)

        self.assertNotIn('sasl.username', conf)
        self.assertNotIn('sasl.password', conf)

    def test_sasl_keys_omitted_when_only_password_present(self):
        config = self._base_config(sasl_username=None, sasl_password='pass')

        conf = self._run_discovery(config)

        self.assertNotIn('sasl.username', conf)
        self.assertNotIn('sasl.password', conf)


class TestInitKafkaConsumerSasl(unittest.TestCase):
    """SASL wiring inside tap_kafka.sync.init_kafka_consumer()."""

    def _run_init(self, config):
        """Run init_kafka_consumer against a mocked DeserializingConsumer; return the conf."""
        with patch('confluent_kafka.DeserializingConsumer') as consumer_cls:
            sync.init_kafka_consumer(config)

        consumer_cls.assert_called_once()
        return consumer_cls.call_args[0][0]

    def test_sasl_keys_added_when_both_credentials_present(self):
        config = _consumer_config_for_sync(sasl_username='user', sasl_password='pass')

        conf = self._run_init(config)

        self.assertEqual(conf['security.protocol'], 'SASL_SSL')
        self.assertEqual(conf['sasl.mechanisms'], 'PLAIN')
        self.assertEqual(conf['sasl.username'], 'user')
        self.assertEqual(conf['sasl.password'], 'pass')

    def test_sasl_keys_use_custom_protocol_and_mechanism(self):
        config = _consumer_config_for_sync(
            sasl_username='user',
            sasl_password='pass',
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanisms='SCRAM-SHA-512',
        )

        conf = self._run_init(config)

        self.assertEqual(conf['security.protocol'], 'SASL_PLAINTEXT')
        self.assertEqual(conf['sasl.mechanisms'], 'SCRAM-SHA-512')

    def test_sasl_keys_omitted_when_credentials_missing(self):
        config = _consumer_config_for_sync()

        conf = self._run_init(config)

        self.assertNotIn('security.protocol', conf)
        self.assertNotIn('sasl.mechanisms', conf)
        self.assertNotIn('sasl.username', conf)
        self.assertNotIn('sasl.password', conf)

    def test_sasl_keys_omitted_when_only_username_present(self):
        config = _consumer_config_for_sync(sasl_username='user', sasl_password=None)

        conf = self._run_init(config)

        self.assertNotIn('sasl.username', conf)
        self.assertNotIn('sasl.password', conf)

    def test_sasl_keys_omitted_when_only_password_present(self):
        config = _consumer_config_for_sync(sasl_username=None, sasl_password='pass')

        conf = self._run_init(config)

        self.assertNotIn('sasl.username', conf)
        self.assertNotIn('sasl.password', conf)


if __name__ == '__main__':
    unittest.main()
