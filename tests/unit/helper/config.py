"""Shared test config builders for the unit test suite."""


def consumer_config_for_sync(**overrides):
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
