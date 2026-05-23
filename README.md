# pipelinewise-tap-kafka

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-kafka.svg)](https://badge.fury.io/py/pipelinewise-tap-kafka)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-kafka.svg)](https://pypi.org/project/pipelinewise-tap-kafka/)
[![License: MIT](https://img.shields.io/badge/License-GPLv3-yellow.svg)](https://opensource.org/licenses/GPL-3.0)

This is a [Singer](https://singer.io) tap that reads data from Kafka topic and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## How to use it

This tap is a standard [Singer Tap](https://singer.io). It can be run directly via the Singer CLI, or invoked as a plugin from an orchestrator like [Meltano](https://meltano.com/) that handles config generation and state management.

## Install and Run

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).

It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  pip install pipelinewise-tap-kafka
```

or

```bash
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install .
```

### Configuration

### Create a config.json

```
{
  "bootstrap_servers": "foo.com,bar.com",
  "group_id": "my_group",
  "topic": "my_topic",
  "primary_keys": {
    "id": "/path/to/primary_key"
  }
}
```

Full list of options in `config.json`:

| Property                  | Type    | Required? | Description                                                                                                                                                                                                                                        |
|---------------------------|---------|-----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| bootstrap_servers         | String  | Yes       | `host[:port]` string (or list of comma separated `host[:port]` strings) that the consumer should contact to bootstrap initial cluster metadata.                                                                                                    |
| group_id                  | String  | Yes       | The name of the consumer group to join for dynamic partition assignment (if enabled), and to use for fetching and committing offsets.                                                                                                              |
| topic                     | String  | Yes       | Name of kafka topic to subscribe to                                                                                                                                                                                                                |
| client_id                 | String  |           | (Default: `TAP_KAFKA_CLIENT_ID` env var, otherwise unset) Optional `client.id` value passed to the Kafka consumer.                                                                                                                                 |
| partitions                | List    |           | (Default: [] (all)) Partition(s) of topic to consume, example `[0,4]`                                                                                                                                                                              |
| primary_keys              | Object  |           | Optionally you can define primary key for the consumed messages. It requires a column name and `/slashed/paths` ala xpath selector to extract the value from the kafka messages. The extracted column will be added to every output singer message. |
| use_message_key           | Bool    |           | (Default: true) Defines whether to use Kafka message key as a primary key for the record. Note: if a custom primary key(s) has been defined, it will be used instead of the message_key.                                                           |
| initial_start_time        | String  |           | (Default: latest) Start time reference of the message consumption if no bookmarked position in `state.json`. One of: `beginning`, `earliest`, `latest` or an ISO-8601 formatted timestamp string. ISO timestamps may be in the future; in that case the tap starts at the partition's high watermark and waits for new messages. |
| bookmark_precedence       | List    |           | (Default: `["offset", "timestamp", "start_time"]`) Order in which bookmark fields are consulted when resuming. The first field present in the bookmark wins. Allowed values: `offset`, `timestamp`, `start_time`.                                  |
| max_runtime_ms            | Integer |           | (Default: 300000) The maximum time for the tap to collect new messages from Kafka topic. If this time exceeds it will flush the batch and close kafka connection.                                                                                  |
| commit_interval_ms        | Integer |           | (Default: 5000) Number of milliseconds between two commits. This is different than the kafka auto commit feature. Tap-kafka sends commit messages automatically but only when the data consumed successfully and persisted to local store.         |
| consumer_timeout_ms       | Integer |           | (Default: 10000) KafkaConsumer setting. Number of milliseconds to block during message iteration before raising StopIteration                                                                                                                      |
| session_timeout_ms        | Integer |           | (Default: 30000) KafkaConsumer setting. The timeout used to detect failures when using Kafka’s group management facilities.                                                                                                                        |
| heartbeat_interval_ms     | Integer |           | (Default: 10000) KafkaConsumer setting. The expected time in milliseconds between heartbeats to the consumer coordinator when using Kafka’s group management facilities.                                                                           |
| max_poll_interval_ms      | Integer |           | (Default: 300000) KafkaConsumer setting. The maximum delay between invocations of poll() when using consumer group management.                                                                                                                     |
| poll_empty_retry_wait_ms  | Integer |           | (Default: -1) When a poll returns no message, wait this many milliseconds before polling again, until `max_runtime_ms` is exhausted. The default of `-1` preserves the previous behavior of stopping on the first empty poll.                       |
| message_format            | String  |           | (Default: json) Supported message formats are `json` and `protobuf`.                                                                                                                                                                               |
| proto_schema              | String  |           | Protobuf message format in `.proto` syntax. Required if the `message_format` is `protobuf`.                                                                                                                                                        |
| proto_classes_dir         | String  |           | (Default: current working dir)                                                                                                                                                                                                                     |
| debug_contexts            | String  |           | comma separated list of debug contexts to enable for the consumer [see librkafka](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#debug-contexts)                                                                                                                                     |
| security_protocol         | String  |           | (Default: `SASL_SSL`) Kafka security protocol. Applied only when both `sasl_username` and `sasl_password` are set. Common values: `SASL_SSL`, `SASL_PLAINTEXT`.                                                                                     |
| sasl_mechanisms           | String  |           | (Default: `PLAIN`) SASL mechanism. Applied only when both `sasl_username` and `sasl_password` are set. Common values: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`.                                                                                    |
| sasl_username             | String  |           | (Default: `TAP_KAFKA_SASL_USERNAME` env var, otherwise unset) SASL username. SASL settings are only applied when both `sasl_username` and `sasl_password` are present.                                                                              |
| sasl_password             | String  |           | (Default: `TAP_KAFKA_SASL_PASSWORD` env var, otherwise unset) SASL password. SASL settings are only applied when both `sasl_username` and `sasl_password` are present.                                                                              |

### Environment variables

A few config values can be sourced from environment variables when not provided in `config.json`. Explicit values in `config.json` always win over the env var.

| Environment variable        | Maps to       |
|-----------------------------|---------------|
| `TAP_KAFKA_CLIENT_ID`       | `client_id`   |
| `TAP_KAFKA_SASL_USERNAME`   | `sasl_username` |
| `TAP_KAFKA_SASL_PASSWORD`   | `sasl_password` |

Note: when running this tap under Meltano, Meltano resolves environment variables into the generated `config.json` itself based on its plugin-setting conventions, so these tap-level env vars are most useful when invoking `tap-kafka` directly via the Singer CLI for local development or debugging.

### Connecting to Kafka with SASL authentication

To connect to a cluster that requires SASL/SSL, set the credentials and any non-default protocol/mechanism. Putting credentials in env vars keeps them out of `config.json`:

```bash
export TAP_KAFKA_SASL_USERNAME='my-user'
export TAP_KAFKA_SASL_PASSWORD='my-password'
```

```json
{
  "bootstrap_servers": "foo.com:9093,bar.com:9093",
  "group_id": "my_group",
  "topic": "my_topic",
  "security_protocol": "SASL_SSL",
  "sasl_mechanisms": "SCRAM-SHA-512"
}
```

SASL keys are only added to the Kafka consumer config when both `sasl_username` and `sasl_password` resolve to a truthy value.

### Bookmark and resume behavior

- On resume, the tap starts consuming at `bookmark.offset + 1` so the last processed message is not delivered twice.
- The bookmark order of precedence (`offset` → `timestamp` → `start_time` by default) can be reordered or trimmed via `bookmark_precedence`. The first key present in the bookmark wins.
- If the bookmarked offset is below the partition's current low watermark (e.g. messages were aged out), the bookmark is considered invalid and the tap falls back to `initial_start_time`.
- If the bookmarked offset is at or beyond the high watermark, the tap is considered caught up and waits at that offset for new messages.
- For an ISO `initial_start_time` in the future, the tap starts at the partition's high watermark instead of consuming any pre-existing messages.


This tap reads Kafka messages and generating singer compatible SCHEMA and RECORD messages in the following format.

| Property Name               | Description                                                                         |
|-----------------------------|-------------------------------------------------------------------------------------|
| MESSAGE_TIMESTAMP           | Timestamp extracted from the kafka metadata                                         |
| MESSAGE_OFFSET              | Offset extracted from the kafka metadata                                            |
| MESSAGE_PARTITION           | Partition extracted from the kafka metadata                                         |
| MESSAGE                     | The original Kafka message                                                          |
| MESSAGE_KEY                 | (Optional) Added by default (can be overridden) in case no custom keys defined      |
| DYNAMIC_PRIMARY_KEY(S)      | (Optional) Dynamically added primary key values, extracted from the Kafka message   |


### Run the tap in Discovery Mode

```
tap-kafka --config config.json --discover                # Should dump a Catalog to stdout
tap-kafka --config config.json --discover > catalog.json # Capture the Catalog
```

### Add Metadata to the Catalog

Each entry under the Catalog's "stream" key will need the following metadata:

```
{
  "streams": [
    {
      "stream_name": "my_topic"
      "metadata": [{
        "breadcrumb": [],
        "metadata": {
          "selected": true,
        }
      }]
    }
  ]
}
```

### Run the tap in Sync Mode

```
tap-kafka --config config.json --properties catalog.json
```

The tap will write bookmarks to stdout which can be captured and passed as an optional `--state state.json` parameter to the tap for the next sync.

## To run tests:

1. Install python test dependencies in a virtual env and run nose unit and integration tests
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install -e .[test]
```

2. To run unit tests:
```
  make unit_test
```

3. To run integration test:
```
  make integration_test
```

## To run pylint:

1. Install python dependencies and run python linter
```
  python3 -m venv venv
  . venv/bin/activate
  pip install --upgrade pip
  pip install -e .[test]
  pylint tap_kafka -d C,W,unexpected-keyword-arg,duplicate-code
```

# Using local dev container

## Create local dev container
```
docker run --name pipelinewise-tap-kafka -it --entrypoint bash -v ${pwd}:/project python:3.14-slim
cd project/
pip install --upgrade pip
pip install -e .[test]
pip install .
```

## Start local dev container (after it exists/is created)
```
# start and attach
docker start pipelinewise-tap-kafka -a

# or attach (if its already running)
docker attach pipelinewise-tap-kafka
```

## Run the thing from in local dev container
```
cd project/

# dump a Catalog to stdout
tap-kafka --config config.json --discover

# Capture the Catalog to file
tap-kafka --config config.json --discover > catalog.json

# run sync
tap-kafka --config config.json --properties catalog.json
tap-kafka --config config.json --properties catalog.json --state state.json
```
