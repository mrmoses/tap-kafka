# Context

Domain glossary for `tap-kafka`. Definitions only — no implementation detail.

## Glossary

### Watermarks

The low and high offsets bounding the messages currently retained in a Kafka
partition. The **low watermark** is the earliest still-retained offset; the
**high watermark** is one past the last produced message. Together they describe
the range a consumer can actually read right now.

### Bookmark

The persisted record of how far a previous run consumed a partition. A bookmark
expresses its position as an **offset**, a **timestamp**, or a **start_time**
(ISO). It is the primary evidence used to decide where the next run resumes.

### Bookmark precedence

The ordered list that decides which bookmark key is honored when more than one is
present (default: `offset`, then `timestamp`, then `start_time`). Only the first
present-and-usable key is used.

### Resolution path

How a partition's start position was ultimately decided. Either a bookmark key
(`offset` / `timestamp` / `start_time`) or the `initial_start_time` fallback. A
bookmark that exists but cannot be used (e.g. its offset predates the low
watermark, or its timestamp has no messages at or after it) falls through to
`initial_start_time` — so the resolution path reflects what the position *boiled
down to*, not merely which key had precedence.

### initial_start_time

The configured fallback start position used when no usable bookmark exists
(`beginning`, `earliest`, `latest`, or an ISO timestamp).

### Selected offset (start position)

The offset the consumer is set to begin reading from for a run — the result of
applying the bookmark (or the `initial_start_time` fallback) against the
partition's watermarks.

### Caught up

A partition whose selected offset equals its high watermark — i.e. there are no
unread messages and the consumer will wait for new ones.

### Stale bookmark

A bookmark whose offset is *above* the partition's high watermark, indicating the
topic was recreated or shrank. Distinct from a bookmark below the low watermark,
which is expired rather than stale.
