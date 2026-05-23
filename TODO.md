# TODO

Open items captured while adding test coverage and updating documentation.
Track or close as work progresses.

## Repository

- [ ] Rename the repo away from `pipelinewise-tap-kafka` now that the
  `pipelinewise-singer-python` dependency has been replaced with
  `singer-python`. GitHub auto-redirects the old name for git remotes,
  web URLs, and the API, so existing clones keep working. Update the
  remaining `pipelinewise-tap-kafka` references in README badges,
  docker container names, and `setup.py` (`name=`) after renaming.

## Tests

- [ ] Coordinated cleanup of the two stale tests in
  `tests/unit/test_tap_kafka.py`:
  `TestSync.test_generate_config_with_defaults` and
  `TestSync.test_generate_config_with_custom_parameters`. They compare
  the full `generate_config` output dict and miss the newer fields
  (`client_id`, `bookmark_precedence`, `poll_empty_retry_wait_ms`, and
  the four SASL fields). Once the per-feature test branches are merged,
  add those keys to the expected dicts in a single follow-up branch.

## Code

- [ ] Likely bug in `tap_kafka/sync.py` (around line 461 on
  `python-updates`): the empty-poll retry block has
  `if sleep_time_s <= 0: break`, which makes
  `poll_empty_retry_wait_ms=0` behave the same as `-1` (immediate
  break) instead of busy-polling until `max_runtime_ms`. If
  busy-polling is intended for `0`, change the check to `< 0`. The
  current behavior is documented in
  `tests/unit/test_poll_empty_retry_wait.py::test_zero_wait_breaks_immediately_like_negative_one`
  and that test will need to flip once the code is fixed.
