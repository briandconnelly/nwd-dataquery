# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-04-25

### Changed

- SSL trust handling: the client now fetches the missing TLS intermediate from USACE's endpoint via AIA (using `aia-chaser`) and builds a per-endpoint `SSLContext`, replacing the previous import-time `truststore.inject_into_ssl()` global injection. Importing `nwd_dataquery` no longer mutates Python's SSL stack. Required because USACE began serving only the leaf certificate ~2026-04-19, breaking chain validation on platforms whose TLS stack doesn't AIA-chase (notably Linux + CPython).

### Fixed

- `nwd-dq fetch --out PATH` now writes CSV and NDJSON output to the given file; previously it was ignored for non-parquet formats and output always went to stdout.

### Removed

- `truststore` is no longer a runtime dependency.

## [0.1.0] - 2026-04-19

Initial release.

### Added

- `AsyncDataQueryClient` — async Python client for the USACE NWD Dataquery 2.0 `getjson` endpoint.
- `client.fetch()` returns a long-format frame; `pyarrow.Table` by default, `polars` or `pandas` via `backend=`.
- `client.describe()` returns location + tsid metadata as a dict (no values).
- Multi-tsid requests in a single call.
- Absolute (`start`/`end`) or relative (`lookback`) time windows; server-side timezone bucketing via `timezone=`.
- Endpoint override (`AsyncDataQueryClient(endpoint=...)`) for the CROHMS mirror or alternate hosts.
- `DataQueryError` raised when the server returns a top-level `"error"` key in the payload.
- `UnknownTsidWarning` emitted on empty payloads (unknown tsid / no data in window / seasonal gauge down).
- On import, `truststore.inject_into_ssl()` is called so USACE `.mil` domains validate against the OS trust store.
- `nwd-dq` CLI with `fetch` and `describe` subcommands; CSV / NDJSON / Parquet output.
- Relative `--lookback` durations (`7d`, `48h`, `10y`, …) on the CLI.
- `--latest` flag to keep only the most recent row per tsid.
- `--strict` flag to exit `3` on empty results.
- Exit codes: `0` success, `1` transport error, `2` server/input error, `3` empty with `--strict`.
- Cassette-based integration tests and opt-in live smoke tests (`-m live`).

[Unreleased]: https://github.com/briandconnelly/nwd-dataquery/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/briandconnelly/nwd-dataquery/releases/tag/v0.2.0
[0.1.0]: https://github.com/briandconnelly/nwd-dataquery/releases/tag/v0.1.0
