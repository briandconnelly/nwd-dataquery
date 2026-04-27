# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `nwd-dq fetch --no-header` to omit the CSV header row (CSV-only; combining with `--format ndjson`/`json`/`parquet` exits 2).
- `nwd-dq fetch --fail-empty` as the new canonical name for the empty-result exit-3 contract.
- `nwd-dq fetch` rejects `--start > --end` with exit 2 before issuing any request.
- `nwd-dq fetch`, `describe`, and `raw` reject explicit `--lookback` combined with both `--start` and `--end` with exit 2 (previously silently ignored). The same overspecified call on `AsyncDataQueryClient.fetch_raw`/`fetch`/`describe` now raises `ValueError`.
- `nwd-dq --install-completion` and `nwd-dq --show-completion` for bash/zsh/fish (closes #33).
- `nwd-dq fetch`/`describe`/`raw` accept `--retries N` (default 2) and `--retry-backoff SECONDS` (default 1.0). Transport errors (connect/read timeouts, network errors) and HTTP 5xx are retried with exponential backoff; 4xx, `DataQueryError`, and other exceptions are never retried. Stderr now distinguishes connect timeout, read timeout, and 5xx in the error message. Retry warnings respect `--quiet`. `nwd-dq describe` now also accepts `-q`/`--quiet` (matching `fetch` and `raw`) so retry warnings can be suppressed there too. Closes #31.

### Deprecated

- `nwd-dq fetch --strict` is deprecated in favor of `--fail-empty`. It still works and still exits 3 on empty, but now prints a one-line deprecation warning to stderr. It will be removed in a future release.

### Changed

- `AsyncDataQueryClient.fetch_raw` (and therefore `fetch`/`describe`) now fills `end = datetime.now(UTC)` when only `start` is provided. Previously the request was sent with `startdate` only and no `enddate`, leaving the upstream end of the window up to the server.
- The `lookback` keyword on `fetch_raw`, `fetch`, and `describe` defaults to `None` instead of `DEFAULT_LOOKBACK`. `None` is resolved to `DEFAULT_LOOKBACK` internally, so callers that omit the argument see no change; callers that passed `lookback=...` alongside both `start=` and `end=` now get a `ValueError`.
- `nwd-dq raw` now accepts the same `--start`/`--end` formats as `fetch` and `describe`, including ISO-8601 with `Z` or numeric offset. Internal refactor consolidated the per-subcommand option declarations and async-run/error-mapping blocks into shared `Annotated` aliases and `_client()`/`_run()` helpers — as a side effect, `nwd-dq describe --help` now also shows help text for `--timeout` and `--endpoint` (previously empty). No runtime behavior change.
- `AsyncDataQueryClient.fetch_raw` (and therefore `fetch`/`describe`) now raises `ValueError` when both `start` and `end` are explicitly provided and `start > end`. Previously this was caught only by the CLI; library callers could issue an upside-down window and get a confusing upstream response. Naive datetimes are treated as UTC for the comparison, matching the existing `_iso` normalization, and the error message reports both bounds in normalized UTC isoformat.
- `AsyncDataQueryClient.fetch_raw` (and `fetch`/`describe`) now raises `httpx.HTTPStatusError` for 5xx responses even when the body contains `{"error": ...}`. Previously the error key short-circuited to `DataQueryError` regardless of status, which made transient server failures look like permanent application errors and bypassed any retry logic in callers. The original error message remains accessible via `exc.response.json()`. 4xx-with-error-body and 2xx-with-error-body still raise `DataQueryError` as before.

### Removed

- The `timezone` kwarg on `AsyncDataQueryClient` and the `--timezone` flag on `nwd-dq fetch`/`describe`/`raw` are removed. They claimed to expose the upstream's per-request timezone bucketing, but our parser unconditionally labeled response timestamps as UTC, which silently corrupted data when the server returned non-GMT times. Live probing also showed the upstream silently falls back to local-sensor time on unknown timezone strings (including `"UTC"`), and reinterprets the `Z` suffix on `--start`/`--end` based on the requested timezone. `"GMT"` is the only string that produces timestamps consistent with our parser's UTC assumption, so we now hardcode it. Closes #6. Proper non-GMT support remains an open future feature.

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
