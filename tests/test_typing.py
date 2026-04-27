"""Static type-check assertions for the public API.

These assertions are validated by `ty` (and any other PEP 526-aware
type-checker), not by pytest at runtime. `assert_type()` is a runtime no-op,
and the only function in this file is not prefixed with `test_`, so pytest
collects nothing from this module.

This file is intentionally placed under `tests/` for proximity to other
verification work. The project's `[tool.ty.src]` config currently scopes ty
to `src/` only, so the pre-commit ty hook will not check this file
automatically. Run `uv run ty check tests/test_typing.py` manually to verify.
A follow-up issue may expand ty's scope to include `tests/`.
"""

from typing import assert_type

import pandas as pd
import polars as pl
import pyarrow as pa

from nwd_dataquery import AsyncDataQueryClient, DataqueryPayload


async def _typing_smoke() -> None:
    client = AsyncDataQueryClient()

    assert_type(await client.fetch_raw("T"), DataqueryPayload)
    assert_type(await client.describe("T"), DataqueryPayload)

    # Default backend is pyarrow.
    assert_type(await client.fetch("T"), pa.Table)
    assert_type(await client.fetch("T", backend="pyarrow"), pa.Table)
    assert_type(await client.fetch("T", backend="polars"), pl.DataFrame)
    assert_type(await client.fetch("T", backend="pandas"), pd.DataFrame)
