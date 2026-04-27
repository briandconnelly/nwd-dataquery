"""Static type-check assertions for the public API.

These assertions are validated by `ty` (and any other PEP 526-aware
type-checker), not by pytest at runtime. `assert_type()` is a runtime no-op.
The file name deliberately doesn't match pytest's `test_*.py` discovery
pattern so pytest doesn't even import it during collection — that matters
because `pandas`/`polars` at the top are unconditional imports and would
break collection in environments where those optional deps aren't installed.

`ty` checks this file via the project's `[tool.ty.src]` config (`include`
covers both `src/` and `tests/`) and the pre-commit `ty check` hook fires
on changes here. A future PR that breaks an `@overload` signature or a
TypedDict shape will fail the hook on this file.
"""

from typing import assert_type

import pandas as pd
import polars as pl
import pyarrow as pa

from nwd_dataquery import AsyncDataQueryClient, DataQueryPayload


async def _typing_smoke() -> None:
    client = AsyncDataQueryClient()

    assert_type(await client.fetch_raw("T"), DataQueryPayload)
    assert_type(await client.describe("T"), DataQueryPayload)

    # Default backend is pyarrow.
    assert_type(await client.fetch("T"), pa.Table)
    assert_type(await client.fetch("T", backend="pyarrow"), pa.Table)
    assert_type(await client.fetch("T", backend="polars"), pl.DataFrame)
    assert_type(await client.fetch("T", backend="pandas"), pd.DataFrame)
