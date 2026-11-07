from functools import lru_cache

import duckdb
import pandas as pd

from src.utils.config import GOLD_DIR

OLIST_FOLDERS = [GOLD_DIR / "olist" / "core", GOLD_DIR / "olist" / "marts"]


@lru_cache(maxsize=1)
def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    for folder in OLIST_FOLDERS:
        for path in sorted(p for p in folder.iterdir() if p.is_dir()):
            con.execute(
                f"create view {path.name} as select * from read_parquet('{path.as_posix()}/*.parquet')")
    return con


def q(query: str) -> pd.DataFrame:
    return connect().execute(query).df()


def tables() -> list[str]:
    return [r[0] for r in connect().execute("select view_name from duckdb_views() where not internal").fetchall()]
