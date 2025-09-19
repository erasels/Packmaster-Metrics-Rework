import duckdb, os
from pathlib import Path

LEDGER_SQL = """
CREATE TABLE IF NOT EXISTS ingested_files(
  path TEXT PRIMARY KEY,
  size BIGINT,
  mtime BIGINT
);
"""

def connect(db_path: Path) -> duckdb.DuckDBPyConnection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path.as_posix())
    con.execute("PRAGMA threads = " + str(os.cpu_count() or 4))
    con.execute("PRAGMA enable_object_cache")
    con.execute(LEDGER_SQL)
    return con
