from collections import defaultdict

import duckdb
import os
from pathlib import Path

from .config import Config
from .tables import *
from .warehouse import connect


def parse_ym_from_path(p: Path) -> tuple[int, int]:
    # expects .../<YYYY>/<MM>/<DD>
    parts = p.parts
    y, m = int(parts[-3]), int(parts[-2])
    return y, m


def discover_files(metrics_root: Path) -> list[Path]:
    return sorted(p for p in metrics_root.glob("*/*/*") if p.is_file())


def file_sig(p: Path):
    st = os.stat(p)
    return st.st_size, int(st.st_mtime)


def _copy_month(con: duckdb.DuckDBPyConnection, sql_tail: str, out_dir: Path, month_glob: str, file_paths: list[str]):
    """
        Copy one month of JSON run files into a partitioned Parquet dataset.

        Steps:
        - Create a temp table listing the exact files to ingest (`file_paths`).
        - Use read_json_auto on a month glob (e.g. ".../YYYY/MM/*") with filename=true.
        - Join to the temp table to filter only the changed files.
        - Apply BASE_CTE + sql_tail to shape the dataset.
        - Write results into Parquet, partitioned by (year, month).
        """
    out_dir.mkdir(parents=True, exist_ok=True)

    # Register files for this month
    con.execute("CREATE TEMP TABLE to_ingest(path TEXT)")
    con.executemany("INSERT INTO to_ingest VALUES (?)", [(p,) for p in file_paths])

    # Parse year/month from the glob
    year = int(month_glob.split("/")[-3])
    month = int(month_glob.split("/")[-2])

    con.execute(f"""
    COPY (
      {BASE_CTE}
      {sql_tail}
    )
    TO '{out_dir.as_posix()}'
    (FORMAT PARQUET,
     PARTITION_BY (year, month),
     COMPRESSION ZSTD,
     ROW_GROUP_SIZE 1000000,
     PER_THREAD_OUTPUT FALSE,
     APPEND)
    """, [year, month, month_glob])

    con.execute("DROP TABLE to_ingest")


def ingest(config: Config, paths: list[Path] | None = None) -> int:
    con = connect(config.duckdb_path)
    con.begin()

    rows = con.execute("SELECT path, size, mtime FROM ingested_files").fetchall()
    seen = {r[0]: (r[1], r[2]) for r in rows}

    todo: list[tuple[Path, int, int]] = []
    for f in (paths or discover_files(config.metrics_root)):
        size, mtime = file_sig(f)
        if seen.get(str(f)) != (size, mtime):
            todo.append((f, size, mtime))

    if not todo:
        con.commit()
        return 0

    # group by (year, month)
    by_ym = defaultdict(list)
    for f, size, mtime in todo:
        y, m = parse_ym_from_path(f)
        by_ym[(y, m)].append(str(f))

    # build month globs like ".../YYYY/MM/*"
    for (y, m), files in by_ym.items():
        month_glob = config.metrics_root.joinpath(f"{y:04d}/{m:02d}/*").as_posix()

        # run one COPY per slice for this month
        _copy_month(con, SQL_RUNS,          config.parquet_paths["runs"],          month_glob, files)
        _copy_month(con, SQL_MASTER_DECK,   config.parquet_paths["master_deck"],   month_glob, files)
        _copy_month(con, SQL_PACKS_PRESENT, config.parquet_paths["packs_present"], month_glob, files)
        _copy_month(con, SQL_PACK_CHOICES,  config.parquet_paths["pack_choices"],  month_glob, files)
        _copy_month(con, SQL_CARDS,         config.parquet_paths["cards"],         month_glob, files)

    # mark all ingested
    con.executemany(
        "INSERT OR REPLACE INTO ingested_files VALUES (?,?,?)",
        [(str(f), size, mtime) for f, size, mtime in todo],
    )
    con.commit()
    return len(todo)
