import json
import shutil
import tempfile
from pathlib import Path
import os, duckdb
from .config import Config
from .warehouse import connect

BASE_CTE = """
WITH base AS (
  SELECT
    event.play_id::VARCHAR                  AS play_id,
    to_timestamp(time)                      AS ts,
    ?::INT                                  AS year,
    ?::INT                                  AS month,
    coalesce(event.victory::BOOLEAN, FALSE) AS victory,
    coalesce(event.ascension_level::INT, 0) AS ascension_level,
    event.character_chosen::VARCHAR         AS character,
    event.currentPacks::VARCHAR             AS current_packs_csv,
    json_extract(event, '$.packChoices')    AS packChoices,
    json_extract(event, '$.card_choices')   AS card_choices,
    json_extract(event, '$.master_deck')    AS master_deck
  FROM read_json_auto(?, format='newline_delimited')
)
"""

SQL_RUNS = BASE_CTE + "SELECT play_id, ts, year, victory, ascension_level, character FROM base"

SQL_PACKS_PRESENT = BASE_CTE + """
SELECT play_id, year, TRIM(pack) AS pack
FROM base,
LATERAL UNNEST(STR_SPLIT(current_packs_csv, ',')) AS t(pack)
WHERE NULLIF(TRIM(pack), '') IS NOT NULL
"""

SQL_PACK_CHOICES = BASE_CTE + """
-- one row for the picked pack
SELECT
  b.play_id,
  b.year,
  json_extract_string(pc_obj, '$.picked') AS picked_pack,
  NULL::VARCHAR                           AS not_picked_pack
FROM base b
, LATERAL unnest(CAST(json_extract(b.packChoices, '$') AS JSON[])) AS pc(pc_obj)

UNION ALL

SELECT
  b.play_id,
  b.year,
  NULL::VARCHAR                           AS picked_pack,
  json_extract_string(np_val, '$')        AS not_picked_pack
FROM base b
, LATERAL unnest(CAST(json_extract(b.packChoices, '$') AS JSON[])) AS pc(pc_obj)
, LATERAL unnest(CAST(json_extract(pc_obj, '$.not_picked') AS JSON[])) AS np(np_val)
"""


SQL_CARDS = BASE_CTE + """
-- picked rows (exclude non-card picks)
SELECT b.play_id, b.year, 'choice' AS context,
       REGEXP_REPLACE(json_extract_string(cc_obj, '$.picked'), '\\+\\d+$', '') AS card_id,
       TRUE AS picked
FROM base b
, LATERAL UNNEST(CAST(json_extract(b.card_choices, '$') AS JSON[])) AS cc(cc_obj)
WHERE UPPER(json_extract_string(cc_obj, '$.picked')) <> 'SKIP'
  AND json_extract_string(cc_obj, '$.picked') <> 'Singing Bowl'
UNION ALL
-- not-picked rows (extract strings, no quotes)
SELECT b.play_id, b.year, 'choice',
       REGEXP_REPLACE(json_extract_string(np_val, '$'), '\\+\\d+$', '') AS card_id,
       FALSE AS picked
FROM base b
, LATERAL UNNEST(CAST(json_extract(b.card_choices, '$') AS JSON[])) AS cc(cc_obj)
, LATERAL UNNEST(CAST(json_extract(cc_obj, '$.not_picked') AS JSON[])) AS np(np_val)
UNION ALL
-- final deck rows (extract strings, no quotes)
SELECT play_id, year, 'final',
       REGEXP_REPLACE(json_extract_string(deck_val, '$'), '\\+\\d+$', '') AS card_id,
       NULL AS picked
FROM base
, LATERAL UNNEST(CAST(json_extract(master_deck, '$') AS JSON[])) AS d(deck_val)
"""


def parse_ym_from_path(p: Path) -> tuple[int, int]:
    # expects .../<YYYY>/<MM>/<DD>
    parts = p.parts
    y, m = int(parts[-3]), int(parts[-2])
    return y, m


def _copy(con: duckdb.DuckDBPyConnection, sql: str, out_dir: Path, params: list[str | int]):
    out_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
    COPY ({sql})
    TO '{out_dir.as_posix()}'
    (FORMAT PARQUET,
     PARTITION_BY (year),
     COMPRESSION ZSTD,
     ROW_GROUP_SIZE 256000,
     APPEND)
    """, params)
    # Should probably add ROW_GROUPS_PER_FILE 8, right now I have a lot of small files, recommended is 128-512MB per file


def discover_files(metrics_root: Path) -> list[Path]:
    return sorted(p for p in metrics_root.glob("*/*/*") if p.is_file())


def file_sig(p: Path):
    st = os.stat(p)
    return st.st_size, int(st.st_mtime)


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

    for f, size, mtime in todo:
        y, m = parse_ym_from_path(f)
        params = [y, m, f.as_posix()]  # year, month, file path

        _copy(con, SQL_RUNS,          config.parquet_paths["runs"],          params)
        _copy(con, SQL_PACKS_PRESENT, config.parquet_paths["packs_present"], params)
        _copy(con, SQL_PACK_CHOICES,  config.parquet_paths["pack_choices"],  params)
        _copy(con, SQL_CARDS,         config.parquet_paths["cards"],         params)

        con.execute(
            "INSERT OR REPLACE INTO ingested_files VALUES (?,?,?)",
            [str(f), size, mtime],
        )

    con.commit()
    return len(todo)
