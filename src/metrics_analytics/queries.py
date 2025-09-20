import duckdb
from pathlib import Path


def _con(db: Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db.as_posix())


def win_rate_by_asc(db: Path):
    w = db.parent.as_posix()
    sql = f"""
    WITH r AS (
      SELECT ascension_level, victory::INT AS win
      FROM parquet_scan('{w}/runs_parquet')
    )
    SELECT
      ascension_level        AS "Ascension Level",
      CAST(SUM(win) AS INT)  AS "Won",
      COUNT(*)               AS "Total",
      AVG(win)               AS "Win Rate"
    FROM r
    GROUP BY ascension_level
    ORDER BY ascension_level
    """
    return _con(db).execute(sql).df()


def pack_pick_rate(db: Path):
    w = db.parent.as_posix()
    sql = f"""
    WITH picked AS (
      SELECT picked_pack AS pack, COUNT(*) AS picked_cnt
      FROM parquet_scan('{w}/pack_choices_parquet')
      WHERE picked_pack IS NOT NULL
      GROUP BY 1
    ),
    offered AS (
      SELECT COALESCE(picked_pack, not_picked_pack) AS pack, COUNT(*) AS offered_cnt
      FROM parquet_scan('{w}/pack_choices_parquet')
      GROUP BY 1
    )
    SELECT
      o.pack                              AS "Pack",
      COALESCE(p.picked_cnt, 0)           AS "Picked",
      offered_cnt                         AS "Seen",
      COALESCE(p.picked_cnt, 0)::DOUBLE / offered_cnt AS "Pick Rate"
    FROM offered o
    LEFT JOIN picked p USING (pack)
    ORDER BY "Pick Rate" DESC
    """
    return _con(db).execute(sql).df()


def pack_win_rate(db: Path, min_runs: int = 100):
    w = db.parent.as_posix()
    sql = f"""
    WITH runs AS (
      SELECT play_id, victory::INT AS win
      FROM parquet_scan('{w}/runs_parquet')
    ),
    presence AS (
      SELECT play_id, pack FROM parquet_scan('{w}/packs_present_parquet')
    )
    SELECT
      p.pack                           AS "Pack",
      CAST(SUM(r.win) AS INT)          AS "Wins",
      COUNT(*)                         AS "Total",
      AVG(r.win)                       AS "Win Rate"
    FROM presence p
    JOIN runs r USING (play_id)
    GROUP BY p.pack
    HAVING COUNT(*) >= {min_runs}
    ORDER BY "Win Rate" DESC
    """
    return _con(db).execute(sql).df()


def card_pick_rate(db: Path, min_seen: int = 200):
    w = db.parent.as_posix()
    sql = f"""
    WITH choices AS (
      SELECT card_id, picked::INT AS picked
      FROM parquet_scan('{w}/cards_parquet')
      WHERE context='choice'
    )
    SELECT
      card_id                  AS "Card",
      SUM(picked)              AS "Picked",
      COUNT(*)                 AS "Seen",
      AVG(picked)              AS "Pick Rate"
    FROM choices
    GROUP BY card_id
    HAVING COUNT(*) >= {min_seen}
    ORDER BY "Pick Rate" DESC
    """
    return _con(db).execute(sql).df()


def card_win_rate(db: Path, min_decks: int = 200):
    w = db.parent.as_posix()
    sql = f"""
    WITH finals AS (
      SELECT play_id, card_id
      FROM parquet_scan('{w}/cards_parquet')
      WHERE context='final'
    ),
    runs AS (
      SELECT play_id, victory::INT AS win
      FROM parquet_scan('{w}/runs_parquet')
    )
    SELECT
      f.card_id                            AS "Card",
      CAST(SUM(r.win) AS INTEGER)          AS "Wins",
      COUNT(*)                             AS "Total",
      AVG(r.win)                           AS "Win Rate"
    FROM finals f
    JOIN runs r USING (play_id)
    GROUP BY f.card_id
    HAVING COUNT(*) >= {min_decks}
    ORDER BY "Win Rate" DESC
    """
    return _con(db).execute(sql).df()


def pack_asc_win_rate(db: Path, min_runs: int = 1):
    w = db.parent.as_posix()
    sql = f"""
    WITH runs AS (
      SELECT play_id, ascension_level::INT AS ascension_level, victory::INT AS win
      FROM parquet_scan('{w}/runs_parquet')
    ),
    presence AS (
      SELECT play_id, TRIM(pack) AS pack
      FROM parquet_scan('{w}/packs_present_parquet')
    ),
    pack_overall AS (
      SELECT p.pack, SUM(r.win) AS wins, COUNT(*) AS total
      FROM presence p
      JOIN runs r USING (play_id)
      GROUP BY p.pack
      HAVING COUNT(*) >= {min_runs}
    ),
    pack_asc AS (
      SELECT p.pack, r.ascension_level, AVG(r.win) AS win_rate
      FROM presence p
      JOIN runs r USING (play_id)
      GROUP BY p.pack, r.ascension_level
    )
    SELECT
      po.pack                                         AS "Pack",
      (po.wins::DOUBLE / NULLIF(po.total,0))          AS "Overall Win Rate",
      MAX(CASE WHEN pa.ascension_level = 20 THEN pa.win_rate END) AS "A20",
      MAX(CASE WHEN pa.ascension_level = 19 THEN pa.win_rate END) AS "A19",
      MAX(CASE WHEN pa.ascension_level = 18 THEN pa.win_rate END) AS "A18",
      MAX(CASE WHEN pa.ascension_level = 17 THEN pa.win_rate END) AS "A17",
      MAX(CASE WHEN pa.ascension_level = 16 THEN pa.win_rate END) AS "A16",
      MAX(CASE WHEN pa.ascension_level = 15 THEN pa.win_rate END) AS "A15",
      MAX(CASE WHEN pa.ascension_level = 14 THEN pa.win_rate END) AS "A14",
      MAX(CASE WHEN pa.ascension_level = 13 THEN pa.win_rate END) AS "A13",
      MAX(CASE WHEN pa.ascension_level = 12 THEN pa.win_rate END) AS "A12",
      MAX(CASE WHEN pa.ascension_level = 11 THEN pa.win_rate END) AS "A11",
      MAX(CASE WHEN pa.ascension_level = 10 THEN pa.win_rate END) AS "A10",
      MAX(CASE WHEN pa.ascension_level =  9 THEN pa.win_rate END) AS "A9",
      MAX(CASE WHEN pa.ascension_level =  8 THEN pa.win_rate END) AS "A8",
      MAX(CASE WHEN pa.ascension_level =  7 THEN pa.win_rate END) AS "A7",
      MAX(CASE WHEN pa.ascension_level =  6 THEN pa.win_rate END) AS "A6",
      MAX(CASE WHEN pa.ascension_level =  5 THEN pa.win_rate END) AS "A5",
      MAX(CASE WHEN pa.ascension_level =  4 THEN pa.win_rate END) AS "A4",
      MAX(CASE WHEN pa.ascension_level =  3 THEN pa.win_rate END) AS "A3",
      MAX(CASE WHEN pa.ascension_level =  2 THEN pa.win_rate END) AS "A2",
      MAX(CASE WHEN pa.ascension_level =  1 THEN pa.win_rate END) AS "A1",
      MAX(CASE WHEN pa.ascension_level =  0 THEN pa.win_rate END) AS "A0"
    FROM pack_overall po
    LEFT JOIN pack_asc pa ON pa.pack = po.pack
    GROUP BY po.pack, po.wins, po.total
    ORDER BY "Overall Win Rate" DESC, "Pack"
    """
    return _con(db).execute(sql).df()

