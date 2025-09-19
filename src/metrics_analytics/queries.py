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
      f.card_id                            AS Card,
      CAST(SUM(r.win) AS INTEGER)          AS Wins,
      COUNT(*)                             AS Total,
      AVG(r.win)                           AS Win Rate
    FROM finals f
    JOIN runs r USING (play_id)
    GROUP BY f.card_id
    HAVING COUNT(*) >= {min_decks}
    ORDER BY win_rate DESC
    """
    return _con(db).execute(sql).df()

