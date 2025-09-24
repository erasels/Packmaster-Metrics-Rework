BASE_CTE = """
WITH base AS (
  SELECT
    event.play_id::VARCHAR                                   AS play_id,
    to_timestamp(time)                                       AS ts,
    ?::INT                                                   AS year,
    ?::INT                                                   AS month,
    host::VARCHAR                                            AS host,
    coalesce(event.victory::BOOLEAN, FALSE)                  AS victory,
    coalesce(event.ascension_level::INT, 0)                  AS ascension_level,
    event.character_chosen::VARCHAR                          AS character,
    event.currentPacks::VARCHAR                              AS current_packs_csv,
    event.pmversion::VARCHAR                                 AS pmversion,
    json_extract_string(event, '$.pickedHat')::VARCHAR       AS picked_hat,
    coalesce(event.enabledExpansionPacks::BOOLEAN, FALSE)    AS expansion_enabled,
    coalesce(event.playtime::INT, 0)                         AS playtime,
    coalesce(event.floor_reached::INT, 0)                    AS floor_reached,
    json_extract_string(event, '$.killed_by')::VARCHAR       AS killed_by,
    json_extract(event, '$.packChoices')                     AS packChoices,
    json_extract(event, '$.card_choices')                    AS card_choices,
    json_extract(event, '$.master_deck')                     AS master_deck
  FROM read_json_auto(?, format='newline_delimited', filename=true) AS src
  JOIN to_ingest AS files ON src.filename = files.path
)
"""

SQL_RUNS = """
SELECT
  play_id,
  ts,
  year,
  month,
  host,
  victory,
  ascension_level,
  character,
  pmversion,
  expansion_enabled,
  picked_hat,
  floor_reached,
  playtime,
  killed_by,
  COALESCE(json_array_length(master_deck), 0) AS master_deck_size
FROM base"""

SQL_MASTER_DECK = """
SELECT
  b.play_id,
  b.year,
  b.month,
  -- raw card string from the save (may include +N)
  json_extract_string(deck_val, '$')                    AS raw_card,
  -- normalized id without upgrade suffix
  REGEXP_REPLACE(json_extract_string(deck_val, '$'), '\\+\\d+$', '') AS card_id,
  -- upgrade level parsed from suffix (+N). default 0
  COALESCE(CAST(NULLIF(regexp_extract(json_extract_string(deck_val, '$'), '\\+(\\d+)$', 1), '') AS INT), 0) AS upgrade_level
FROM base b
, LATERAL UNNEST(CAST(json_extract(b.master_deck, '$') AS JSON[])) AS d(deck_val)
"""

SQL_PACKS_PRESENT = """
SELECT play_id, year, month, TRIM(pack) AS pack
FROM base,
LATERAL UNNEST(STR_SPLIT(current_packs_csv, ',')) AS t(pack)
WHERE NULLIF(TRIM(pack), '') IS NOT NULL
"""

SQL_PACK_CHOICES = """
-- one row for the picked pack
SELECT
  b.play_id,
  b.year,
  b.month,
  json_extract_string(pc_obj, '$.picked') AS picked_pack,
  NULL::VARCHAR                           AS not_picked_pack
FROM base b
, LATERAL unnest(CAST(json_extract(b.packChoices, '$') AS JSON[])) AS pc(pc_obj)

UNION ALL

SELECT
  b.play_id,
  b.year,
  b.month,
  NULL::VARCHAR                           AS picked_pack,
  json_extract_string(np_val, '$')        AS not_picked_pack
FROM base b
, LATERAL unnest(CAST(json_extract(b.packChoices, '$') AS JSON[])) AS pc(pc_obj)
, LATERAL unnest(CAST(json_extract(pc_obj, '$.not_picked') AS JSON[])) AS np(np_val)
"""


SQL_CARDS = """
-- picked rows (exclude non-card picks)
SELECT b.play_id, b.year, b.month, 'choice' AS context,
       REGEXP_REPLACE(json_extract_string(cc_obj, '$.picked'), '\\+\\d+$', '') AS card_id,
       TRUE AS picked
FROM base b
, LATERAL UNNEST(CAST(json_extract(b.card_choices, '$') AS JSON[])) AS cc(cc_obj)
WHERE UPPER(json_extract_string(cc_obj, '$.picked')) <> 'SKIP'
  AND json_extract_string(cc_obj, '$.picked') <> 'Singing Bowl'
UNION ALL
-- not-picked rows (extract strings, no quotes)
SELECT b.play_id, b.year, b.month, 'choice',
       REGEXP_REPLACE(json_extract_string(np_val, '$'), '\\+\\d+$', '') AS card_id,
       FALSE AS picked
FROM base b
, LATERAL UNNEST(CAST(json_extract(b.card_choices, '$') AS JSON[])) AS cc(cc_obj)
, LATERAL UNNEST(CAST(json_extract(cc_obj, '$.not_picked') AS JSON[])) AS np(np_val)
UNION ALL
-- final deck rows (extract strings, no quotes)
SELECT play_id, year, month, 'final',
       REGEXP_REPLACE(json_extract_string(deck_val, '$'), '\\+\\d+$', '') AS card_id,
       NULL AS picked
FROM base
, LATERAL UNNEST(CAST(json_extract(master_deck, '$') AS JSON[])) AS d(deck_val)
"""