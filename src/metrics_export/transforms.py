import json
from pathlib import Path

import pandas as pd

from metrics_analytics.queries import win_rate_by_asc, pack_pick_rate, pack_win_rate, card_pick_rate, card_win_rate, pack_asc_win_rate, median_deck_size_by_asc, \
    expansion_rate


def _strip_modid_prefix(obj: str) -> str:
    return obj.replace("anniv5:", '')


def _strip_pack_suffix(pack: str) -> str:
    return pack.rsplit("Pack", 1)[0]  # Strip only last occurence


def _format_pack(pack: str) -> str:
    pack = _strip_pack_suffix(pack)
    pack = _strip_modid_prefix(pack)
    return pack


def load_card_mappings(data_dir: Path | str = "data"):
    data_dir = Path(data_dir)

    # packCards.json: { pack_id: [card1, card2, ...], ... }
    with open(data_dir / "packCards.json", "r", encoding="utf-8") as f:
        pack_to_cards: dict[str, list[str]] = json.load(f)

    # invert to { card: pack }
    card_to_pack: dict[str, str] = {}
    for pack, cards in pack_to_cards.items():
        for card in cards:
            card_to_pack[card] = pack

    # rarities.json: { card: rarity }
    with open(data_dir / "rarities.json", "r", encoding="utf-8") as f:
        card_to_rarity: dict[str, str] = json.load(f)

    return pack_to_cards, card_to_pack, card_to_rarity


def win_rate_by_asc_insights(db: Path, min_runs: int = 100, include_overall: bool = True) -> dict:
    df = win_rate_by_asc(db)  # cols: Ascension Level, Won, Total, Win Rate

    insights = {
        "Win Rate by Ascension Level": {
            "description": "Win rate for each ascension level",
            "headers": ["Ascension Level", "Won", "Total", "Win Rate"],
            "data": []
        }
    }

    # Adds sum of all ascs entry
    if include_overall:
        wins = int(df["Won"].sum())
        total = int(df["Total"].sum())
        win_rate = (wins / total) if total else 0.0
        insights["Win Rate by Ascension Level"]["data"].append(
            ["Overall", wins, total, f"{win_rate * 100:.2f}"]
        )

    for row in df.itertuples(index=False):
        asc, won, total, rate = row
        if total > min_runs:
            insights["Win Rate by Ascension Level"]["data"].append(
                [int(asc), int(won), int(total), f"{rate * 100:.2f}"]
            )

    return insights


def median_deck_size_by_asc_insights(db: Path, min_runs: int = 100) -> dict:
    df = median_deck_size_by_asc(db)  # cols: Ascension Level, Median Deck Size, Total Runs

    insights = {
        "Median Deck Sizes": {
            "description": "Median deck size of winning runs for each ascension level",
            "headers": ["Ascension Level", "Median Deck Size"],
            "data": []
        }
    }

    for row in df.itertuples(index=False):
        asc, median_sz, total = row
        if int(total) > min_runs:
            insights["Median Deck Sizes"]["data"].append(
                [int(asc), int(median_sz)]
            )

    return insights


def pack_pick_rate_insights(db: Path) -> dict:
    df = pack_pick_rate(db)  # cols: Pack, Picked, Seen, Pick Rate

    insights = {
        "Pack Pick Rate": {
            "description": "How often a pack is picked",
            "headers": ["Pack", "Picked", "Seen", "Pick Rate"],
            "data": []
        }
    }

    for pack, picked, seen, rate in df.itertuples(index=False, name=None):

        insights["Pack Pick Rate"]["data"].append(
            [_format_pack(pack), int(picked), int(seen), f"{float(rate) * 100:.2f}"]
        )

    return insights


def pack_win_rate_insights(db: Path, min_runs: int = 1) -> dict:
    df = pack_win_rate(db, min_runs)  # cols: Pack, Wins, Total, Win Rate

    insights = {
        "Pack Win Rate": {
            "description": "Win rate for each pack",
            "headers": ["Pack", "Wins", "Total", "Win Rate"],
            "data": []
        }
    }

    for pack, wins, total, rate in df.itertuples(index=False, name=None):
        insights["Pack Win Rate"]["data"].append(
            [_format_pack(pack), int(wins), int(total), f"{float(rate)*100:.2f}"]
        )

    return insights


def card_pick_rate_insights(
    db: Path,
    card_to_pack: dict[str, str],
    card_to_rarity: dict[str, str],
    min_seen: int = 1
) -> dict:
    df = card_pick_rate(db, min_seen)  # cols: Card, Picked, Seen, Pick Rate

    insights = {
        "Card Pick Rate": {
            "description": "How often a card is picked when offered as a card reward",
            "headers": ["Rarity", "Pack", "Card", "Picked", "Seen", "Pick Rate"],
            "data": []
        }
    }

    for card, picked, seen, rate in df.itertuples(index=False, name=None):
        rarity = card_to_rarity.get(card, "Unknown")
        pack = card_to_pack.get(card)

        # skip cards without a pack mapping
        if not pack:
            continue

        insights["Card Pick Rate"]["data"].append([
            rarity,
            _format_pack(pack),
            _strip_modid_prefix(card),
            int(picked),
            int(seen),
            f"{float(rate)*100:.2f}"
        ])

    return insights


def card_win_rate_insights(
    db: Path,
    card_to_pack: dict[str, str],
    card_to_rarity: dict[str, str],
    min_decks: int = 1
) -> dict:
    df = card_win_rate(db, min_decks)  # cols: Card, Wins, Total, Win Rate

    insights = {
        "Win Rate by Card": {
            "description": "Win rate for each card",
            "headers": ["Rarity", "Pack", "Card", "Wins", "Total", "Win Rate"],
            "data": []
        }
    }

    for card, wins, total, rate in df.itertuples(index=False, name=None):
        pack = card_to_pack.get(card)
        rarity = card_to_rarity.get(card, "Unknown")

        if not pack:
            continue

        insights["Win Rate by Card"]["data"].append([
            rarity,
            _format_pack(pack),
            _strip_modid_prefix(card),
            int(wins),
            int(total),
            f"{float(rate)*100:.2f}"
        ])

    return insights


def pack_asc_win_rate_insights(db: Path, min_runs: int = 1) -> dict:
    df = pack_asc_win_rate(db, min_runs=min_runs)

    headers = ["Pack", "Overall Win Rate"] + [f"A{lvl}" for lvl in range(20, -1, -1)]
    insights = {
        "Win Rate by Pack and Asc": {
            "description": "Pack win rates across ascension levels",
            "headers": headers,
            "data": []
        }
    }

    for _, row in df.iterrows():
        data_row = [
            _format_pack(row["Pack"]),
            (
                f"{float(row['Overall Win Rate'])*100:.2f}"
                if pd.notna(row.get("Overall Win Rate"))
                else "N/A"
            ),
        ]
        for lvl in range(20, -1, -1):
            val = row.get(f"A{lvl}")
            data_row.append(f"{float(val)*100:.2f}" if pd.notna(val) else "N/A")

        insights["Win Rate by Pack and Asc"]["data"].append(data_row)

    return insights


def expansion_rate_insights(db: Path) -> dict:
    df = expansion_rate(db)  # cols: Total Runs, With Expansion, Rate

    insights = {
        "Expansion Pack Usage": {
            "description": "Percentage of runs with expansion packs enabled",
            "headers": ["Total Runs", "With Expansion", "Rate"],
            "data": []
        }
    }

    if not df.empty:
        total = int(df.at[0, "Total Runs"])
        with_exp = int(df.at[0, "With Expansion"])
        rate_pct = (float(df.at[0, "Rate"]) * 100.0) if total else 0.0
        insights["Expansion Pack Usage"]["data"].append([total, with_exp, f"{rate_pct:.2f}"])

    return insights
