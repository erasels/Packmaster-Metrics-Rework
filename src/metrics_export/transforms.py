import json
from pathlib import Path

from metrics_analytics.queries import win_rate_by_asc, pack_pick_rate, pack_win_rate, card_pick_rate


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
