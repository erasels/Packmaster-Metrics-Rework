from pathlib import Path

from metrics_analytics.queries import win_rate_by_asc, pack_pick_rate


def _strip_modid_prefix(obj: str) -> str:
    return obj.replace("anniv5:", '')


def _strip_pack_suffix(pack: str) -> str:
    return pack.rsplit("Pack", 1)[0]  # Strip only last occurence


def _format_pack(pack: str) -> str:
    pack = _strip_pack_suffix(pack)
    pack = _strip_modid_prefix(pack)
    return pack


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
