from __future__ import annotations
from pathlib import Path
import typer

from metrics_export.sheets.upload import update_summary_sheet, update_insights
from metrics_export.transforms import (
    win_rate_by_asc_insights,
    pack_pick_rate_insights,
    pack_win_rate_insights,
    card_pick_rate_insights,
    card_win_rate_insights,
    load_card_mappings, pack_asc_win_rate_insights,
)

app = typer.Typer(no_args_is_help=True)


def _print_insight(insight: dict):
    title, block = next(iter(insight.items()))
    headers = block["headers"]
    rows = block["data"]
    typer.echo(f"{title}: {len(rows)} rows")
    typer.echo(", ".join(headers))
    for r in rows[:10]:
        typer.echo(", ".join(map(str, r)))
    if len(rows) > 10:
        typer.echo("...")


@app.command()
def summary(
        db: Path = typer.Option(Path("warehouse/metrics.duckdb")),
        dry_run: bool = typer.Option(False),
):
    """Update the summary sheet."""
    if dry_run:
        typer.echo("[dry-run] summary")
        return
    update_summary_sheet()


@app.command()
def insight(
        kind: str = typer.Argument(..., help="win_by_asc | pack_pick | pack_win | card_pick | card_win | win_by_asc_and_pack"),
        db: Path = typer.Option(Path("data/warehouse/metrics.duckdb")),
        min_support: int = typer.Option(1, help="min rows threshold used by some insights"),
        mappings_dir: Path = typer.Option(Path("data"), help="dir for card→pack/rarity mappings"),
        include_overall: bool = typer.Option(True, help="only for win_by_asc"),
        dry_run: bool = typer.Option(False),
):
    """Compute one insight and push it via update_insights()."""
    pack_to_cards, card_to_pack, card_to_rarity = load_card_mappings(mappings_dir)

    if kind == "win_by_asc":
        ins = win_rate_by_asc_insights(db, min_support, include_overall)
    elif kind == "pack_pick":
        ins = pack_pick_rate_insights(db)
    elif kind == "pack_win":
        ins = pack_win_rate_insights(db, min_support)
    elif kind == "card_pick":
        ins = card_pick_rate_insights(db, card_to_pack, card_to_rarity, min_support)
    elif kind == "card_win":
        ins = card_win_rate_insights(db, card_to_pack, card_to_rarity, min_support)
    elif kind == "win_by_asc_and_pack":
        ins = pack_asc_win_rate_insights(db, min_support)
    else:
        raise typer.BadParameter("Unknown kind")

    if dry_run:
        _print_insight(ins)
        return
    update_insights(ins)


@app.command()
def all(
        db: Path = typer.Option(Path("data/warehouse/metrics.duckdb")),
        min_support: int = typer.Option(1),
        mappings_dir: Path = typer.Option(Path("data")),
        include_overall: bool = typer.Option(True),
        dry_run: bool = typer.Option(False),
):
    """Compute all insights and push each via update_insights()."""
    pack_to_cards, card_to_pack, card_to_rarity = load_card_mappings(mappings_dir)

    jobs = [
        win_rate_by_asc_insights(db, min_support, include_overall),
        pack_pick_rate_insights(db),
        pack_win_rate_insights(db, min_support),
        card_pick_rate_insights(db, card_to_pack, card_to_rarity, min_support),
        card_win_rate_insights(db, card_to_pack, card_to_rarity, min_support),
        pack_asc_win_rate_insights(db, min_support)
    ]

    for ins in jobs:
        if dry_run:
            _print_insight(ins)
        else:
            update_insights(ins)


if __name__ == "__main__":
    app()
