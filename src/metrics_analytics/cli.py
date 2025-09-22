import typer
from rich import print
from pathlib import Path
from rich.console import Console
from rich.table import Table
from .config import Config
from .ingest import ingest
from .queries import (
    win_rate_by_asc, pack_pick_rate, pack_win_rate, card_pick_rate, card_win_rate, pack_asc_win_rate, median_deck_size_by_asc
)

app = typer.Typer(no_args_is_help=True)


def show_df(df):
    table = Table(show_header=True, header_style="bold cyan")
    for col in df.columns:
        table.add_column(str(col))
    for row in df.itertuples(index=False):
        table.add_row(*[str(x) for x in row])
    Console().print(table)

@app.command()
def init(warehouse: Path = typer.Option(Path("warehouse"))):
    cfg = Config(warehouse_dir=warehouse)
    for p in cfg.parquet_paths.values():
        p.mkdir(parents=True, exist_ok=True)
    print(f"[green]Initialized at {warehouse}[/green]")


@app.command()
def load(
        metrics_root: Path = typer.Option(Path("metrics")),
        warehouse: Path = typer.Option(Path("warehouse"))
):
    cfg = Config(metrics_root=metrics_root, warehouse_dir=warehouse)
    n = ingest(cfg)
    print(f"[cyan]Ingested {n} file(s)[/cyan]")


@app.command()
def insight(kind: str,
            warehouse: Path = typer.Option(Path("warehouse")),
            min_support: int = 100):
    db = (warehouse / "metrics.duckdb")
    if kind == "win_by_asc":
        df = win_rate_by_asc(db)
    elif kind == "median_deck":
        df = median_deck_size_by_asc(db)
    elif kind == "pack_pick":
        df = pack_pick_rate(db)
    elif kind == "pack_win":
        df = pack_win_rate(db, min_support)
    elif kind == "card_pick":
        df = card_pick_rate(db, min_support)
    elif kind == "card_win":
        df = card_win_rate(db, min_support)
    elif kind == "win_by_asc_and_pack":
        df = pack_asc_win_rate(db, min_support)
    else:
        raise typer.BadParameter("Unknown kind")
    show_df(df)


if __name__ == "__main__":
    app()
