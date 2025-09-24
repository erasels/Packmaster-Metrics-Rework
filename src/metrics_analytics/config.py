from pydantic import BaseModel
from pathlib import Path


class Config(BaseModel):
    metrics_root: Path = Path("metrics")
    warehouse_dir: Path = Path("warehouse")
    dbfile: Path | None = None

    @property
    def duckdb_path(self) -> Path:
        return self.dbfile or (self.warehouse_dir / "metrics.duckdb")

    @property
    def parquet_paths(self) -> dict[str, Path]:
        w = self.warehouse_dir
        return {
            "runs": w / "runs_parquet",
            "master_deck": w / "master_deck_parquet",
            "packs_present": w / "packs_present_parquet",
            "pack_choices": w / "pack_choices_parquet",
            "cards": w / "cards_parquet",
        }
