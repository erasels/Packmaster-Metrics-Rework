import json
import sys
from pathlib import Path

def clean_file(path: Path) -> tuple[int, int]:
    """Rewrite path in place. Return (kept, skipped)."""
    tmp_path = path.with_suffix(".cleaning")
    kept = skipped = 0
    with open(path, "r", encoding="utf-8", errors="replace") as fin, \
         open(tmp_path, "w", encoding="utf-8", newline="\n") as fout:
        for line in fin:
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue
            fout.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False))
            fout.write("\n")
            kept += 1
    tmp_path.replace(path)
    return kept, skipped

def walk_metrics(root: Path):
    for p in sorted(root.rglob("*")):
        if p.is_file():
            yield p

def main(root: str):
    root = Path(root)
    total_kept = total_skipped = 0
    for f in walk_metrics(root):
        kept, skipped = clean_file(f)
        total_kept += kept
        total_skipped += skipped
        if skipped:
            print(f"{f}: kept {kept}, skipped {skipped}")
    print(f"Done. Total kept={total_kept}, skipped={total_skipped}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: metric_cleaner.py <metrics-root>")
        sys.exit(1)
    main(sys.argv[1])
