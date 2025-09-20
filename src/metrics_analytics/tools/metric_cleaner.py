import json
import sys
from pathlib import Path

STATE_PATH = Path(__file__).with_name(Path(__file__).stem + ".state.json")


def load_state() -> dict:
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data if isinstance(data, dict) else {}
    except FileNotFoundError:
        return {}
    except Exception:
        # Corrupt or unreadable state; start fresh.
        return {}


def save_state(state: dict) -> None:
    tmp = STATE_PATH.with_suffix(".state.json.tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        json.dump(state, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")
    tmp.replace(STATE_PATH)


def should_skip(path: Path, state: dict) -> bool:
    key = str(path.resolve())
    try:
        cur_mtime = path.stat().st_mtime_ns
    except FileNotFoundError:
        return True
    last = state.get(key)
    return isinstance(last, int) and last == cur_mtime


def mark_cleaned(path: Path, state: dict) -> None:
    try:
        state[str(path.resolve())] = path.stat().st_mtime_ns
    except FileNotFoundError:
        pass


def clean_file(path: Path) -> tuple[int, int]:
    """Rewrite path in place. Return (kept, skipped)."""
    tmp_path = path.with_suffix(path.suffix + ".cleaning")
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


def walk_metrics(root: Path, exclude: set[Path]):
    for p in sorted(root.rglob("*")):
        if p.is_file() and p not in exclude and not p.name.endswith(".cleaning"):
            yield p


def main(root: str):
    root = Path(root)
    state = load_state()

    # Exclude the state file if it falls under the root.
    exclude = {STATE_PATH.resolve()}

    total_kept = total_skipped = 0
    total_skipped_due_to_state = 0

    for f in walk_metrics(root, exclude):
        if should_skip(f, state):
            total_skipped_due_to_state += 1
            continue

        kept, skipped = clean_file(f)
        total_kept += kept
        total_skipped += skipped
        mark_cleaned(f, state)
        if skipped:
            print(f"{f}: kept {kept}, skipped {skipped}")

    save_state(state)
    print(
        f"Done. Total kept={total_kept}, skipped_bad_lines={total_skipped}, "
        f"skipped_already_clean={total_skipped_due_to_state}"
    )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: metric_cleaner.py <metrics-root>")
        sys.exit(1)
    main(sys.argv[1])
