"""Compute total scraping progress across every category and signal whether
everything is done, for the GitHub Actions workflow to act on.

Run after the parallel per-category matrix jobs finish. Writes
complete=true/false plus the numbers to $GITHUB_OUTPUT so a later step can
decide whether to open a "you're done" notification issue -- without this,
the workflow just keeps running forever on its 6h schedule with no distinct
signal that the whole job is actually finished.
"""
import json
import os

RAW_DIR = os.path.join("cgtrader_deep", "raw")
DETAILS_DIR = "cgtrader_details"

CATEGORIES = [
    "award", "electronics", "household", "science", "space", "sport",
    "vehicle", "watercraft", "plant", "aircraft", "industrial", "food",
    "military", "car", "interior", "animal", "character", "architectural",
    "exterior",
]


def ids_in_jsonl(path):
    ids = set()
    if not os.path.exists(path):
        return ids
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                mid = json.loads(line).get("id")
            except json.JSONDecodeError:
                continue
            if mid is not None:
                ids.add(mid)
    return ids


def ids_in_shard_dir(d):
    if not os.path.isdir(d):
        return set()
    ids = set()
    for fn in os.listdir(d):
        if fn.startswith("part_") and fn.endswith(".jsonl"):
            ids |= ids_in_jsonl(os.path.join(d, fn))
    return ids


def main():
    total_target = total_done = 0
    lines = []
    for cat in CATEGORIES:
        target_ids = ids_in_jsonl(os.path.join(RAW_DIR, f"{cat}.jsonl"))
        done_ids = ids_in_shard_dir(os.path.join(DETAILS_DIR, f"{cat}_details"))
        t, d = len(target_ids), len(done_ids)
        total_target += t
        total_done += d
        pct = (d / t * 100) if t else 0
        lines.append(f"| {cat} | {d:,} | {t:,} | {pct:.1f}% |")
        print(f"  {cat:14s} {d:>8,} / {t:<8,}  ({pct:5.1f}%)")

    overall_pct = (total_done / total_target * 100) if total_target else 0
    print(f"\nOVERALL: {total_done:,} / {total_target:,} ({overall_pct:.2f}%)")

    complete = total_target > 0 and total_done >= total_target

    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(f"complete={'true' if complete else 'false'}\n")
            f.write(f"overall_pct={overall_pct:.2f}\n")
            f.write(f"total_done={total_done}\n")
            f.write(f"total_target={total_target}\n")
            f.write("table<<EOF\n")
            f.write("| category | done | target | % |\n|---|---|---|---|\n")
            f.write("\n".join(lines))
            f.write("\nEOF\n")

    if complete:
        print("\n=== ALL CATEGORIES COMPLETE ===")


if __name__ == "__main__":
    main()
