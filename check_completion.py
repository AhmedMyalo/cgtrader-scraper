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
                # Normalised because the two sides disagree on type: the raw
                # listing files carry ids as strings, the detail shards as
                # ints. Only counts were ever compared here so the mismatch was
                # harmless, but the residue below needs a real set difference.
                ids.add(str(mid))
    return ids


def ids_in_shard_dir(d):
    if not os.path.isdir(d):
        return set()
    ids = set()
    for fn in os.listdir(d):
        if fn.startswith("part_") and fn.endswith(".jsonl"):
            ids |= ids_in_jsonl(os.path.join(d, fn))
    return ids


# If a handful of models can never be fetched AND never get recorded, strict
# equality below can never be satisfied and the "you're done" notification
# never fires -- the run just goes quiet forever. That is a real bug we already
# hit once (soft-404 redirects, see cgtrader_detail_scrape.py), and the cost of
# hitting a new variant of it is the user never learning the scrape finished.
# So allow a tiny absolute residue, and name the leftover ids in the issue
# rather than papering over them. Deliberately an absolute count, not a
# percentage: 0.1% of 774k would be ~774 models quietly dropped.
RESIDUE_TOLERANCE = 25


def main():
    total_target = total_done = 0
    lines = []
    residue = []
    for cat in CATEGORIES:
        target_ids = ids_in_jsonl(os.path.join(RAW_DIR, f"{cat}.jsonl"))
        done_ids = ids_in_shard_dir(os.path.join(DETAILS_DIR, f"{cat}_details"))
        t, d = len(target_ids), len(done_ids)
        total_target += t
        total_done += d
        pct = (d / t * 100) if t else 0
        lines.append(f"| {cat} | {d:,} | {t:,} | {pct:.1f}% |")
        print(f"  {cat:14s} {d:>8,} / {t:<8,}  ({pct:5.1f}%)")
        gap = target_ids - done_ids
        if gap and len(gap) <= RESIDUE_TOLERANCE:
            residue.append(f"{cat}: {', '.join(sorted(gap))}")

    overall_pct = (total_done / total_target * 100) if total_target else 0
    missing = total_target - total_done
    print(f"\nOVERALL: {total_done:,} / {total_target:,} ({overall_pct:.2f}%)"
          f"  missing={missing:,}")

    complete = total_target > 0 and missing <= RESIDUE_TOLERANCE
    if complete and missing:
        print(f"Treating as complete: only {missing} model(s) unaccounted for "
              f"(tolerance {RESIDUE_TOLERANCE}). They are named in the issue.")
        for r in residue:
            print(f"  residue -> {r}")

    gh_out = os.environ.get("GITHUB_OUTPUT")
    if gh_out:
        with open(gh_out, "a", encoding="utf-8") as f:
            f.write(f"complete={'true' if complete else 'false'}\n")
            f.write(f"overall_pct={overall_pct:.2f}\n")
            f.write(f"total_done={total_done}\n")
            f.write(f"total_target={total_target}\n")
            f.write(f"residue={'; '.join(residue)}\n")
            f.write("table<<EOF\n")
            f.write("| category | done | target | % |\n|---|---|---|---|\n")
            f.write("\n".join(lines))
            f.write("\nEOF\n")

    # Second, independent channel for the same news. Actions emails on workflow
    # FAILURE but never on success, so the completion issue is otherwise the
    # only positive signal -- and if that one step ever breaks, the scrape
    # finishes in total silence. The run summary costs nothing and can't fail.
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(f"## {'DONE - scraping complete' if complete else 'Still scraping'}\n\n")
            f.write(f"**{total_done:,} / {total_target:,}** models ({overall_pct:.2f}%)\n\n")
            f.write("| category | done | target | % |\n|---|---|---|---|\n")
            f.write("\n".join(lines) + "\n")
            if residue:
                f.write(f"\nPermanently unavailable: {'; '.join(residue)}\n")

    if complete:
        print("\n=== ALL CATEGORIES COMPLETE ===")


if __name__ == "__main__":
    main()
