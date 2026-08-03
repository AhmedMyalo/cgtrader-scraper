"""
Orchestrator for the GitHub Actions cron job.

Three phases, in order, each bounded so the whole run always stops cleanly
well inside GitHub's 6-hour hard per-job limit:

  1. LISTING catch-up  -- finish any category whose page-listing pass never
     completed locally (science/space/sport/vehicle/watercraft).
  2. QUICK SAMPLE       -- a small --limit fetch (default 30 models) of EVERY
     category's per-model detail data. This exists specifically so the very
     first run produces a CSV you can actually look at within about an hour,
     covering all 19 categories, instead of only "award" plus a fraction of
     whichever category happens to be first in the full-scale queue. Without
     this, quality problems in category #7's data wouldn't surface until
     categories #1-6 had each taken hours to fully finish -- exactly the
     "found out after everything ran" failure mode this phase avoids.
  3. FULL SCALE         -- the real, complete per-model scrape, category by
     category in priority order, for whatever budget remains.

A CSV is re-exported after EVERY category step in every phase (not just on
natural completion), so the artifact this run uploads always reflects
whatever has actually been scraped so far -- including a category that got
cut off mid-way by the time budget. Before this, a category killed mid-run
by the subprocess timeout never got its export_csv() call at all (that only
ran after main()'s loop finished normally), so a run that got interrupted
produced NO reviewable output for that category, no matter how much of it
had actually been scraped and safely written to the sharded jsonl files.

Each phase invokes the underlying scrapers as SEPARATE PROCESSES so a hard
per-command timeout can kill one cleanly if the budget runs out mid-category.
That's a SIGKILL, not a graceful shutdown -- but every write in both scrapers
is either an atomic temp-file-then-rename or a single open/write/close
append, and every reader already tolerates a torn last line
(json.JSONDecodeError -> skip). A hard kill mid-write loses at most one
in-flight row, never corrupts anything already on disk.

USAGE (normally only invoked by the workflow, but works standalone too):
    python run_pipeline.py
"""
import json
import os
import subprocess
import sys
import time

TOTAL_BUDGET_SECONDS = int(5.25 * 3600)      # 5h15m inside a 6h job, leaving
# ~45min headroom for checkout/setup/commit steps around this script.
LISTING_PHASE_CAP_SECONDS = int(1.5 * 3600)   # phase 1 cap
SAMPLE_PHASE_CAP_SECONDS = int(0.75 * 3600)   # phase 2 cap (~45 min)
SAMPLE_SIZE = 30                              # models per category in phase 2

# Ascending by known listing size for phase 3, so small categories finish --
# and stay visible in the output -- quickly, instead of the whole remaining
# budget disappearing into the single largest category for weeks. Categories
# whose listing pass never finished locally (science/space/sport/vehicle/
# watercraft) are placed early so their real sizes get discovered soon.
CATEGORY_ORDER = [
    "electronics", "household", "science", "space", "sport", "vehicle",
    "watercraft", "plant", "aircraft", "industrial", "food", "military",
    "car", "interior", "animal", "character", "architectural", "exterior",
]
# "award" is deliberately excluded -- already fully scraped (9,960/9,960).

RAW_DIR = os.path.join("cgtrader_deep", "raw")
DETAILS_DIR = "cgtrader_details"


def run(cmd, budget_seconds, label=""):
    """Run one subprocess, hard-capped at budget_seconds. Returns False if the
    budget ran out mid-command, True otherwise (finished on its own).
    """
    if budget_seconds <= 15:
        return False
    print(f"\n$ {' '.join(cmd)}  (budget: {budget_seconds / 60:.0f} min)"
          f"{f' [{label}]' if label else ''}", flush=True)
    try:
        subprocess.run(cmd, timeout=budget_seconds, check=False)
    except subprocess.TimeoutExpired:
        print("[pipeline] time budget hit mid-command -- stopping here "
              "(safe to resume next scheduled run)", flush=True)
        return False
    return True


def export_csv(category, remaining_budget):
    """Cheap, local, no network -- always safe to call regardless of how the
    scraping step just ended, so the artifact never lags behind what's
    actually been scraped and saved.
    """
    if remaining_budget <= 5:
        return
    run([sys.executable, "cgtrader_detail_scrape.py",
         "--category", category, "--export"], min(remaining_budget, 120),
        label="export")


def has_listing(category):
    return os.path.exists(os.path.join(RAW_DIR, f"{category}.jsonl"))


def category_progress(category):
    """(done_count, total_count) for a category's detail scrape, or (0, 0) if
    the listing pass hasn't reached it yet. Cheap: reads small JSON files.
    """
    if not has_listing(category):
        return 0, 0
    seen = set()
    with open(os.path.join(RAW_DIR, f"{category}.jsonl"), encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                mid = json.loads(line).get("id")
            except json.JSONDecodeError:
                continue
            if mid:
                seen.add(mid)
    total = len(seen)

    shard_dir = os.path.join(DETAILS_DIR, f"{category}_details")
    done = 0
    if os.path.isdir(shard_dir):
        ids = set()
        for fn in os.listdir(shard_dir):
            if not (fn.startswith("part_") and fn.endswith(".jsonl")):
                continue
            with open(os.path.join(shard_dir, fn), encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        mid = json.loads(line).get("id")
                    except json.JSONDecodeError:
                        continue
                    if mid:
                        ids.add(mid)
        done = len(ids)
    return done, total


def main():
    start = time.time()

    def remaining():
        return TOTAL_BUDGET_SECONDS - (time.time() - start)

    print("=== phase 1: listing catch-up "
          f"(capped {LISTING_PHASE_CAP_SECONDS / 60:.0f} min) ===", flush=True)
    budget = min(remaining(), LISTING_PHASE_CAP_SECONDS)
    if budget > 60:
        run([sys.executable, "cgtrader_deep_scrape.py",
             "--out-dir", "./cgtrader_deep", "--sorts", "oldest",
             "--delay", "1.5"], budget, label="listing")

    print(f"\n=== phase 2: quick {SAMPLE_SIZE}-model sample of every category "
          f"(capped {SAMPLE_PHASE_CAP_SECONDS / 60:.0f} min) -- so the very "
          "first run gives you something to review across ALL categories, "
          "not just whichever one is first in line ===", flush=True)
    phase2_deadline = time.time() + min(remaining(), SAMPLE_PHASE_CAP_SECONDS)
    for cat in CATEGORY_ORDER:
        left = phase2_deadline - time.time()
        if left <= 30 or remaining() <= 60:
            print("[pipeline] sample-phase budget used up", flush=True)
            break
        if not has_listing(cat):
            print(f"[pipeline] skipping {cat} sample: no listing data yet",
                  flush=True)
            continue
        run([sys.executable, "cgtrader_detail_scrape.py",
             "--category", cat, "--order", "price_desc",
             "--limit", str(SAMPLE_SIZE), "--delay", "1.5"], left,
            label="sample")
        export_csv(cat, remaining())

    print("\n=== phase 3: full-scale detail pass, one category at a time ===",
          flush=True)
    for cat in CATEGORY_ORDER:
        if remaining() <= 60:
            print("[pipeline] out of time budget -- stopping for this run",
                  flush=True)
            break
        if not has_listing(cat):
            print(f"[pipeline] skipping {cat}: listing not scraped yet",
                  flush=True)
            continue
        ok = run([sys.executable, "cgtrader_detail_scrape.py",
                  "--category", cat, "--order", "price_desc",
                  "--delay", "1.5"], remaining(), label="full")
        export_csv(cat, remaining())
        if not ok:
            break

    print("\n=== progress summary ===", flush=True)
    for cat in ["award"] + CATEGORY_ORDER:
        if cat == "award":
            print(f"  {cat:14s} complete (9,960/9,960)", flush=True)
            continue
        done, total = category_progress(cat)
        if total:
            print(f"  {cat:14s} {done:>6}/{total:<6} detail rows", flush=True)
        elif has_listing(cat):
            print(f"  {cat:14s} listed, no detail rows yet", flush=True)
        else:
            print(f"  {cat:14s} not listed yet", flush=True)

    print(f"\n=== pipeline run finished in {(time.time() - start) / 60:.1f} min ===",
          flush=True)


if __name__ == "__main__":
    main()
