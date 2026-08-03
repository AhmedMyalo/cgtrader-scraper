"""
Orchestrator for the GitHub Actions cron job.

Works through every CGTrader category in priority order (listing pass, then
per-model detail pass), respecting a wall-clock time budget so it ALWAYS
stops cleanly well inside GitHub's 6-hour hard per-job limit. The next
scheduled run picks up exactly where this one left off -- the underlying
scrapers are already fully resumable via their jsonl/state files, which this
script does nothing to bypass, it just decides how long each gets to run
before moving on / stopping.

Each phase is invoked as a SEPARATE PROCESS (not an in-process function call),
so a hard per-command timeout can kill it cleanly if the budget runs out
mid-category. That's a SIGKILL, not a graceful shutdown -- but every write in
both scrapers is either an atomic temp-file-then-rename, or a single
open/write/close append, and every reader already tolerates a torn last
line (json.JSONDecodeError -> skip). So a hard kill mid-write loses at most
one in-flight row, never corrupts anything already on disk.

USAGE (normally only invoked by the workflow, but works standalone too):
    python run_pipeline.py
"""
import os
import subprocess
import sys
import time

TOTAL_BUDGET_SECONDS = int(5.25 * 3600)   # 5h15m of work inside a 6h job,
# leaving ~45min headroom for checkout/setup/export/commit steps around it.
LISTING_PHASE_CAP_SECONDS = int(1.5 * 3600)  # cap phase 1 so an incomplete
# listing pass can never starve phase 2 (the actual per-model data, which is
# the point of this whole pipeline) of every run's entire time budget.

# Ascending by known listing size, so small categories finish -- and show up
# as usable output -- quickly, instead of the whole budget disappearing into
# the single largest category for weeks before anything else gets touched.
# The categories whose listing pass never finished locally (science, space,
# sport, vehicle, watercraft) are placed early so their real sizes get
# discovered soon rather than last.
CATEGORY_ORDER = [
    "electronics", "household", "science", "space", "sport", "vehicle",
    "watercraft", "plant", "aircraft", "industrial", "food", "military",
    "car", "interior", "animal", "character", "architectural", "exterior",
]
# "award" is deliberately excluded here -- already fully scraped (9,960/9,960).


def run(cmd, budget_seconds):
    """Run one subprocess, hard-capped at budget_seconds. Returns False if the
    budget ran out mid-command (caller should stop dispatching further work
    this cycle) or True if the command finished on its own within budget.
    """
    if budget_seconds <= 30:
        return False
    print(f"\n$ {' '.join(cmd)}  (budget: {budget_seconds/60:.0f} min)", flush=True)
    try:
        subprocess.run(cmd, timeout=budget_seconds, check=False)
    except subprocess.TimeoutExpired:
        print("[pipeline] time budget hit mid-command -- stopping here "
              "(safe to resume next scheduled run)", flush=True)
        return False
    return True


def main():
    start = time.time()

    def remaining():
        return TOTAL_BUDGET_SECONDS - (time.time() - start)

    print("=== phase 1: listing pass (catches up any incomplete/missing "
          f"categories, capped at {LISTING_PHASE_CAP_SECONDS/60:.0f} min) ===",
          flush=True)
    phase1_budget = min(remaining(), LISTING_PHASE_CAP_SECONDS)
    if phase1_budget > 60:
        run([sys.executable, "cgtrader_deep_scrape.py",
             "--out-dir", "./cgtrader_deep", "--sorts", "oldest",
             "--delay", "1.5"], phase1_budget)

    print("\n=== phase 2: per-model detail pass, one category at a time ===",
          flush=True)
    for cat in CATEGORY_ORDER:
        if remaining() <= 60:
            print("[pipeline] out of time budget -- stopping for this run",
                  flush=True)
            break
        raw_path = os.path.join("cgtrader_deep", "raw", f"{cat}.jsonl")
        if not os.path.exists(raw_path):
            print(f"[pipeline] skipping {cat}: listing not scraped yet "
                  "(phase 1 hasn't reached it)", flush=True)
            continue
        ok = run([sys.executable, "cgtrader_detail_scrape.py",
                  "--category", cat, "--order", "price_desc",
                  "--delay", "1.5"], remaining())
        if not ok:
            break

    print(f"\n=== pipeline run finished in {(time.time() - start) / 60:.1f} min ===",
          flush=True)


if __name__ == "__main__":
    main()
