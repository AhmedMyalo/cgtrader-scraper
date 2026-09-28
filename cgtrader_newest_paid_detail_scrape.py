"""
Detail-fetch for the 'newest'-sort discovery pass, PAID items only.

WHY THIS EXISTS (separate from cgtrader_detail_scrape.py)
-----------------------------------------------------------
Some categories (sport, award, ...) hit the 9,960-per-URL cap on their
'oldest' listing pass and got a second 'newest' pass to reach recent
uploads (see cgtrader_deep_scrape.py --sorts oldest,newest). The 'newest'
pass mixes in a lot of free models we don't want fetched, and the business
question driving this only cares about the paid market. This script reads
the SAME raw listing jsonl as the main pipeline, but narrows the target
list to: sort_used == 'newest' AND price_usd > 0 AND not already present
in an 'oldest' (or other) row for the same id (i.e. genuinely new, not a
duplicate the main pass would also reach).

It shares ShardedWriter / load_done / export_csv / fetch_detail with
cgtrader_detail_scrape.py, so its output lands in the exact same shard
directory and merges into the one CSV per category -- this is additive,
not a fork of the main pipeline.

USAGE
-----
    python cgtrader_newest_paid_detail_scrape.py --category sport
    python cgtrader_newest_paid_detail_scrape.py --category award --max-minutes 180
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cgtrader_scraper_v2 import (  # noqa: E402
    CGTraderSession, jittered_sleep, note_progress, start_stall_watchdog,
)
from cgtrader_detail_scrape import (  # noqa: E402
    fetch_detail, ShardedWriter, load_done, export_csv, _signal_category_complete,
)


def load_newest_paid_targets(raw_dir, category):
    path = os.path.join(raw_dir, f"{category}.jsonl")
    if not os.path.exists(path):
        raise SystemExit(f"No listing data at {path}\nRun cgtrader_deep_scrape.py first.")

    non_newest_ids = set()
    newest_rows = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                it = json.loads(line)
            except json.JSONDecodeError:
                continue
            mid = it.get("id")
            if not mid:
                continue
            if it.get("sort_used") == "newest":
                newest_rows[mid] = it  # last-seen wins, fine either way
            else:
                non_newest_ids.add(mid)

    targets = []
    for mid, it in newest_rows.items():
        if mid in non_newest_ids:
            continue  # also reachable via another sort -> not genuinely new
        if float(it.get("price_usd") or 0) <= 0:
            continue  # free -- out of scope for this pass
        url = it.get("url")
        if not url:
            continue
        targets.append({"id": mid, "url": url})
    return targets


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--category", required=True)
    ap.add_argument("--raw-dir", default="./cgtrader_deep/raw")
    ap.add_argument("--out-dir", default="./cgtrader_details")
    ap.add_argument("--delay", type=float, default=1.5)
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--max-stall-min", type=float, default=10)
    ap.add_argument("--max-minutes", type=float, default=None,
                     help="stop cleanly after this many minutes so the caller's "
                          "commit/export steps still run (same reasoning as in "
                          "cgtrader_detail_scrape.py)")
    args = ap.parse_args()

    os.makedirs(args.out_dir, exist_ok=True)
    stamp = f"{datetime.now(timezone.utc):%Y%m%d}"

    targets = load_newest_paid_targets(args.raw_dir, args.category)
    done = load_done(args.out_dir, args.category)
    todo = [t for t in targets if str(t["id"]) not in done]

    print(f"{args.category}: {len(targets)} paid newest-only targets, "
          f"{len(done)} already done overall, {len(todo)} to fetch")

    if not todo:
        _signal_category_complete(args.category)
        path, n = export_csv(args.out_dir, args.category, stamp)
        if path:
            print(f"csv: {n} models -> {path}")
        return

    start_stall_watchdog(int(args.max_stall_min * 60), label="newest-paid detail pass")
    sess = CGTraderSession(headless=not args.headed)
    print("Clearing the AWS WAF challenge...")
    sess.solve_challenge(args.category if args.category not in ("sport", "award") else "aircraft")

    writer = ShardedWriter(args.out_dir, args.category)

    ok = missing = failed = 0
    t0 = time.time()
    budget_s = args.max_minutes * 60 if args.max_minutes else None
    stopped_on_budget = False
    for i, t in enumerate(todo, 1):
        if budget_s and (time.time() - t0) >= budget_s:
            stopped_on_budget = True
            print(f"\n[budget] hit --max-minutes {args.max_minutes:g} after "
                  f"{i - 1} of {len(todo)} -- stopping cleanly. Re-run to resume.")
            break

        row = fetch_detail(sess, t["url"])
        note_progress()
        if row is None:
            failed += 1
            print(f"  [{i}/{len(todo)}] FAILED {t['url']}")
        elif row.get("__missing__"):
            missing += 1
            why = row.get("__reason__") or f"HTTP {row.get('__status__')}"
            placeholder = {"id": t["id"], "url": t["url"],
                            "title": f"[unavailable: {why}]",
                            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            writer.append(placeholder)
        else:
            writer.append(row)
            ok += 1
            if ok % 25 == 0 or ok == 1:
                rate = ok / max(time.time() - t0, 1) * 3600
                left = (len(todo) - i) / max(rate, 1)
                print(f"  [{i}/{len(todo)}] ok={ok} missing={missing} failed={failed} "
                      f"({rate:.0f}/h, ~{left:.1f}h left)")

        jittered_sleep(args.delay)

    print(f"\ndone: ok={ok} missing={missing} failed={failed}"
          f"{' (stopped on time budget)' if stopped_on_budget else ''}")
    if not stopped_on_budget:
        _signal_category_complete(args.category)
    path, n = export_csv(args.out_dir, args.category, stamp)
    if path:
        print(f"csv: {n} models -> {path}")


if __name__ == "__main__":
    main()
