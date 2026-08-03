"""
Deep-coverage CGTrader scrape -- works around the 9,960-per-URL pagination cap.

THE PROBLEM
-----------
Every CGTrader listing URL stops at 83 pages x 120 = 9,960 models, no matter how
big the category really is (`interior` reports 678,735 models but serves 83
pages; page 100 -> 404). So a single category URL can never yield more than
~10K rows. This is almost certainly why the original Octoparse tasks topped out
at "tens of thousands" -- they were per-subcategory, each with its own ceiling.

Filter query params can't be used to slice a category further: `polygons`,
`pricing`, `modelType` and every price-range spelling
(`price_min`/`min_price`/`price_from`/`budget`) are silently ignored -- the site
applies those client-side. Proof: all six disjoint `polygons` buckets returned
the *same* 52,057 total, summing to 312,342 for a 52,080-item category.

WHAT ACTUALLY WORKS
-------------------
Every subcategory and every tag is its own listing URL with its own 9,960
ceiling. `/3d-models/helicopter` -> 6,059 models in 51 pages, i.e. fully
reachable. So coverage comes from scraping MANY narrow URLs instead of a few
broad ones, then deduping globally by model id.

This script:
  1. builds a target list from the category tree the site ships in its nav props
     (19 requested categories + all their subcategories),
  2. checks each target's real size first and reports what fraction is reachable,
  3. scrapes each target with `sort_by=oldest` (stable pagination), optionally
     adding more sort orders for targets that hit the cap, since a different sort
     surfaces a different 9,960-item window,
  4. dedupes across every target into one master CSV per top-level category, plus
     an `all_models.csv`.

It reuses the proven fetch/retry/resume core from `cgtrader_scraper_v2.py` -- the
WAF challenge solving, cookie transplant, card parsing and per-page checkpointing
all behave identically here.

USAGE
-----
    python cgtrader_deep_scrape.py --plan              # build+show the target list, no scraping
    python cgtrader_deep_scrape.py                     # scrape every target, resumable
    python cgtrader_deep_scrape.py --sorts oldest,newest
    python cgtrader_deep_scrape.py --categories aircraft,car
    python cgtrader_deep_scrape.py --export            # rebuild CSVs from saved .jsonl

Interrupt any time with Ctrl+C; re-run the same command to resume.
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cgtrader_scraper_v2 import (  # noqa: E402
    BASE_URL,
    CATEGORIES,
    CSV_FIELDS,
    CGTraderSession,
    RunState,
    _append,
    _fetch_with_recovery,
    _soup,
    extract_listing_meta,
    is_challenge,
    jittered_sleep,
    note_progress,
    start_stall_watchdog,
)
import cgtrader_scraper_v2 as v2  # noqa: E402

PAGE_CAP = 83  # server-side hard limit; see module docstring
PER_PAGE = 120


class _Tee:
    """Write to the console and a log file at once.

    Done in Python on purpose: piping through `powershell Tee-Object` from a .bat
    needs a valid, quoted path, and building a timestamped name in cmd relies on
    `wmic`, which no longer ships with Windows 11. Both failure modes produced a
    filename containing ':' that PowerShell read as a drive letter.
    """

    def __init__(self, stream, path):
        self.stream = stream
        self.file = open(path, "a", encoding="utf-8", buffering=1)

    def write(self, data):
        self.stream.write(data)
        try:
            self.file.write(data)
        except Exception:
            pass  # a locked/full log must never break the scrape
        return len(data)

    def flush(self):
        self.stream.flush()
        try:
            self.file.flush()
        except Exception:
            pass

    def isatty(self):
        return getattr(self.stream, "isatty", lambda: False)()


# ---------------------------------------------------------------------------
# Target discovery
# ---------------------------------------------------------------------------

def harvest_tree(sess, slug="aircraft"):
    """Category -> subcategory tree with counts, from the nav React props."""
    for attempt in range(4):
        r = sess.session.get(f"{BASE_URL}/3d-models/{slug}", params={"page": 1}, timeout=60)
        if is_challenge(r.status_code, r.text):
            sess.solve_challenge(slug)
            continue
        soup = _soup(r.text)
        for el in soup.find_all(attrs={"data-react-props": True}):
            raw = el["data-react-props"]
            if "itemCategories" not in raw:
                continue
            try:
                props = json.loads(raw)
            except ValueError:
                continue
            nav = props.get("unifiedNavData", {}) or {}
            tree = {}
            for group in ("assetCategories", "printableCategories"):
                for cat in (nav.get(group, {}) or {}).get("itemCategories", []) or []:
                    if not cat.get("slug"):
                        continue
                    tree[cat["slug"]] = {
                        "title": cat.get("title"),
                        "navCount": cat.get("itemCount"),
                        "subcategories": [
                            {"slug": s.get("slug"), "title": s.get("title"),
                             "navCount": s.get("itemCount")}
                            for s in (cat.get("subcategories") or []) if s.get("slug")
                        ],
                    }
            if tree:
                return tree
        time.sleep(3)
    return {}


SOLVE_SLUG = "aircraft"  # a listing that reliably renders cards, for challenge solving


def measure(sess, slug, sort="oldest"):
    """Real size of one listing URL: (totalCount, totalPages), or (None, None) if we
    couldn't establish it. Never raises — one awkward slug must not abort the plan.
    """
    for attempt in range(4):
        try:
            r = sess.session.get(f"{BASE_URL}/3d-models/{slug}",
                                 params={"page": 1, "sort_by": sort}, timeout=60)
        except Exception:
            time.sleep(5)
            continue
        if r.status_code == 404:
            return 0, 0
        if is_challenge(r.status_code, r.text):
            try:
                sess.solve_challenge(SOLVE_SLUG)
            except Exception:
                pass
            time.sleep(3)
            continue
        meta = extract_listing_meta(r.text) or {}
        return meta.get("totalCount"), meta.get("totalPages")
    return None, None


def build_plan(sess, wanted, plan_path, refresh=False):
    """Target list: each requested category plus its subcategories, with real sizes.

    Measuring is checkpointed and RESUMABLE: a plan file may be partially measured
    (the run was interrupted), so anything without `measured: true` is measured now.
    Treating a partial plan as finished would silently skip those targets entirely.
    """
    targets = None
    if os.path.exists(plan_path) and not refresh:
        try:
            with open(plan_path, encoding="utf-8") as f:
                targets = json.load(f).get("targets")
        except (OSError, ValueError):
            targets = None

    if targets is None:
        print("Harvesting the category tree...")
        tree = harvest_tree(sess)
        if not tree:
            raise SystemExit("Could not read the category tree -- try again in a minute.")
        print(f"  {len(tree)} top-level categories in nav data")

        targets = []
        for cat in wanted:
            node = tree.get(cat)
            slugs = [(cat, cat)]
            if node:
                slugs += [(cat, s["slug"]) for s in node["subcategories"]]
            else:
                print(f"  [note] '{cat}' isn't in the nav tree (still scraped directly)")
            for parent, slug in slugs:
                if any(t["slug"] == slug for t in targets):
                    continue  # a slug can appear under two parents
                targets.append({"parent": parent, "slug": slug})

    pending = [t for t in targets if not t.get("measured")]
    if not pending:
        print(f"Plan already measured: {len(targets)} listing URLs")
        return {"targets": targets}
    print(f"\nMeasuring {len(pending)} of {len(targets)} listing URLs (1 request each)...")

    def save(partial):
        with open(plan_path, "w", encoding="utf-8") as f:
            json.dump({"built_at": datetime.now(timezone.utc).isoformat(),
                       "targets": partial}, f, indent=2, ensure_ascii=False)

    for i, t in enumerate(pending, 1):
        try:
            tc, tp = measure(sess, t["slug"])
        except Exception as exc:
            print(f"  [{i:3d}/{len(pending)}] {t['slug']:28s} measure failed "
                  f"({type(exc).__name__}) -- will use the cap")
            tc, tp = None, None
        t["totalCount"] = tc
        t["totalPages"] = tp
        # Unknown size: assume it's worth the full cap rather than skipping it.
        t["pagesToFetch"] = min(tp, PAGE_CAP) if tp else (PAGE_CAP if tc is None else 0)
        t["reachable"] = t["pagesToFetch"] * PER_PAGE
        t["capped"] = bool(tp and tp >= PAGE_CAP)
        t["measured"] = True
        print(f"  [{i:3d}/{len(pending)}] {t['slug']:28s} total={tc} pages={tp} "
              f"reachable~{t['reachable']}{'  (CAPPED)' if t['capped'] else ''}")
        save(targets)  # checkpoint, so an interruption here loses nothing
        jittered_sleep(1.5)

    save(targets)
    return {"targets": targets}


# ---------------------------------------------------------------------------
# Scraping a target
# ---------------------------------------------------------------------------

def scrape_target(sess, target, sort, state, out_dir):
    """One listing URL under one sort order, resumably. Returns rows added."""
    slug, parent = target["slug"], target["parent"]
    key = f"{slug}::{sort}"
    cs = state.cat(key)
    if cs["complete"]:
        return 0

    jsonl_path = os.path.join(out_dir, "raw", f"{parent}.jsonl")
    os.makedirs(os.path.dirname(jsonl_path), exist_ok=True)

    pages = target.get("pagesToFetch") or PAGE_CAP
    page_num = cs["last_page"] + 1
    added = 0

    if page_num > pages:
        cs["complete"] = True
        state.save()
        return 0

    print(f"  {slug} [{sort}] pages {page_num}..{pages}")
    while page_num <= pages:
        if v2._stop:
            break
        items, status, meta = _fetch_with_recovery(sess, slug, page_num, sort, "sort_by")

        if status == "end":
            cs["complete"] = True
            state.save()
            break
        if status == "failed":
            if page_num not in cs["failed_pages"]:
                cs["failed_pages"].append(page_num)
            cs["last_page"] = page_num
            state.save()
            page_num += 1
            jittered_sleep(v2.PAGE_DELAY)
            continue

        for it in items:
            it["source_slug"] = slug
            it["source_parent"] = parent
            it["sort_used"] = sort
        _append(jsonl_path, items)
        added += len(items)
        cs["count"] += len(items)
        cs["last_page"] = page_num
        state.save()
        note_progress()

        if page_num % 20 == 0:
            print(f"    page {page_num}/{pages} (+{cs['count']} from this target)")
        page_num += 1
        jittered_sleep(v2.PAGE_DELAY)
    else:
        cs["complete"] = True
        state.save()

    return added


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

EXPORT_FIELDS = CSV_FIELDS + ["source_slug", "source_parent", "sort_used"]


def export(out_dir, stamp):
    raw_dir = os.path.join(out_dir, "raw")
    if not os.path.isdir(raw_dir):
        print("Nothing to export yet.")
        return
    all_seen, all_rows = set(), []
    for fn in sorted(os.listdir(raw_dir)):
        if not fn.endswith(".jsonl"):
            continue
        parent = fn[:-6]
        seen, rows = set(), []
        with open(os.path.join(raw_dir, fn), encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    it = json.loads(line)
                except json.JSONDecodeError:
                    continue
                mid = it.get("id") or it.get("url")
                if mid in seen:
                    continue
                seen.add(mid)
                rows.append(it)
                if mid not in all_seen:
                    all_seen.add(mid)
                    all_rows.append(it)
        path = os.path.join(out_dir, f"{parent}_cgtrader_{stamp}.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=EXPORT_FIELDS, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)
        print(f"  {parent:16s} {len(rows):>8} unique -> {os.path.basename(path)}")

    master = os.path.join(out_dir, f"all_models_{stamp}.csv")
    with open(master, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=EXPORT_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(all_rows)
    print(f"  {'TOTAL':16s} {len(all_rows):>8} unique -> {os.path.basename(master)}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Deep CGTrader scrape (beats the 9,960 cap).")
    ap.add_argument("--categories", default=None, help="comma-separated subset of the 19")
    ap.add_argument("--out-dir", default="./cgtrader_deep")
    ap.add_argument("--sorts", default="oldest",
                    help="comma-separated sort orders. Extra sorts are only used for targets "
                         "that hit the 83-page cap, where a different sort exposes a different "
                         "window. e.g. oldest,newest")
    ap.add_argument("--delay", type=float, default=1.5, help="avg seconds between pages")
    ap.add_argument("--plan", action="store_true", help="build/show the target plan and exit")
    ap.add_argument("--refresh-plan", action="store_true", help="re-measure all targets")
    ap.add_argument("--export", action="store_true", help="rebuild CSVs from .jsonl and exit")
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--max-stall-min", type=float, default=10,
                    help="restart if no page advances for this many minutes "
                         "(0 disables). Guards against hung connections that "
                         "never raise, so retries alone cannot catch them.")
    ap.add_argument("--log", nargs="?", const="auto", default=None,
                    help="also append all output to a log file "
                         "(bare --log picks run_<timestamp>.log)")
    args = ap.parse_args()

    if args.log:
        log_path = args.log
        if log_path == "auto":
            log_path = f"run_{datetime.now():%Y%m%d_%H%M%S}.log"
        sys.stdout = _Tee(sys.stdout, log_path)
        sys.stderr = sys.stdout
        print(f"=== run started {datetime.now():%Y-%m-%d %H:%M:%S} -> {log_path} ===")

    start_stall_watchdog(int(args.max_stall_min * 60), label='listing pass')
    v2.PAGE_DELAY = args.delay
    os.makedirs(args.out_dir, exist_ok=True)
    stamp = f"{datetime.now(timezone.utc):%Y%m%d}"

    if args.export:
        export(args.out_dir, stamp)
        return

    wanted = ([c.strip() for c in args.categories.split(",") if c.strip()]
              if args.categories else list(CATEGORIES))
    sorts = [s.strip() for s in args.sorts.split(",") if s.strip()]

    import signal
    signal.signal(signal.SIGINT, v2._sigint)

    sess = CGTraderSession(headless=not args.headed)
    print("Clearing the AWS WAF challenge...")
    sess.solve_challenge(wanted[0])

    plan = build_plan(sess, wanted, os.path.join(args.out_dir, "plan.json"),
                      refresh=args.refresh_plan)
    targets = plan["targets"]

    total_reach = sum(t.get("reachable") or 0 for t in targets)
    total_real = sum(t.get("totalCount") or 0 for t in targets)
    capped = [t for t in targets if t.get("capped")]
    pages_total = sum(t.get("pagesToFetch") or 0 for t in targets)
    print(f"\nPlan: {len(targets)} listing URLs, {pages_total} pages to fetch")
    print(f"  reachable rows (with overlap): {total_reach:,}")
    print(f"  sum of reported sizes:          {total_real:,}")
    print(f"  targets pinned at the cap:       {len(capped)}"
          f"{' -> extra sorts will be used' if len(sorts) > 1 else ''}")
    est_h = pages_total * (args.delay + 1.2) / 3600
    print(f"  rough time for one sort pass:    ~{est_h:.1f} h")

    if args.plan:
        return

    state = RunState(os.path.join(args.out_dir, "state.json"))
    for i, t in enumerate(targets, 1):
        if v2._stop:
            break
        if t.get("totalCount") == 0 or not t.get("pagesToFetch"):
            continue  # genuinely empty/404 listing
        print(f"\n=== [{i}/{len(targets)}] {t['slug']} "
              f"(parent={t['parent']}, {t.get('totalCount')} models) ===")
        use_sorts = sorts if t.get("capped") else sorts[:1]
        for sort in use_sorts:
            if v2._stop:
                break
            try:
                scrape_target(sess, t, sort, state, args.out_dir)
            except Exception as exc:
                print(f"  [target error] {type(exc).__name__}: {exc} -- moving on")
                state.save()
        jittered_sleep(4)

    print("\n=== export ===")
    export(args.out_dir, stamp)
    if v2._stop:
        print("\nInterrupted -- re-run the same command to resume where it stopped.")


if __name__ == "__main__":
    main()
