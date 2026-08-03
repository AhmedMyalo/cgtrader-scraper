"""
CGTrader category scraper — the version that works. Replaces the 19 Octoparse tasks.

WHY THE OLD SCRAPER BROKE (don't re-diagnose this from scratch)
--------------------------------------------------------------
Two separate things were misread as "our IP got blocked":

1. CGTrader sits behind **AWS WAF Bot Control**. A client that can't run
   JavaScript gets `HTTP 202` + a ~2.4 KB challenge page (`window.gokuProps`,
   `AwsWafIntegration.getToken()`) instead of content. No error status, so it
   looks like a silent throttle. A real browser clears it in ~5s and gets an
   `aws-waf-token` cookie.
2. The old `Accept: application/json` trick **no longer returns JSON at all**.
   CGTrader now answers that request with HTML. The old code treated "not JSON"
   as proof of a block, so it retried, backed off, tripped its circuit breaker
   and reported "blocked" — while the server was happily serving the real page.

Verified 2026-08-01: waiting does not help, changing IP does not help, Colab does
not help. Octoparse worked all along because it drives a real browser.

HOW THIS WORKS
--------------
* Playwright opens Chromium **once** and waits for a real product card to appear,
  which is the unambiguous signal that the WAF challenge cleared (~5s).
* Those cookies are copied into a plain `requests.Session`, which then fetches
  listing pages directly — no rendering per page, so it's fast. Confirmed to
  return all 148 cards per page.
* Products are read out of the server-rendered HTML cards
  (`article[data-model-id]`), which carry id, title, url, price, file formats,
  type flags and image. Category/subcategory come from the product URL path.
* A page past the end returns **404** — a clean end-of-category signal.
* If a fetch ever comes back as a challenge again, the browser re-solves it,
  cookies are refreshed, and the same page is retried. The run does not abort.

RESILIENCE (why a multi-hour run won't die)
------------------------------------------
* Challenge mid-run        -> re-solve, refresh cookies, retry the same page.
* Transient / 5xx / network -> capped exponential backoff, several attempts.
* A page that still fails   -> recorded in state.json and SKIPPED, so one bad
                               page can't kill a 200-page category. Sweep them
                               up later with --retry-failed.
* Crash / Ctrl+C / sleep    -> every page is appended to <category>.jsonl and the
                               page number committed to state.json atomically,
                               so re-running resumes at the exact next page.
* Finished categories       -> skipped on re-run (--force to override).
* CSVs are rebuilt from the .jsonl with dedupe on id, so partial data is always
  exportable (--csv-only, safe to run any time, even mid-scrape).

USAGE
-----
    pip install playwright requests beautifulsoup4 lxml
    playwright install chromium

    python cgtrader_scraper_v2.py --probe            # health check, one page
    python cgtrader_scraper_v2.py --counts           # how big is each category?
    python cgtrader_scraper_v2.py                    # all 19, full, resumable
    python cgtrader_scraper_v2.py --categories award,car
    python cgtrader_scraper_v2.py --max-pages 3      # smoke test
    python cgtrader_scraper_v2.py --retry-failed     # re-attempt skipped pages
    python cgtrader_scraper_v2.py --csv-only         # rebuild CSVs from .jsonl

Re-running the same command after any interruption is always safe.
"""

import argparse
import csv
import json
import os
import random
import re
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.cgtrader.com"
CARD_SELECTOR = "article[data-model-id]"

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

HTML_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": f"{BASE_URL}/3d-models",
    "Upgrade-Insecure-Requests": "1",
}

CATEGORIES = {
    "aircraft": "aircraft",
    "animal": "animal",
    "architectural": "architectural",
    "award": "award",
    "car": "car",
    "character": "character",
    "electronics": "electronics",
    "exterior": "exterior",
    "food": "food",
    "household": "household",
    "industrial": "industrial",
    "interior": "interior",
    "military": "military",
    "plant": "plant",
    "science": "science",
    "space": "space",
    "sport": "sport",
    "vehicle": "vehicle",
    "watercraft": "watercraft",
}

CSV_FIELDS = [
    "id", "title", "url", "price_usd", "price_text", "category", "subcategory",
    "formats", "is_pbr", "is_low_poly", "is_rigged", "is_animated",
    "is_print_ready", "type_tags", "image_url", "source_category", "page",
]

# Politeness / resilience.
PAGE_DELAY = 1.5
CATEGORY_COOLDOWN = 12.0
MAX_ATTEMPTS_PER_PAGE = 6
BACKOFF_SECONDS = [5, 15, 30, 60, 120, 240]

_stop = False


def _sigint(signum, frame):
    global _stop
    if _stop:
        print("\n[force quit]")
        sys.exit(130)
    _stop = True
    print("\n[finishing the current page, then stopping — progress is saved. "
          "Ctrl+C again to quit now]")


def jittered_sleep(base):
    time.sleep(random.uniform(base * 0.6, base * 1.6))


# ---------------------------------------------------------------------------
# Stall watchdog
# ---------------------------------------------------------------------------
# Retries and backoff only help when something *raises*. A socket that accepts
# the connection and then goes quiet does not raise -- `requests`' timeout is
# per-read, so a trickling or half-dead connection can hang indefinitely. That
# happened on 2026-08-01: the process stayed alive with no output for 20+ minutes,
# so the .bat restart loop never fired either (it only reacts to an exit).
#
# So: a daemon thread watches a progress timestamp and hard-exits the process if
# nothing advances. Every page is already committed to disk before we move on, so
# a hard exit loses at most the page in flight, and the wrapper script restarts
# and resumes.

_last_progress = time.time()
_watchdog_started = False


def note_progress():
    global _last_progress
    _last_progress = time.time()


def start_stall_watchdog(max_stall_seconds=600, label="run"):
    global _watchdog_started
    if _watchdog_started or not max_stall_seconds:
        return
    _watchdog_started = True

    def _watch():
        while True:
            time.sleep(20)
            stalled = time.time() - _last_progress
            if stalled > max_stall_seconds:
                print(f"\n[watchdog] no progress for {stalled/60:.1f} min -- the "
                      f"connection is hung, not slow. Exiting so the wrapper can "
                      f"restart and resume ({label}).", flush=True)
                os._exit(3)  # bypass atexit/threads; state is already on disk

    threading.Thread(target=_watch, daemon=True, name="stall-watchdog").start()
    print(f"[watchdog] armed: will restart if nothing advances for "
          f"{max_stall_seconds // 60} min")


def _soup(html):
    """lxml when available (much faster on these ~1.5 MB pages), else stdlib."""
    try:
        return BeautifulSoup(html, "lxml")
    except Exception:
        return BeautifulSoup(html, "html.parser")


# ---------------------------------------------------------------------------
# Card parsing
# ---------------------------------------------------------------------------

_PRICE_RE = re.compile(r"([\d,]+(?:\.\d+)?)")

TYPE_FLAG_MAP = {
    "pbr": "is_pbr",
    "l-poly": "is_low_poly",
    "low-poly": "is_low_poly",
    "rigged": "is_rigged",
    "animated": "is_animated",
    "print-ready": "is_print_ready",
    "printable": "is_print_ready",
}


def _text(el):
    return el.get_text(strip=True) if el else ""


def extract_listing_meta(html):
    """Pull the listing's own pagination info out of the page's React props.

    The page ships `{"totalCount": 10503, "currentPage": 1, "totalPages": 83,
    "perPage": 120}` plus `sortByValue`, which lets us report real progress and
    confirm the sort we asked for was actually applied.
    """
    soup = _soup(html)
    for el in soup.find_all(attrs={"data-react-props": True}):
        raw = el["data-react-props"]
        if "totalPages" not in raw:
            continue
        try:
            props = json.loads(raw)
        except (ValueError, TypeError):
            continue
        meta = {"sortByValue": props.get("sortByValue")}
        stack = [props]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                if "totalPages" in node and "perPage" in node:
                    meta.update({k: node.get(k) for k in
                                 ("totalCount", "totalPages", "perPage", "currentPage")})
                    return meta
                stack.extend(node.values())
            elif isinstance(node, list):
                stack.extend(node[:5])
        return meta
    return {}


def _is_recommendation_card(art):
    """The listing grid sits in a plain .col-12; a second .col-12.cgt-mt-20 block
    holds 28 promoted/recommended cards that are IDENTICAL on every page. Those
    would pollute the category with foreign models, so drop them.
    """
    anc = art.find_parent("div", class_="col-12")
    return bool(anc) and "cgt-mt-20" in (anc.get("class") or [])


def parse_cards(html, source_category, page_num):
    """Extract the real listing cards from one page (excluding recommendations)."""
    soup = _soup(html)
    items = []

    articles = soup.find_all("article", attrs={"data-model-id": True})
    grid = [a for a in articles if not _is_recommendation_card(a)]
    # If CGTrader renames those classes, fall back to everything rather than
    # silently returning zero and declaring the category finished.
    if articles and not grid:
        grid = articles

    for art in grid:
        link = art.select_one("a.cgt-model-card__link") or art.find("a", href=True)
        url = link.get("href") if link else None

        # Category/subcategory live in the product URL:
        # /3d-models/<category>/<subcategory>/<slug>
        category = subcategory = None
        if url:
            parts = [p for p in urlparse(url).path.split("/") if p]
            if len(parts) >= 4 and parts[0] == "3d-models":
                category, subcategory = parts[1], parts[2]

        title = _text(art.select_one(".card-3d-model__footer-title a"))
        if not title:
            img = art.find("img", alt=True)
            title = (img["alt"] or "").replace("3D asset ", "").strip() if img else ""

        price_text = _text(art.select_one(".card-3d-model__price .cgt-tag__text"))
        price_usd = None
        if price_text:
            if "free" in price_text.lower():
                price_usd = 0.0
            else:
                m = _PRICE_RE.search(price_text)
                if m:
                    try:
                        price_usd = float(m.group(1).replace(",", ""))
                    except ValueError:
                        pass

        formats = [_text(b) for b in
                   art.select(".card-3d-model__format-tags .cgt-badge__content")]
        type_tags = [_text(b) for b in
                     art.select(".card-3d-model__type-tags .cgt-badge__content")]

        row = {
            "id": art.get("data-model-id"),
            "title": title,
            "url": url,
            "price_usd": price_usd,
            "price_text": price_text,
            "category": category,
            "subcategory": subcategory,
            "formats": ", ".join(f for f in formats if f),
            "is_pbr": False,
            "is_low_poly": False,
            "is_rigged": False,
            "is_animated": False,
            "is_print_ready": False,
            "type_tags": ", ".join(t for t in type_tags if t),
            "image_url": None,
            "source_category": source_category,
            "page": page_num,
        }

        for tag in type_tags:
            key = TYPE_FLAG_MAP.get(tag.strip().lower())
            if key:
                row[key] = True

        img = art.find("img", src=True)
        if img:
            row["image_url"] = img["src"]

        items.append(row)

    return items


def is_challenge(status, body):
    if status == 202:
        return True
    head = body[:4000]
    return "gokuProps" in head or "AwsWafIntegration" in head or "awswaf" in head


# ---------------------------------------------------------------------------
# Session that can heal itself
# ---------------------------------------------------------------------------

class CGTraderSession:
    """A requests session kept valid by re-solving the WAF challenge in Chromium
    whenever CGTrader stops serving content.
    """

    def __init__(self, headless=True, verbose=True):
        self.headless = headless
        self.verbose = verbose
        self.session = requests.Session()
        self.session.headers.update(HTML_HEADERS)
        self.solves = 0
        self.nav_counts = {}  # category slug -> itemCount string, harvested for free

    def solve_challenge(self, slug="aircraft"):
        """Drive a real browser until the WAF token exists, then steal the cookies.

        Never raises: a long run must not die because one challenge pass was slow or
        one page rendered oddly. Returns True if an aws-waf-token was obtained.
        """
        from playwright.sync_api import sync_playwright

        # Always solve against a listing known to render cards; the slug being
        # measured might be an odd/empty one, and that is not a WAF problem.
        url = f"{BASE_URL}/3d-models/{slug}"
        t0 = time.time()
        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(
                    headless=self.headless,
                    args=["--disable-blink-features=AutomationControlled"],
                )
                try:
                    ctx = browser.new_context(
                        viewport={"width": 1440, "height": 900},
                        locale="en-US",
                        user_agent=USER_AGENT,
                    )
                    # Images/fonts are irrelevant to the data and cost seconds.
                    ctx.route("**/*", lambda route: route.abort()
                              if route.request.resource_type in ("image", "font", "media")
                              else route.continue_())
                    page = ctx.new_page()
                    page.goto(url, wait_until="domcontentloaded", timeout=90_000)

                    # "attached" not "visible": cards lazy-load and images are blocked,
                    # so the first card often never becomes visible even on a good page.
                    try:
                        page.wait_for_selector(CARD_SELECTOR, state="attached",
                                               timeout=45_000)
                    except Exception:
                        pass  # the token check below is what actually matters

                    got = self._token_present(ctx)
                    if not got:
                        # The challenge page reloads itself once the token is minted.
                        for _ in range(20):
                            time.sleep(1)
                            if self._token_present(ctx):
                                got = True
                                break

                    try:
                        self._harvest_nav_counts(page.content())
                    except Exception:
                        pass

                    for c in ctx.cookies():
                        self.session.cookies.set(
                            c["name"], c["value"],
                            domain=c["domain"], path=c.get("path", "/"),
                        )
                    self.solves += 1
                    if self.verbose:
                        state = "cleared" if got else "no token (continuing anyway)"
                        print(f"  [waf] {state} in {time.time() - t0:.1f}s "
                              f"(solve #{self.solves})")
                    return got
                finally:
                    browser.close()
        except Exception as exc:
            if self.verbose:
                print(f"  [waf] solve failed after {time.time() - t0:.1f}s: "
                      f"{type(exc).__name__}: {exc}")
            return False

    @staticmethod
    def _token_present(ctx):
        return any(c["name"].startswith("aws-waf-token") for c in ctx.cookies())

    def _harvest_nav_counts(self, html):
        """The nav's React props carry each category's real item count — free totals."""
        try:
            soup = _soup(html)
            el = soup.find(attrs={"data-react-props": True})
            if not el:
                return
            props = json.loads(el["data-react-props"])
            nav = props.get("unifiedNavData", {}) or {}
            for group in ("assetCategories", "printableCategories"):
                for cat in (nav.get(group, {}) or {}).get("itemCategories", []) or []:
                    if cat.get("slug"):
                        self.nav_counts[cat["slug"]] = cat.get("itemCount")
                    for sub in cat.get("subcategories", []) or []:
                        if sub.get("slug"):
                            self.nav_counts[sub["slug"]] = sub.get("itemCount")
        except Exception:
            pass  # purely informational

    def fetch_page(self, slug, page_num, sort=None, sort_param="sort_by"):
        """Return (items, status, meta). status is 'ok' | 'end' | 'challenge'."""
        params = {"page": page_num}
        if sort:
            params[sort_param] = sort
        r = self.session.get(f"{BASE_URL}/3d-models/{slug}", params=params, timeout=60)
        if r.status_code == 404:
            return [], "end", {}
        body = r.text
        if is_challenge(r.status_code, body):
            return [], "challenge", {}
        r.raise_for_status()
        meta = extract_listing_meta(body)
        items = parse_cards(body, slug, page_num)
        if not items:
            # 200 but no cards: treat as end-of-listing rather than an error.
            return [], "end", meta
        return items, "ok", meta


# ---------------------------------------------------------------------------
# Resumable state
# ---------------------------------------------------------------------------

class RunState:
    def __init__(self, path):
        self.path = path
        self.data = {"categories": {}}
        if os.path.exists(path):
            try:
                with open(path, encoding="utf-8") as f:
                    self.data = json.load(f)
            except (OSError, json.JSONDecodeError) as exc:
                print(f"[warn] unreadable state file ({exc}); starting fresh")

    def cat(self, name):
        return self.data["categories"].setdefault(
            name, {"last_page": 0, "complete": False, "count": 0, "failed_pages": []})

    def save(self):
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, self.path)  # atomic — a crash mid-write can't corrupt it


# ---------------------------------------------------------------------------
# Scraping
# ---------------------------------------------------------------------------

def _append(jsonl_path, items):
    with open(jsonl_path, "a", encoding="utf-8") as f:
        for it in items:
            f.write(json.dumps(it, ensure_ascii=False) + "\n")


def _fetch_with_recovery(sess, slug, page_num, sort=None, sort_param="sort_by"):
    """Fetch one page, healing challenges and transient errors. (items, status, meta)."""
    for attempt in range(MAX_ATTEMPTS_PER_PAGE):
        try:
            items, status, meta = sess.fetch_page(slug, page_num, sort, sort_param)
            if status == "challenge":
                print(f"  page {page_num}: WAF challenge — re-solving")
                sess.solve_challenge(slug)
                continue
            return items, status, meta
        except Exception as exc:
            wait = BACKOFF_SECONDS[min(attempt, len(BACKOFF_SECONDS) - 1)]
            print(f"  page {page_num}: {type(exc).__name__}: {exc} — retry in {wait}s "
                  f"({attempt + 1}/{MAX_ATTEMPTS_PER_PAGE})")
            time.sleep(wait)
            if attempt >= 1:
                try:
                    sess.solve_challenge(slug)
                except Exception as exc2:
                    print(f"    (re-solve failed: {exc2})")
    return [], "failed", {}


def scrape_category(sess, name, slug, state, out_dir, max_pages=None,
                    sort=None, sort_param="sort_by"):
    cs = state.cat(name)
    if cs["complete"]:
        print(f"  already complete ({cs['count']} items) — skipping")
        return
    jsonl_path = os.path.join(out_dir, f"{name}.jsonl")
    page_num = cs["last_page"] + 1
    if page_num > 1:
        print(f"  resuming at page {page_num} ({cs['count']} items saved so far)")

    nav = sess.nav_counts.get(slug)
    if nav:
        print(f"  nav data says ~{nav} models in this category")

    total_pages = cs.get("total_pages")
    sort_checked = False

    while not _stop:
        if max_pages and page_num > max_pages:
            print(f"  hit --max-pages {max_pages}; stopping this category here")
            break
        if total_pages and page_num > total_pages:
            cs["complete"] = True
            state.save()
            print(f"  reached the last page ({total_pages}) — {cs['count']} items total")
            break

        items, status, meta = _fetch_with_recovery(sess, slug, page_num, sort, sort_param)

        if meta:
            if meta.get("totalPages") and not total_pages:
                total_pages = meta["totalPages"]
                cs["total_pages"] = total_pages
                cs["total_count"] = meta.get("totalCount")
                print(f"  listing reports {meta.get('totalCount')} models "
                      f"across {total_pages} pages ({meta.get('perPage')}/page)")
            if sort and not sort_checked:
                sort_checked = True
                applied = meta.get("sortByValue")
                if applied != sort:
                    print(f"  [warn] asked for {sort_param}={sort} but the page reports "
                          f"sortByValue={applied!r} — the sort did NOT apply. Ordering may "
                          f"drift between pages, so some models can be missed or repeated.")
                else:
                    print(f"  sort '{sort}' applied (stable pagination)")

        if status == "end":
            cs["complete"] = True
            state.save()
            print(f"  page {page_num}: end of '{name}' — {cs['count']} items total")
            break

        if status == "failed":
            print(f"  page {page_num}: skipping for now (recorded for --retry-failed)")
            if page_num not in cs["failed_pages"]:
                cs["failed_pages"].append(page_num)
            cs["last_page"] = page_num
            state.save()
            page_num += 1
            jittered_sleep(PAGE_DELAY)
            continue

        _append(jsonl_path, items)
        cs["count"] += len(items)
        cs["last_page"] = page_num
        state.save()  # only after the data is durably on disk

        if page_num % 10 == 0 or page_num == 1:
            pct = f" ({page_num}/{total_pages})" if total_pages else ""
            print(f"  page {page_num}{pct}: +{len(items)} -> {cs['count']} items")

        page_num += 1
        jittered_sleep(PAGE_DELAY)


def retry_failed(sess, name, slug, state, out_dir):
    cs = state.cat(name)
    pending = list(cs.get("failed_pages", []))
    if not pending:
        print("  no failed pages")
        return
    print(f"  retrying {len(pending)} page(s): {pending}")
    jsonl_path = os.path.join(out_dir, f"{name}.jsonl")
    still = []
    for page_num in pending:
        if _stop:
            still.append(page_num)
            continue
        items, status, _ = _fetch_with_recovery(sess, slug, page_num)
        if status == "ok":
            _append(jsonl_path, items)
            cs["count"] += len(items)
            print(f"    page {page_num}: recovered +{len(items)}")
        elif status == "end":
            print(f"    page {page_num}: past the end, nothing to get")
        else:
            still.append(page_num)
            print(f"    page {page_num}: still failing")
        state.save()
        jittered_sleep(PAGE_DELAY)
    cs["failed_pages"] = still
    state.save()


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

def write_csv(name, out_dir, stamp):
    jsonl_path = os.path.join(out_dir, f"{name}.jsonl")
    if not os.path.exists(jsonl_path):
        return None, 0
    seen, rows = set(), []
    with open(jsonl_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                it = json.loads(line)
            except json.JSONDecodeError:
                continue  # torn final line from a hard kill
            key = it.get("id") or it.get("url")
            if key in seen:
                continue
            seen.add(key)
            rows.append(it)
    path = os.path.join(out_dir, f"{name}_cgtrader_{stamp}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    return path, len(rows)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    global PAGE_DELAY

    ap = argparse.ArgumentParser(description="CGTrader scraper (WAF-aware, resumable).")
    ap.add_argument("--categories", default=None, help="comma-separated subset")
    ap.add_argument("--max-pages", type=int, default=None, help="cap pages per category")
    ap.add_argument("--out-dir", default="./cgtrader_data")
    ap.add_argument("--delay", type=float, default=PAGE_DELAY,
                    help="avg seconds between pages (randomized around this)")
    ap.add_argument("--headed", action="store_true", help="show the browser window")
    ap.add_argument("--probe", action="store_true", help="fetch one page and exit")
    ap.add_argument("--counts", action="store_true",
                    help="print the site's own item count per category and exit")
    ap.add_argument("--retry-failed", action="store_true", help="only re-attempt skipped pages")
    ap.add_argument("--csv-only", action="store_true", help="rebuild CSVs from .jsonl")
    ap.add_argument("--force", action="store_true", help="re-scrape completed categories")
    ap.add_argument("--sort", default="oldest",
                    help="listing sort. 'oldest' is the default because new uploads land at "
                         "the end, so pages stay stable through a long run. The site's default "
                         "('best_match') reshuffles between requests, which makes paginating "
                         "tens of thousands of models lossy. Pass '' to use the site default.")
    ap.add_argument("--sort-param", default="sort_by",
                    help="query-param name for the sort (the scraper verifies it applied and "
                         "warns if not)")
    args = ap.parse_args()
    PAGE_DELAY = args.delay

    os.makedirs(args.out_dir, exist_ok=True)
    stamp = f"{datetime.now(timezone.utc):%Y%m%d}"
    state = RunState(os.path.join(args.out_dir, "state.json"))

    if args.categories:
        wanted = [c.strip() for c in args.categories.split(",") if c.strip()]
        unknown = [c for c in wanted if c not in CATEGORIES]
        if unknown:
            raise SystemExit(f"Unknown categories: {unknown}\nKnown: {list(CATEGORIES)}")
        cats = {k: CATEGORIES[k] for k in wanted}
    else:
        cats = dict(CATEGORIES)

    if args.csv_only:
        print("Rebuilding CSVs from saved .jsonl files...")
        for name in cats:
            path, n = write_csv(name, args.out_dir, stamp)
            if path:
                print(f"  {name}: {n} unique items -> {path}")
        return

    signal.signal(signal.SIGINT, _sigint)

    if args.force:
        for name in cats:
            state.cat(name)["complete"] = False
        state.save()

    sess = CGTraderSession(headless=not args.headed)
    print("Clearing the AWS WAF challenge in a real browser...")
    sess.solve_challenge(next(iter(cats.values())))

    if args.counts:
        print("\nCGTrader's own reported counts:")
        for name, slug in cats.items():
            print(f"  {name:14s} {sess.nav_counts.get(slug, '(not in nav data)')}")
        return

    if args.probe:
        slug = next(iter(cats.values()))
        items, status, meta = _fetch_with_recovery(sess, slug, 1,
                                                  args.sort or None, args.sort_param)
        print(f"PROBE: status={status} items={len(items)}")
        if meta:
            print(f"  listing meta: {json.dumps(meta, ensure_ascii=False)}")
            if args.sort and meta.get("sortByValue") not in (None, args.sort):
                print(f"  [warn] {args.sort_param}={args.sort} did not apply "
                      f"(page reports {meta.get('sortByValue')!r})")
        if items:
            print(json.dumps(items[0], ensure_ascii=False, indent=2))
        raise SystemExit(0 if items else 1)

    for name, slug in cats.items():
        if _stop:
            break
        print(f"\n=== {name} ===")
        try:
            if args.retry_failed:
                retry_failed(sess, name, slug, state, args.out_dir)
            else:
                scrape_category(sess, name, slug, state, args.out_dir,
                                max_pages=args.max_pages,
                                sort=args.sort or None, sort_param=args.sort_param)
        except Exception as exc:
            # A category-level surprise must never end the whole run.
            print(f"  [category error] {type(exc).__name__}: {exc} — moving on")
            state.save()
        path, n = write_csv(name, args.out_dir, stamp)
        if path:
            print(f"  csv: {n} unique items -> {path}")
        if not _stop:
            jittered_sleep(CATEGORY_COOLDOWN)

    print("\n=== summary ===")
    total = 0
    for name in cats:
        cs = state.cat(name)
        total += cs["count"]
        where = "complete" if cs["complete"] else f"stopped at page {cs['last_page']}"
        extra = f", {len(cs['failed_pages'])} page(s) to retry" if cs["failed_pages"] else ""
        print(f"  {name:14s} {cs['count']:>8} items  ({where}{extra})")
    print(f"  {'TOTAL':14s} {total:>8} items")
    if any(state.cat(n)["failed_pages"] for n in cats):
        print("\nRe-run with --retry-failed to pick up skipped pages.")
    if _stop:
        print("Interrupted — re-run the same command to resume.")


if __name__ == "__main__":
    main()
