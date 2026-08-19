"""
Per-model detail scraper — everything the listing pages don't carry.

WHY A SECOND PASS
-----------------
`cgtrader_deep_scrape.py` reads listing pages, which only expose title, price,
url, formats, type flags and image. Everything else the user cares about
(overview + full description, designer, publish date, model ID, polygon/vertex
counts, per-format file sizes, CGTrader verification checks, related tags,
reviews/comments, likes, views) lives ONLY on the individual product page.

That means one HTTP request per model. There is no bulk endpoint — so this is
inherently slow: ~1.5s/model is ~40 min per 1,000 models. Scope accordingly.

GOOD NEWS: the product page ships all of it as structured JSON, so nothing here
depends on fragile HTML/CSS scraping:
  * `data-react-props` on `ItemPage/TopSection/TopSection` ->
    badgesArea, pricingArea, sellerArea, detailsArea, descriptionArea,
    reviewsArea, galleryArea, useCasesArea
  * `ld+json` @type=Product -> aggregateRating, offers, review, sku, brand

It reuses the WAF handling from `cgtrader_scraper_v2.py` (challenge solved once
in Chromium, cookies transplanted into a plain requests session, re-solved
automatically whenever a challenge reappears).

INPUT: the .jsonl files produced by cgtrader_deep_scrape.py (deduped by model id).
OUTPUT: <category>_details.jsonl (one line per model, appended as it goes) and
        <category>_details_<date>.csv

Resumable: already-fetched model ids are skipped, so re-running the same command
continues where it stopped. Interrupt with Ctrl+C any time.

USAGE
-----
    python cgtrader_detail_scrape.py --test-url <product url>     # verify parsing
    python cgtrader_detail_scrape.py --category award             # one full category
    python cgtrader_detail_scrape.py --category award --limit 100 # a sample first
    python cgtrader_detail_scrape.py --category award --export    # rebuild CSV only
"""

import argparse
import csv
import json
import os
import re
import signal
import sys
import time
import zlib
from datetime import datetime, timezone
from urllib.parse import urlparse

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from cgtrader_scraper_v2 import (  # noqa: E402
    CGTraderSession,
    _soup,
    is_challenge,
    jittered_sleep,
    note_progress,
    start_stall_watchdog,
)
import cgtrader_scraper_v2 as v2  # noqa: E402

TOP_SECTION_CLASS = "ItemPage/TopSection/TopSection"
MAX_ATTEMPTS = 5
BACKOFF = [5, 15, 30, 60, 120]

DETAIL_FIELDS = [
    "id", "url", "title", "badges",
    # pricing
    "price", "price_numeric", "is_free", "license", "ai_friendly",
    "rating_score", "reviews_count",
    "price_original", "is_sale_active", "sale_discount_pct", "price_final",
    "accepts_price_offer",
    # seller
    "designer_id", "designer_name", "designer_rating", "designer_reviews_count",
    "designer_available_for_hire", "designer_url",
    # dates / ids
    "publish_date", "model_id",
    # geometry
    "geometry_type", "polygons", "vertices", "triangles", "unwrapped_uvs",
    # flags
    "is_animated", "is_rigged", "is_game_ready", "is_pbr", "has_textures",
    "has_materials", "has_uv_mapping", "plugins_used", "ready_for_3d_printing",
    # formats
    "formats", "formats_detail", "total_file_size",
    # verification
    "cgt_verified", "cgt_report_type", "cgt_score", "cgt_checks_passed",
    "cgt_checks_failed",
    # text
    "overview", "description_text", "description_html_len",
    # taxonomy
    "breadcrumbs", "category_path", "related_tags", "related_tags_count",
    # engagement
    "views_count", "favorites_count", "downloads_count", "likes", "dislikes",
    "comments_count", "images_count", "top_reviews",
    # bookkeeping
    "fetched_at",
]

_stop = False


def _sigint(signum, frame):
    global _stop
    if _stop:
        print("\n[force quit]")
        sys.exit(130)
    _stop = True
    print("\n[stopping after this model — progress is saved. Ctrl+C again to quit now]")


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

_TAG_RE = re.compile(r"<[^>]+>")


def html_to_text(html):
    """Flatten the description HTML to readable text, keeping paragraph breaks."""
    if not html:
        return ""
    s = re.sub(r"<br\s*/?>", "\n", html, flags=re.I)
    s = re.sub(r"</(p|div|li|h[1-6])>", "\n", s, flags=re.I)
    s = re.sub(r"<li[^>]*>", "  - ", s, flags=re.I)
    s = _TAG_RE.sub("", s)
    s = (s.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<")
          .replace("&gt;", ">").replace("&quot;", '"').replace("&#39;", "'"))
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def split_overview(text):
    """The description starts with an 'Overview' heading; separate it out."""
    if not text:
        return "", ""
    m = re.search(r"^\s*Overview\s*\n(.*?)(?:\n\s*(?:Description|Textures|"
                  r"Technical Description|Features)\s*\n|\Z)", text, re.S | re.I)
    if m:
        return m.group(1).strip(), text
    return "", text


def flatten_checks(node, prefix="", passed=None, failed=None):
    """cgtVerificationData.data is nested groups of {name: {pass: bool}}."""
    passed = [] if passed is None else passed
    failed = [] if failed is None else failed
    if isinstance(node, dict):
        if "pass" in node and isinstance(node["pass"], bool):
            (passed if node["pass"] else failed).append(prefix)
            return passed, failed
        for k, v in node.items():
            flatten_checks(v, f"{prefix}.{k}" if prefix else k, passed, failed)
    return passed, failed


def parse_detail(html, url):
    soup = _soup(html)

    props = None
    for el in soup.find_all(attrs={"data-react-props": True}):
        if el.get("data-react-class") == TOP_SECTION_CLASS:
            try:
                props = json.loads(el["data-react-props"])
            except ValueError:
                props = None
            break
    if props is None:
        raise ValueError("TopSection props not found (page shape changed?)")

    pricing = props.get("pricingArea") or {}
    seller = props.get("sellerArea") or {}
    details_area = props.get("detailsArea") or {}
    det = details_area.get("details") or {}
    desc_area = props.get("descriptionArea") or {}
    reviews = props.get("reviewsArea") or {}
    gallery = props.get("galleryArea") or {}

    # ld+json fills in a couple of things the props don't have directly
    ld = {}
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            d = json.loads(tag.string or "{}")
        except ValueError:
            continue
        if d.get("@type") == "Product":
            ld = d
            break

    price_text = pricing.get("price") or ""
    price_num = None
    m = re.search(r"([\d,]+(?:\.\d+)?)", price_text)
    if m:
        try:
            price_num = float(m.group(1).replace(",", ""))
        except ValueError:
            pass
    if price_num is None and pricing.get("free"):
        price_num = 0.0

    formats = details_area.get("ungroupedFormats") or []
    for key in ("nativeFormats", "exchangeFormats"):
        extra = details_area.get(key) or []
        if isinstance(extra, list):
            formats = formats + extra
    fmt_names, fmt_detail = [], []
    for f in formats:
        if not isinstance(f, dict):
            continue
        name = f.get("title") or f.get("fullName")
        if name:
            fmt_names.append(name)
        fmt_detail.append({
            "format": name,
            "full_name": f.get("fullName"),
            "file_size": f.get("fileSize"),
            "file_count": f.get("fileCount"),
            "version": f.get("version"),
            "renderer": f.get("rendererName"),
        })

    ver = details_area.get("cgtVerificationData") or {}
    passed, failed = flatten_checks(ver.get("data") or {})

    desc_html = desc_area.get("description") or ""
    desc_text = html_to_text(desc_html)
    overview, _ = split_overview(desc_text)

    crumbs = [c.get("title") for c in (desc_area.get("breadcrumbs") or [])
              if isinstance(c, dict) and c.get("title")]
    tags = [t.get("text") for t in (desc_area.get("relatedTags") or [])
            if isinstance(t, dict) and t.get("text")]

    # a few representative reviews rather than the whole thread
    top_reviews = []
    for r in (reviews.get("ratings") or [])[:5]:
        if not isinstance(r, dict):
            continue
        top_reviews.append({
            "score": r.get("score"),
            "comment": (r.get("comment") or "").strip(),
            "date": (r.get("createdAt") or "")[:10],
        })

    agg = ld.get("aggregateRating") or {}

    return {
        "id": pricing.get("productId") or details_area.get("id") or ld.get("sku"),
        "url": url,
        "title": pricing.get("title") or gallery.get("title") or ld.get("name"),
        "badges": ", ".join(props.get("badgesArea") or []),

        "price": price_text,
        "price_numeric": price_num,
        "is_free": pricing.get("free"),
        "license": pricing.get("license"),
        "ai_friendly": pricing.get("publishedAiFriendly"),
        "rating_score": pricing.get("ratingScore") or agg.get("ratingValue"),
        "reviews_count": pricing.get("reviewsCount"),

        "designer_id": seller.get("designerId"),
        "designer_name": seller.get("designerName"),
        "designer_rating": seller.get("rating"),
        "designer_reviews_count": seller.get("reviewsCount"),
        "designer_available_for_hire": seller.get("availableForHire"),
        "designer_url": (f"https://www.cgtrader.com/designers/"
                         f"{seller.get('designerName','').lower()}"
                         if seller.get("designerName") else None),

        "publish_date": details_area.get("publishDate"),
        "model_id": details_area.get("id"),

        "geometry_type": details_area.get("geometryType"),
        "polygons": details_area.get("polygons"),
        "vertices": details_area.get("vertices"),
        "triangles": details_area.get("triangles"),
        "unwrapped_uvs": details_area.get("unwrappedUvs") or det.get("unwrappedUvs"),

        "is_animated": details_area.get("animated", det.get("animated")),
        "is_rigged": details_area.get("rigged", det.get("rigged")),
        "is_game_ready": details_area.get("gameReady", det.get("gameReady")),
        "is_pbr": details_area.get("pbr", det.get("pbr")),
        "has_textures": det.get("textures"),
        "has_materials": det.get("materials"),
        "has_uv_mapping": det.get("uvMapping"),
        "plugins_used": det.get("pluginsUsed"),
        "ready_for_3d_printing": details_area.get("readyFor3dPrinting"),

        "formats": ", ".join(fmt_names),
        "formats_detail": json.dumps(fmt_detail, ensure_ascii=False),
        "total_file_size": ", ".join(
            f"{f['format']}:{f['file_size']}" for f in fmt_detail if f.get("file_size")),

        "cgt_verified": bool(ver),
        "cgt_report_type": ver.get("reportType"),
        "cgt_score": ver.get("score"),
        "cgt_checks_passed": ", ".join(passed),
        "cgt_checks_failed": ", ".join(failed),

        "overview": overview,
        "description_text": desc_text,
        "description_html_len": len(desc_html),

        "breadcrumbs": " / ".join(crumbs),
        "category_path": " / ".join(crumbs[1:]) if len(crumbs) > 1 else "",
        "related_tags": ", ".join(tags),
        "related_tags_count": len(tags),

        "likes": reviews.get("likeCount"),
        "dislikes": reviews.get("dislikeCount"),
        "comments_count": reviews.get("commentsCount"),
        "images_count": len(gallery.get("medias") or []),
        "top_reviews": json.dumps(top_reviews, ensure_ascii=False),

        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# Fetching
# ---------------------------------------------------------------------------

_PRODUCT_JSON_RE = re.compile(r'"product"\s*:\s*(\{.*?\})\s*,\s*"subscriptionFlowEnabled"')

EXTRA_FIELD_MAP = {
    # our field name -> key in window.pageConfig.product
    "views_count": "views",
    "favorites_count": "likes",
    "downloads_count": "downloads",
    "accepts_price_offer": "acceptsPriceOffer",
}


def fetch_product_extra(sess, product_url, solve_slug="aircraft", max_attempts=3):
    """The listing/product-page JSON never carries view/heart counts, download
    counts, or the ACTIVE sale discount -- all of that is injected client-side
    by React from a SEPARATE endpoint: `https://www.cgtrader.com/products/
    <slug>.js`, in a `window.pageConfig = {...{"product": {...}}}` blob. It's
    same-origin and answers a plain `requests` GET with the already-solved WAF
    cookies -- no browser needed.

    This used to be parsed with two narrow regexes for just views/likes, which
    silently discarded the rest of the payload -- including `saleOffDiscount`,
    the field a $10,000 "IPL Trophy" model turned out to actually be selling
    at $5,000 through (spotted 2026-08-03: the plain product-page price is the
    PRE-discount price; the "-50%" badge and true price are computed here).

    Returns a dict with keys: views_count, favorites_count, downloads_count,
    price_original, is_sale_active, sale_discount_pct, price_final,
    accepts_price_offer -- or {} if the endpoint was unreachable (callers
    should not fail the whole row just because this one extra fetch failed).
    """
    slug = urlparse(product_url).path.rstrip("/").rsplit("/", 1)[-1]
    if not slug:
        return {}
    js_url = f"https://www.cgtrader.com/products/{slug}.js"

    for attempt in range(max_attempts):
        try:
            r = sess.session.get(
                js_url, params={"prev_page": ""}, timeout=30,
                headers={"Accept": "text/javascript, */*; q=0.01",
                        "X-Requested-With": "XMLHttpRequest"})
            if r.status_code == 404:
                return {}
            if is_challenge(r.status_code, r.text):
                sess.solve_challenge(solve_slug)
                continue
            r.raise_for_status()
            m = _PRODUCT_JSON_RE.search(r.text)
            if not m:
                return {}
            try:
                product = json.loads(m.group(1))
            except json.JSONDecodeError:
                return {}

            out = {k: product.get(v) for k, v in EXTRA_FIELD_MAP.items()}
            price = product.get("price")
            discount = product.get("saleOffDiscount") or 0
            is_sale = bool(product.get("isSaleOffApplicable")) and discount > 0
            out["price_original"] = price
            out["is_sale_active"] = is_sale
            out["sale_discount_pct"] = discount if is_sale else 0
            out["price_final"] = (round(price * (1 - discount / 100), 2)
                                  if is_sale and isinstance(price, (int, float))
                                  else price)
            return out
        except Exception:
            time.sleep(BACKOFF[min(attempt, len(BACKOFF) - 1)])
    return {}


# These are real, final answers from the server about the RESOURCE, not a
# transient network/WAF issue -- retrying them just burns the 5-attempt backoff
# (up to ~4 min) for nothing. Discovered 2026-08-02: two models stuck on 410
# Gone / 423 Locked kept exhausting retries, which starved note_progress() long
# enough to trip the stall watchdog, which restarted the whole process, which
# hit the SAME two dead URLs again -- an infinite restart loop that looked like
# "stuck at 9958/9960" for hours.
PERMANENT_FAILURE_CODES = {404, 410, 423, 451}

# A deleted model does not always answer with one of those codes. CGTrader also
# soft-404s: it 301s the dead product URL to a plain tag-browse page, which
# returns a perfectly healthy 200 that simply has no product on it.
#     /free-3d-models/vehicle/train/train-wagon -> 301 -> /3d-models/wagon
# parse_detail() then (correctly) raises "TopSection props not found", every
# retry reproduces it, and the model is never recorded -- so it is re-queued on
# every future run, forever, and the category can never reach 100%. That is what
# pinned interior/character/vehicle one or two models short of target while all
# three displayed as "100.0%" (2026-08-19).
#
# A product URL always has three path segments after the [free-]3d-models root
# (category/subcategory/slug); a tag-browse page has one. Renamed-slug redirects
# still land on a product URL, so they keep the normal retry path.
_PRODUCT_PATH_RE = re.compile(r"^/(?:free-)?3d-models/[^/]+/[^/]+/[^/]+/?$")


def _redirected_off_product(resp):
    """True if the request was redirected to something that isn't a model page."""
    if not resp.history:
        return False
    return not _PRODUCT_PATH_RE.match(urlparse(resp.url).path)

# Sentinel the CI chunk loop watches for. The loop reruns this script until the
# category is finished; without an explicit "finished" signal it can't tell
# "ran out of time for this chunk" (rerun me) from "ran out of models"
# (stop), and would keep relaunching a process that exits in seconds.
COMPLETE_SENTINEL = ".scrape_category_complete"


def _signal_category_complete(category):
    try:
        with open(COMPLETE_SENTINEL, "w", encoding="utf-8") as f:
            f.write(f"{category}\n")
    except OSError:
        pass  # advisory only -- never fail a run over the sentinel


def _shard_key(model_id):
    """Stable numeric key for partitioning. Ids are numeric strings in practice,
    but fall back to a hash so a stray non-numeric id still lands somewhere
    (exactly one worker) instead of crashing the run."""
    s = str(model_id)
    return int(s) if s.isdigit() else zlib.crc32(s.encode())


def _parse_shard(spec):
    """'3/8' -> (3, 8, 's3of8'). None -> (0, 1, None), i.e. take everything."""
    if not spec:
        return 0, 1, None
    try:
        i, n = (int(x) for x in spec.split("/", 1))
    except ValueError:
        raise SystemExit(f"--shard wants I/N (e.g. 3/8), got {spec!r}")
    if n < 1 or not 0 <= i < n:
        raise SystemExit(f"--shard {spec}: need 1 <= N and 0 <= I < N")
    return i, n, (None if n == 1 else f"s{i}of{n}")


def fetch_detail(sess, url, solve_slug="aircraft"):
    """Fetch and parse one product page, healing WAF challenges. None if hopeless."""
    for attempt in range(MAX_ATTEMPTS):
        try:
            r = sess.session.get(url, timeout=60)
            if r.status_code in PERMANENT_FAILURE_CODES:
                return {"__missing__": True, "__status__": r.status_code}
            if is_challenge(r.status_code, r.text):
                sess.solve_challenge(solve_slug)
                continue
            if _redirected_off_product(r):
                return {"__missing__": True, "__status__": 301,
                        "__reason__": f"redirected to {urlparse(r.url).path}"}
            r.raise_for_status()
            row = parse_detail(r.text, url)
            row.update(fetch_product_extra(sess, url, solve_slug))
            return row
        except Exception as exc:
            wait = BACKOFF[min(attempt, len(BACKOFF) - 1)]
            print(f"    {type(exc).__name__}: {exc} -- retry in {wait}s "
                  f"({attempt + 1}/{MAX_ATTEMPTS})")
            time.sleep(wait)
            if attempt >= 1:
                try:
                    sess.solve_challenge(solve_slug)
                except Exception:
                    pass
    return None


# ---------------------------------------------------------------------------
# Inputs / outputs
# ---------------------------------------------------------------------------

def load_targets(raw_dir, category, limit=None, order="price_desc"):
    """Unique (id, url) pairs for a category, from the listing-pass .jsonl."""
    path = os.path.join(raw_dir, f"{category}.jsonl")
    if not os.path.exists(path):
        raise SystemExit(f"No listing data at {path}\n"
                         f"Run cgtrader_deep_scrape.py first.")
    seen, rows = set(), []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                it = json.loads(line)
            except json.JSONDecodeError:
                continue
            mid, url = it.get("id"), it.get("url")
            if not mid or not url or mid in seen:
                continue
            seen.add(mid)
            rows.append({"id": mid, "url": url,
                         "price_usd": it.get("price_usd") or 0})
    if order == "price_desc":
        rows.sort(key=lambda r: r["price_usd"] or 0, reverse=True)
    elif order == "price_asc":
        rows.sort(key=lambda r: r["price_usd"] or 0)
    if limit:
        rows = rows[:limit]
    return rows


# ---------------------------------------------------------------------------
# Sharded storage
# ---------------------------------------------------------------------------
# A single ever-growing <category>_details.jsonl breaks GitHub the moment a
# category's detail data crosses ~100 MB (a hard per-file cap; git push just
# fails). At ~3 KB/row, several categories already queued after `award` land
# in that danger zone once fully detail-scraped (e.g. industrial: 31,846
# models * ~3 KB ~= 99 MB). So data is stored as capped shards instead:
# <out_dir>/<category>_details/part_00001.jsonl, part_00002.jsonl, ...
# each holding at most SHARD_MAX_ROWS rows (comfortably under the limit
# regardless of category size). A pre-sharding flat file, if found, is
# migrated into this layout automatically and transparently the first time
# it's touched.

SHARD_MAX_ROWS = 8000


def _shard_dir(out_dir, category):
    return os.path.join(out_dir, f"{category}_details")


def _shard_paths(out_dir, category):
    d = _shard_dir(out_dir, category)
    if not os.path.isdir(d):
        return []
    return sorted(
        os.path.join(d, fn) for fn in os.listdir(d)
        if fn.startswith("part_") and fn.endswith(".jsonl")
    )


def _write_shards(out_dir, category, rows, max_rows=SHARD_MAX_ROWS):
    """Full, atomic-per-file rewrite of every shard from an in-memory row list.
    Used by patch_extra()'s checkpoint, which already needs the whole category
    in memory to backfill fields.
    """
    d = _shard_dir(out_dir, category)
    os.makedirs(d, exist_ok=True)
    n_shards = max(1, -(-len(rows) // max_rows)) if rows else 0
    written = set()
    for i in range(n_shards):
        chunk = rows[i * max_rows:(i + 1) * max_rows]
        path = os.path.join(d, f"part_{i + 1:05d}.jsonl")
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            for r in chunk:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        os.replace(tmp, path)
        written.add(path)
    for p in _shard_paths(out_dir, category):
        if p not in written:
            os.remove(p)  # a shrink (shouldn't normally happen) leaves no orphans


def _migrate_legacy_flat_file(out_dir, category):
    """A category scraped before sharding existed has one big
    <category>_details.jsonl. Fold it into properly-capped shards once.
    """
    legacy = os.path.join(out_dir, f"{category}_details.jsonl")
    if not os.path.exists(legacy) or _shard_paths(out_dir, category):
        return
    rows, seen = [], set()
    with open(legacy, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                it = json.loads(line)
            except json.JSONDecodeError:
                continue
            if it.get("id") in seen:
                continue
            seen.add(it.get("id"))
            rows.append(it)
    _write_shards(out_dir, category, rows)
    os.remove(legacy)
    print(f"  [migrate] folded legacy {legacy} into "
          f"{len(_shard_paths(out_dir, category))} shard(s)")


class ShardedWriter:
    """Cheap append-only writer for the main scrape loop: no full-category
    rewrite per row, just append to the current shard and roll to a new one
    once it hits SHARD_MAX_ROWS.
    """

    def __init__(self, out_dir, category, max_rows=SHARD_MAX_ROWS, tag=None):
        _migrate_legacy_flat_file(out_dir, category)
        self.dir = _shard_dir(out_dir, category)
        os.makedirs(self.dir, exist_ok=True)
        self.max_rows = max_rows
        # With --shard, several runners scrape one category at the same time.
        # They must not append to the SAME part_NNNNN.jsonl: every one of them
        # would open it at the same length, and the push-time union merge would
        # then have to reconcile files that disagree line-for-line. Giving each
        # worker its own filename series makes the shards disjoint by
        # construction, so the merge is a plain add of new files. Readers glob
        # part_*.jsonl, which matches both the tagged and untagged series.
        self.tag = tag
        # Anchored, so the untagged writer claims only part_00001.jsonl and
        # never mistakes another worker's part_s3_00001.jsonl for its own.
        self._mine = re.compile(
            r"^part_\d+\.jsonl$" if tag is None
            else rf"^part_{re.escape(tag)}_\d+\.jsonl$")
        own = [p for p in _shard_paths(out_dir, category)
               if self._mine.match(os.path.basename(p))]
        if own:
            self.path = own[-1]
            self.index = len(own)
            with open(self.path, encoding="utf-8") as f:
                self.count = sum(1 for _ in f)
        else:
            self.index = 1
            self.path = os.path.join(self.dir, self._name(self.index))
            self.count = 0

    def _prefix(self):
        return "part_" if self.tag is None else f"part_{self.tag}_"

    def _name(self, index):
        return f"{self._prefix()}{index:05d}.jsonl"

    def append(self, row):
        if self.count >= self.max_rows:
            self.index += 1
            self.path = os.path.join(self.dir, self._name(self.index))
            self.count = 0
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
        self.count += 1


def load_rows(out_dir, category):
    _migrate_legacy_flat_file(out_dir, category)
    rows, seen = [], set()
    for path in _shard_paths(out_dir, category):
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
                if mid in seen:
                    continue
                seen.add(mid)
                rows.append(it)
    return rows


def load_done(out_dir, category):
    return {str(r.get("id")) for r in load_rows(out_dir, category)}


def patch_extra(sess, category, out_dir, solve_slug="aircraft", budget_s=None):
    """One-time backfill: add views/favorites/downloads/discount/final-price to
    rows fetched before this endpoint's fields were fully parsed, WITHOUT
    re-fetching the whole product page (everything else about those rows is
    already correct and complete). Idempotent: only rows missing "price_final"
    are touched (the first version of this patch only wrote views_count/
    favorites_count, so almost every row needs this second pass once), so an
    interrupted run can just be re-run.
    """
    rows = load_rows(out_dir, category)
    if not rows:
        raise SystemExit(f"No details found for {category!r} in {out_dir}")

    # Placeholder rows stand for models CGTrader has deleted; there is no
    # product page left to read views or pricing off, so they can never gain a
    # price_final and would otherwise be re-attempted (5 retries each, with
    # backoff) on every single backfill run, forever.
    todo = [r for r in rows
            if "price_final" not in r
            and not str(r.get("title", "")).startswith("[unavailable")]
    print(f"{category}: {len(rows)} rows, {len(rows) - len(todo)} already patched "
          f"or permanently unavailable, {len(todo)} to patch")
    if not todo:
        return

    def checkpoint():
        _write_shards(out_dir, category, rows)

    CHECKPOINT_EVERY = 25  # a full-file rewrite only at the very end meant a
    # hang anywhere in a 670-row run (which happened for real, twice, on
    # 2026-08-01) lost 100% of progress. Rewriting periodically caps the loss.

    patched = failed = 0
    t0 = time.time()
    for i, row in enumerate(todo, 1):
        if _stop:
            break
        if budget_s and (time.time() - t0) >= budget_s:
            # Same reason as the main scrape loop: exit while we still control
            # the process so the caller's commit step runs. Already-patched
            # rows are safe on disk via checkpoint(); the rest stay unpatched
            # and get picked up next run, since todo is "missing price_final".
            print(f"\n[budget] backfill hit its time budget after {patched} of "
                  f"{len(todo)} rows -- stopping cleanly. Re-run to continue.")
            break
        extra = fetch_product_extra(sess, row["url"], solve_slug)
        row.update(extra)
        row.setdefault("price_final", row.get("price_numeric"))  # endpoint
        # unreachable for this row -- mark it patched anyway so it isn't
        # retried forever; price_numeric from the page is the best fallback.
        if not extra:
            failed += 1
        patched += 1
        note_progress()
        if patched % CHECKPOINT_EVERY == 0 or i == len(todo):
            checkpoint()
        if patched % 50 == 0 or patched == 1 or i == len(todo):
            print(f"  [{i}/{len(todo)}] patched={patched} empty={failed}")
        jittered_sleep(0.8)

    checkpoint()
    print(f"patched {patched} rows ({failed} came back empty) -> {_shard_dir(out_dir, category)}")


def export_csv(out_dir, category, stamp):
    rows = load_rows(out_dir, category)
    if not rows:
        print(f"  nothing to export for {category}")
        return None, 0
    path = os.path.join(out_dir, f"{category}_details_{stamp}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=DETAIL_FIELDS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    return path, len(rows)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="Scrape per-model detail pages.")
    ap.add_argument("--category", default=None, help="category name (e.g. award)")
    ap.add_argument("--raw-dir", default="./cgtrader_deep/raw",
                    help="where the listing-pass .jsonl files live")
    ap.add_argument("--out-dir", default="./cgtrader_details")
    ap.add_argument("--limit", type=int, default=None, help="only the first N models")
    ap.add_argument("--shard", default=None, metavar="I/N",
                    help="split this category across N parallel workers and "
                         "handle slice I (0-based), e.g. --shard 3/8")
    ap.add_argument("--order", default="price_desc",
                    choices=["price_desc", "price_asc", "as_listed"],
                    help="which models to do first (default: most expensive)")
    ap.add_argument("--delay", type=float, default=1.5, help="avg seconds between models")
    ap.add_argument("--test-url", default=None, help="parse one URL and print it")
    ap.add_argument("--export", action="store_true", help="rebuild CSV from .jsonl only")
    ap.add_argument("--patch-views", action="store_true",
                    help="backfill views_count/favorites_count into existing rows "
                         "that predate this endpoint, without re-fetching them")
    ap.add_argument("--headed", action="store_true")
    ap.add_argument("--max-stall-min", type=float, default=10,
                    help="restart if no model completes for this many minutes")
    ap.add_argument("--max-minutes", type=float, default=None,
                    help="stop scraping cleanly after this many minutes, then still "
                         "export/commit whatever was fetched. REQUIRED when running "
                         "under a hard external time limit (e.g. GitHub Actions' "
                         "timeout-minutes): without it the loop runs until something "
                         "else kills the process, and a SIGKILL means the caller's "
                         "commit/push step is skipped entirely -- so on 2026-08-10 "
                         "eight parallel category jobs each scraped ~5h50m and then "
                         "had ALL of it discarded when Actions cancelled them. "
                         "Set this comfortably below the external limit.")
    ap.add_argument("--log", nargs="?", const="auto", default=None)
    args = ap.parse_args()

    if args.log:
        from cgtrader_deep_scrape import _Tee
        p = args.log if args.log != "auto" else f"details_{datetime.now():%Y%m%d_%H%M%S}.log"
        sys.stdout = _Tee(sys.stdout, p)
        sys.stderr = sys.stdout
        print(f"=== detail run started {datetime.now():%Y-%m-%d %H:%M:%S} -> {p} ===")

    os.makedirs(args.out_dir, exist_ok=True)
    stamp = f"{datetime.now(timezone.utc):%Y%m%d}"

    if args.export:
        if not args.category:
            raise SystemExit("--export needs --category")
        path, n = export_csv(args.out_dir, args.category, stamp)
        if path:
            print(f"  {n} models -> {path}")
        return

    signal.signal(signal.SIGINT, _sigint)
    start_stall_watchdog(int(args.max_stall_min * 60), label='detail pass')
    sess = CGTraderSession(headless=not args.headed)
    print("Clearing the AWS WAF challenge...")
    sess.solve_challenge("aircraft")

    if args.test_url:
        row = fetch_detail(sess, args.test_url)
        if not row or row.get("__missing__"):
            raise SystemExit("could not fetch/parse that URL")
        print(json.dumps(row, ensure_ascii=False, indent=2))
        filled = sum(1 for k in DETAIL_FIELDS
                     if row.get(k) not in (None, "", [], 0, False))
        print(f"\n{filled}/{len(DETAIL_FIELDS)} fields populated")
        return

    if not args.category:
        raise SystemExit("give --category (or --test-url)")

    if args.patch_views:
        patch_extra(sess, args.category, args.out_dir,
                    budget_s=args.max_minutes * 60 if args.max_minutes else None)
        path, n = export_csv(args.out_dir, args.category, stamp)
        if path:
            print(f"  {n} models -> {path}")
        return

    targets = load_targets(args.raw_dir, args.category, args.limit, args.order)
    shard_i, shard_n, shard_tag = _parse_shard(args.shard)
    writer = ShardedWriter(args.out_dir, args.category, tag=shard_tag)
    done = load_done(args.out_dir, args.category)
    todo = [t for t in targets if str(t["id"]) not in done]

    if shard_n > 1:
        # Partition on the id, not on position in the list: the list shrinks as
        # rows land, so a positional split would hand overlapping work to the
        # workers on the next run. Every worker reads the SAME done-set, so
        # anything a sibling has already written is skipped here too.
        before = len(todo)
        todo = [t for t in todo if _shard_key(t["id"]) % shard_n == shard_i]
        print(f"shard {shard_i}/{shard_n}: {len(todo)} of {before} outstanding "
              f"models are mine")

    print(f"\n{args.category}: {len(targets)} unique models, {len(done)} already done, "
          f"{len(todo)} to fetch")
    if todo:
        est_h = len(todo) * (args.delay + 1.0) / 3600
        print(f"estimated time: ~{est_h:.1f} h at {args.delay}s delay")
    else:
        # Tell the workflow's chunk loop to stop instead of spinning: with
        # nothing to fetch this process exits in seconds, so without a signal
        # the loop would keep relaunching it (paying a WAF solve each time)
        # for the rest of the scrape budget.
        _signal_category_complete(args.category)
        return

    ok = missing = failed = 0
    t0 = time.time()
    budget_s = args.max_minutes * 60 if args.max_minutes else None
    stopped_on_budget = False
    for i, t in enumerate(todo, 1):
        if _stop:
            break
        if budget_s and (time.time() - t0) >= budget_s:
            # Deliberate clean exit while we still control the process, so the
            # caller's export/commit steps still get to run. Everything
            # fetched so far is already durably written to the shards.
            stopped_on_budget = True
            print(f"\n[budget] hit --max-minutes {args.max_minutes:g} after "
                  f"{i - 1} of {len(todo)} models -- stopping cleanly so this "
                  f"run's progress gets committed. Re-run to resume.")
            break
        row = fetch_detail(sess, t["url"])
        # Every outcome here -- success, confirmed-gone, or exhausted retries --
        # means fetch_detail() returned control to us, i.e. the process is
        # alive and working. Only a genuine hang (no outcome at all, forever)
        # should look like "no progress" to the watchdog, so count all three.
        note_progress()
        if row is None:
            failed += 1
            print(f"  [{i}/{len(todo)}] FAILED {t['url']}")
        elif row.get("__missing__"):
            missing += 1
            # Record it as done anyway (id + url only) so a 410/423/404 that's
            # permanently gone doesn't get re-attempted, and re-counted as
            # "still to fetch", on every future run of this same category.
            why = row.get("__reason__") or f"HTTP {row.get('__status__')}"
            placeholder = {"id": t["id"], "url": t["url"],
                          "title": f"[unavailable: {why}]",
                          "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds")}
            writer.append(placeholder)
            print(f"  [{i}/{len(todo)}] unavailable ({why}): {t['url']}")
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
    if not stopped_on_budget and not _stop:
        # Fell off the end of `todo` under our own steam, so there is nothing
        # left for this category -- tell the workflow's chunk loop to move on.
        _signal_category_complete(args.category)
    path, n = export_csv(args.out_dir, args.category, stamp)
    if path:
        print(f"csv: {n} models -> {path}")
    if _stop:
        print("Interrupted -- re-run the same command to resume.")


if __name__ == "__main__":
    main()
