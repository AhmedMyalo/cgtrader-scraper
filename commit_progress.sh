#!/usr/bin/env bash
# Commit + push whatever scraped data is currently on disk.
#
# Extracted from .github/workflows/scrape.yml so the workflow can call it
# REPEATEDLY (every scrape chunk) instead of once at the very end. Committing
# only at the end meant any abrupt death of the job lost the entire run's
# work, and we hit four separate causes of exactly that:
#   1. push rejected by a concurrent run          (2026-08-08)
#   2. SIGKILL when Actions' timeout beat us      (2026-08-10)
#   3. rebase conflict + retry loop that couldn't resolve it (2026-08-12)
#   4. the runner itself dying mid-scrape         (2026-08-15, architectural,
#      1h50m in: step left "in_progress", job marked failure, nothing pushed)
# Periodic commits bound the loss for ALL of these -- including whatever the
# fifth cause turns out to be -- to one chunk instead of five hours.
#
# Usage: commit_progress.sh <category-label>
set -u

LABEL="${1:-progress}"

git config user.name "cgtrader-scraper-bot"
git config user.email "actions@users.noreply.github.com"

# Only the resumable source-of-truth data, never the generated CSVs (some
# categories' full CSV is well over GitHub's 100MB per-file limit, which
# would break the push). CSVs go out as the run's artifact instead.
git add cgtrader_deep/raw cgtrader_deep/plan.json cgtrader_deep/state.json \
        'cgtrader_details/*_details/**' 2>/dev/null || true

if git diff --staged --quiet; then
  echo "[commit] nothing new to commit for $LABEL"
  exit 0
fi

git commit -q -m "scrape progress ($LABEL): $(date -u +'%Y-%m-%d %H:%M UTC')"

# The shard files are append-mostly JSONL and every reader dedupes by model
# id (load_rows / export_csv), so when two commits both touched the same
# category's shards the correct resolution is the UNION of both sides, never
# "pick one". Taking :2: and :3: for each unmerged path and sort -u'ing them
# together is what makes the rebase-and-retry loop below actually able to
# make progress -- the earlier version just aborted and retried the identical
# rebase, hit the identical conflict all 8 times, and stranded the commit.
resolve_conflicts() {
  local f
  local any=0
  for f in $(git diff --name-only --diff-filter=U); do
    any=1
    echo "[commit]   union-merging conflicted $f"
    { git show ":2:$f" 2>/dev/null; git show ":3:$f" 2>/dev/null; } \
      | sort -u > "$f.union"
    mv "$f.union" "$f"
    git add "$f"
  done
  return $((1 - any))
}

for attempt in 1 2 3 4 5 6 7 8; do
  if git push -q; then
    echo "[commit] pushed $LABEL on attempt $attempt"
    exit 0
  fi
  echo "[commit] push rejected (attempt $attempt/8) -- fetching and rebasing"
  git fetch -q origin main
  if ! git rebase origin/main; then
    if resolve_conflicts; then
      GIT_EDITOR=true git rebase --continue >/dev/null 2>&1 || git rebase --abort
    else
      git rebase --abort
      echo "[commit] rebase failed with no conflicted files -- retrying"
    fi
  fi
  sleep $((attempt * 8 + RANDOM % 10))
done

echo "::error::push still failing after 8 attempts for $LABEL -- this chunk's progress is committed LOCALLY on the runner but could not reach origin/main."
exit 1
