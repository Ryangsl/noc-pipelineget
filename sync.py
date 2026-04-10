#!/usr/bin/env python3
"""
NOC Pipeline — API to MySQL sync
Two independent operations per run:

  1. Historical scan  (INITIAL_DATE → now, chronological)
     Starts at INITIAL_DATE and advances forward BACKWARD_WINDOW_DAYS per run.
     Uses ASC sort, no early-stop — every record in the window is upserted.
     Tracks its cursor (forward_cursor) in sync_config so interruptions are safe.
     Logs % progress so you can see how much history is left.

  2. Incremental sync  (last_sync_date → now)
     Catches up with records created since the last run.
     Uses DESC sort so the newest records are processed first; stops early once
     a page is >= DUPLICATE_THRESHOLD% already-known records (avoids redundant
     API calls once we've reached already-synced territory).

Usage:
    python sync.py
"""

import logging
import sys
from datetime import datetime, timedelta

import api_client
import config
import db

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("sync")


# ---------------------------------------------------------------------------
# Core page-fetching helper
# ---------------------------------------------------------------------------

def _save_pages(conn, use_cases_map, data_from, data_to, *, stop_on_duplicates=False, sort_dir=None):
    """Fetches all pages in [data_from, data_to] and upserts them in batches.

    sort_dir : forwarded to the API.
               None  → API default (ASC / oldest-first, used by historical scan)
               "DESC"→ newest-first (used by incremental sync)

    stop_on_duplicates : if True, checks each page's IDs against the DB BEFORE
        upserting and breaks when existing/total >= DUPLICATE_THRESHOLD.
        Only meaningful for incremental sync where DESC sort guarantees that
        new records come first and duplicates appear at the tail.

    Returns total records upserted (new inserts + updates).
    """
    count = 0
    batch = []

    for page_num, page_records in api_client.fetch_pages(data_from, data_to, sort_dir=sort_dir):
        should_stop = False

        if stop_on_duplicates:
            record_ids = [r.get("id") for r in page_records if r.get("id")]
            if record_ids:
                existing = db.count_existing_ids(conn, record_ids)
                new_in_page = len(record_ids) - existing
                dup_ratio = existing / len(record_ids)
                logger.info(
                    "Page %d: %d new + %d already in DB (%.0f%% duplicate)",
                    page_num, new_in_page, existing, dup_ratio * 100,
                )
                if dup_ratio >= config.DUPLICATE_THRESHOLD:
                    logger.info(
                        "Page %d: %.0f%% duplicate threshold reached — "
                        "all newer records already processed, stopping",
                        page_num, dup_ratio * 100,
                    )
                    should_stop = True

        batch.extend(page_records)
        if len(batch) >= config.BATCH_SIZE:
            db.upsert_records_batch(conn, batch, use_cases_map)
            count += len(batch)
            logger.info("Upserted %d records so far...", count)
            batch = []

        if should_stop:
            break

    if batch:
        db.upsert_records_batch(conn, batch, use_cases_map)
        count += len(batch)

    return count


# ---------------------------------------------------------------------------
# Operation 1 — Historical scan (INITIAL_DATE → now, ASC)
# ---------------------------------------------------------------------------

def run_historical_scan(conn, use_cases_map):
    """Scans history chronologically from INITIAL_DATE toward now.

    Advances forward_cursor by BACKWARD_WINDOW_DAYS each run.
    ASC sort, no early-stop: every record in the window is upserted.
    When the cursor reaches 'now' the scan is complete and this becomes a no-op.
    """
    now_dt     = datetime.now()
    initial_dt = datetime.strptime(config.INITIAL_DATE, "%Y-%m-%dT%H:%M")

    cursor_str = db.get_forward_cursor(conn)
    if cursor_str is None:
        cursor_str = config.INITIAL_DATE
        logger.info("Historical scan: starting from INITIAL_DATE %s", cursor_str)

    cursor_dt = datetime.strptime(cursor_str, "%Y-%m-%dT%H:%M")

    if cursor_dt >= now_dt:
        logger.info("Historical scan: cursor at %s — all history covered, nothing to do", cursor_str)
        return 0

    window_end_dt  = min(cursor_dt + timedelta(days=config.BACKWARD_WINDOW_DAYS), now_dt)
    data_from      = cursor_str
    data_to        = window_end_dt.strftime("%Y-%m-%dT%H:%M")

    total_days     = max(1, (now_dt - initial_dt).days)
    done_days      = max(0, (cursor_dt - initial_dt).days)
    remaining_days = max(0, (now_dt - window_end_dt).days)
    pct_done       = done_days / total_days * 100

    logger.info(
        "=== Historical scan: %s → %s | %.1f%% complete | %d days remaining ===",
        data_from, data_to, pct_done, remaining_days,
    )

    # ASC sort (oldest first), no early-stop — upsert everything in window
    count = _save_pages(conn, use_cases_map, data_from, data_to,
                        stop_on_duplicates=False, sort_dir=None)

    db.set_forward_cursor(conn, data_to)
    logger.info(
        "Historical scan done — %d records upserted | cursor → %s | %d days to go",
        count, data_to, remaining_days,
    )
    return count


# ---------------------------------------------------------------------------
# Operation 2 — Incremental sync (last_sync_date → now, DESC)
# ---------------------------------------------------------------------------

def run_incremental_sync(conn, use_cases_map):
    """Syncs records created since last_sync_date up to now.

    DESC sort ensures newest records are seen first; duplicate-stop avoids
    re-fetching territory that was already fully synced.
    Updates last_sync_date on success.
    """
    data_from = db.get_last_sync_date(conn) or config.INITIAL_DATE
    data_to   = datetime.now().strftime("%Y-%m-%dT%H:%M")
    logger.info("=== Incremental sync: %s → %s ===", data_from, data_to)

    count = _save_pages(conn, use_cases_map, data_from, data_to,
                        stop_on_duplicates=True, sort_dir="DESC")
    db.set_last_sync_date(conn, data_to)
    logger.info("Incremental sync done — %d records upserted, last_sync_date → %s", count, data_to)
    return count


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run():
    logger.info("=== NOC Sync started ===")
    conn = None
    try:
        conn = db.get_connection()
        db.init_tables(conn)

        if not config.API_TOKEN:
            raise ValueError(
                "API_TOKEN is not set in .env — copy the Bearer token from the browser"
            )

        # Diagnostic snapshot — printed before any work so gaps are visible
        stats      = db.get_db_stats(conn)
        last_sync  = db.get_last_sync_date(conn)
        fwd_cursor = db.get_forward_cursor(conn)
        logger.info(
            "DB: %d records | insert_date range: %s → %s",
            stats["count"],
            stats["min_date"] or "empty",
            stats["max_date"] or "empty",
        )
        logger.info(
            "State: last_sync=%s | forward_cursor=%s | INITIAL_DATE=%s",
            last_sync  or "none (will use INITIAL_DATE)",
            fwd_cursor or "none (will start from INITIAL_DATE)",
            config.INITIAL_DATE,
        )

        # Sync use cases (technology / vendor mapping)
        use_cases_map = api_client.get_use_cases()
        db.upsert_use_cases(conn, use_cases_map)

        # 1. Historical scan — chronological fill from INITIAL_DATE forward
        run_historical_scan(conn, use_cases_map)

        # 2. Incremental sync — catch up with data since last run
        run_incremental_sync(conn, use_cases_map)

        logger.info("=== NOC Sync completed successfully ===")

    except Exception as exc:
        logger.error("Sync failed: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if conn and conn.is_connected():
            conn.close()


if __name__ == "__main__":
    run()
