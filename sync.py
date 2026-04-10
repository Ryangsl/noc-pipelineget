#!/usr/bin/env python3
"""
NOC Pipeline — API to MySQL sync
Dual-direction sync per execution:

  1. Forward sync  (newest → now)
     Fetches records from last_sync_date up to now.
     Stops early once a page reaches DUPLICATE_THRESHOLD% of already-known
     records, avoiding wasteful API calls on data we've already ingested.
     Updates last_sync_date on success.

  2. Backward sync (oldest → INITIAL_DATE)
     After the forward pass, fetches one time-window of historical data going
     further back from the oldest frontier we've reached so far.
     Each run covers BACKWARD_WINDOW_DAYS days. Runs until the frontier
     reaches INITIAL_DATE, at which point backward sync is complete.
     Updates oldest_sync_date (in sync_config) on success.

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


def _save_pages(conn, use_cases_map, data_from, data_to, *, stop_on_duplicates=False):
    """Fetches all pages in [data_from, data_to] and upserts them in batches.

    If stop_on_duplicates=True, checks each page's IDs against the DB BEFORE
    upserting. When the fraction of already-existing records reaches
    DUPLICATE_THRESHOLD, stops fetching further pages (the upsert for the
    current page still runs so any updates are applied).

    Returns the total number of records saved.
    """
    count = 0
    batch = []

    for page_num, page_records in api_client.fetch_pages(data_from, data_to):
        should_stop = False

        if stop_on_duplicates:
            record_ids = [r.get("id") for r in page_records if r.get("id")]
            if record_ids:
                existing = db.count_existing_ids(conn, record_ids)
                dup_ratio = existing / len(record_ids)
                logger.debug(
                    "Page %d duplicate check: %d/%d already in DB (%.0f%%)",
                    page_num, existing, len(record_ids), dup_ratio * 100,
                )
                if dup_ratio >= config.DUPLICATE_THRESHOLD:
                    logger.info(
                        "Page %d: %.0f%% duplicates (threshold %.0f%%) — "
                        "stopping forward sync early to save API calls",
                        page_num, dup_ratio * 100, config.DUPLICATE_THRESHOLD * 100,
                    )
                    should_stop = True

        batch.extend(page_records)
        if len(batch) >= config.BATCH_SIZE:
            db.upsert_records_batch(conn, batch, use_cases_map)
            count += len(batch)
            logger.info("Saved %d records so far...", count)
            batch = []

        if should_stop:
            break

    if batch:
        db.upsert_records_batch(conn, batch, use_cases_map)
        count += len(batch)

    return count


def run_forward_sync(conn, use_cases_map):
    """Syncs records from last_sync_date to now, with early-stop on duplicates.

    Updates last_sync_date in sync_state on success.
    Returns the number of records saved.
    """
    data_from = db.get_last_sync_date(conn) or config.INITIAL_DATE
    data_to = datetime.now().strftime("%Y-%m-%dT%H:%M")
    logger.info("=== Forward sync: %s → %s ===", data_from, data_to)

    count = _save_pages(conn, use_cases_map, data_from, data_to, stop_on_duplicates=True)
    db.set_last_sync_date(conn, data_to)
    logger.info("Forward sync done — %d records saved, last_sync_date → %s", count, data_to)
    return count


def run_backward_sync(conn, use_cases_map):
    """Fetches one historical window going backward from the oldest sync frontier.

    Frontier (oldest_sync_date) is stored in sync_config and initialized from
    MIN(insert_date) of records already in DB so we start exactly where the
    existing data begins and work backwards toward INITIAL_DATE.

    Each call covers BACKWARD_WINDOW_DAYS days. Does nothing once the frontier
    has reached INITIAL_DATE.

    Returns the number of records saved.
    """
    initial_dt = datetime.strptime(config.INITIAL_DATE, "%Y-%m-%dT%H:%M")

    oldest_str = db.get_oldest_sync_date(conn)
    if oldest_str is None:
        oldest_str = db.get_oldest_record_date(conn)
        if oldest_str is None:
            logger.info("Backward sync: no records in DB yet — skipping")
            return 0
        logger.info("Backward sync: initializing frontier from oldest DB record → %s", oldest_str)
        db.set_oldest_sync_date(conn, oldest_str)

    oldest_dt = datetime.strptime(oldest_str, "%Y-%m-%dT%H:%M")

    if oldest_dt <= initial_dt:
        logger.info(
            "Backward sync: frontier %s already at/before INITIAL_DATE %s — nothing to do",
            oldest_str, config.INITIAL_DATE,
        )
        return 0

    data_to_dt = oldest_dt
    data_from_dt = oldest_dt - timedelta(days=config.BACKWARD_WINDOW_DAYS)
    if data_from_dt < initial_dt:
        data_from_dt = initial_dt

    data_from = data_from_dt.strftime("%Y-%m-%dT%H:%M")
    data_to = data_to_dt.strftime("%Y-%m-%dT%H:%M")
    remaining_days = max(0, (data_from_dt - initial_dt).days)

    logger.info(
        "=== Backward sync: %s → %s (window %d days, %d days still to go after this) ===",
        data_from, data_to, config.BACKWARD_WINDOW_DAYS, remaining_days,
    )

    count = _save_pages(conn, use_cases_map, data_from, data_to, stop_on_duplicates=False)
    db.set_oldest_sync_date(conn, data_from)
    logger.info(
        "Backward sync done — %d records saved, frontier → %s (%d days remaining to INITIAL_DATE)",
        count, data_from, remaining_days,
    )
    return count


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

        # Step 1: sync use cases
        use_cases_map = api_client.get_use_cases()
        db.upsert_use_cases(conn, use_cases_map)

        # Step 2: forward sync — bring the latest records up to now
        run_forward_sync(conn, use_cases_map)

        # Step 3: backward sync — fill one window of historical data
        run_backward_sync(conn, use_cases_map)

        logger.info("=== NOC Sync completed successfully ===")

    except Exception as exc:
        logger.error("Sync failed: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if conn and conn.is_connected():
            conn.close()


if __name__ == "__main__":
    run()
