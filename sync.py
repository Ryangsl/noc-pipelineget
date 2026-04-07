#!/usr/bin/env python3
"""
NOC Pipeline — API to MySQL sync
Runs incrementally: fetches data from the last synced date up to now.

Usage:
    python sync.py
"""

import logging
import sys
from datetime import datetime

import api_client
import config
import db

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("sync")


def run():
    logger.info("=== NOC Sync started ===")
    conn = None
    try:
        conn = db.get_connection()
        db.init_tables(conn)

        if not config.API_TOKEN:
            raise ValueError("API_TOKEN is not set in .env — copy the Bearer token from the browser")

        # Step 1: sync use cases
        use_cases_map = api_client.get_use_cases()
        db.upsert_use_cases(conn, use_cases_map)

        # Step 2: determine date range
        data_from = db.get_last_sync_date(conn) or config.INITIAL_DATE
        data_to = datetime.now().strftime("%Y-%m-%dT%H:%M")
        logger.info("Sync window: %s → %s", data_from, data_to)

        # Step 3: fetch and save records in batches
        count = 0
        batch = []
        for record in api_client.fetch_all_monitoring(data_from, data_to):
            batch.append(record)
            if len(batch) >= config.BATCH_SIZE:
                db.upsert_records_batch(conn, batch, use_cases_map)
                count += len(batch)
                logger.info("Saved %d records so far...", count)
                batch = []

        if batch:
            db.upsert_records_batch(conn, batch, use_cases_map)
            count += len(batch)

        logger.info("Saved %d records to history_io", count)

        # Step 4: update sync state
        db.set_last_sync_date(conn, data_to)

        logger.info("=== NOC Sync completed successfully ===")

    except Exception as exc:
        logger.error("Sync failed: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        if conn and conn.is_connected():
            conn.close()


if __name__ == "__main__":
    run()
