import json
import logging
import mysql.connector
import config

logger = logging.getLogger(__name__)

DDL_USE_CASES = """
CREATE TABLE IF NOT EXISTS use_cases (
    use_case      VARCHAR(150) PRIMARY KEY,
    label         VARCHAR(255),
    network_value VARCHAR(100),
    network_label VARCHAR(100),
    vendor        VARCHAR(100),
    synced_at     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
"""

DDL_HISTORY_IO = """
CREATE TABLE IF NOT EXISTS history_io (
    id            VARCHAR(36)  PRIMARY KEY,
    insert_date   DATETIME,
    cod_response  INT,
    result        TEXT,
    msg_id        VARCHAR(36),
    ticket_id     VARCHAR(100),
    use_cases     JSON,
    type_event    VARCHAR(50),
    system_origin VARCHAR(100),
    micro_service JSON,
    technology    VARCHAR(100),
    vendor        VARCHAR(100),
    synced_at     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_insert_date (insert_date),
    INDEX idx_ticket_id   (ticket_id),
    INDEX idx_type_event  (type_event),
    INDEX idx_technology  (technology)
)
"""

DDL_SYNC_STATE = """
CREATE TABLE IF NOT EXISTS sync_state (
    id             INT PRIMARY KEY AUTO_INCREMENT,
    last_sync_date DATETIME NOT NULL,
    updated_at     DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
"""

DDL_SYNC_CONFIG = """
CREATE TABLE IF NOT EXISTS sync_config (
    key_name   VARCHAR(50) PRIMARY KEY,
    value      VARCHAR(100),
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
"""


def get_connection():
    return mysql.connector.connect(
        host=config.MYSQL_HOST,
        port=config.MYSQL_PORT,
        database=config.MYSQL_DB,
        user=config.MYSQL_USER,
        password=config.MYSQL_PASSWORD,
    )


def init_tables(conn):
    cursor = conn.cursor()
    for ddl in (DDL_USE_CASES, DDL_HISTORY_IO, DDL_SYNC_STATE, DDL_SYNC_CONFIG):
        cursor.execute(ddl)
    conn.commit()
    cursor.close()
    logger.info("Tables initialized")


def upsert_use_cases(conn, use_cases_map: dict):
    sql = """
        INSERT INTO use_cases (use_case, label, network_value, network_label, vendor)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            label         = VALUES(label),
            network_value = VALUES(network_value),
            network_label = VALUES(network_label),
            vendor        = VALUES(vendor),
            synced_at     = CURRENT_TIMESTAMP
    """
    cursor = conn.cursor()
    rows = [
        (
            use_case,
            info["label"],
            info["network_value"],
            info["network_label"],
            info["vendor"],
        )
        for use_case, info in use_cases_map.items()
    ]
    cursor.executemany(sql, rows)
    conn.commit()
    cursor.close()
    logger.info("Upserted %d use cases into DB", len(rows))


_UPSERT_SQL = """
    INSERT INTO history_io
        (id, insert_date, cod_response, result, msg_id, ticket_id,
         use_cases, type_event, system_origin, micro_service, technology, vendor)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        insert_date   = VALUES(insert_date),
        cod_response  = VALUES(cod_response),
        result        = VALUES(result),
        msg_id        = VALUES(msg_id),
        ticket_id     = VALUES(ticket_id),
        use_cases     = VALUES(use_cases),
        type_event    = VALUES(type_event),
        system_origin = VALUES(system_origin),
        micro_service = VALUES(micro_service),
        technology    = VALUES(technology),
        vendor        = VALUES(vendor),
        synced_at     = CURRENT_TIMESTAMP
"""


def _record_to_row(record: dict, use_cases_map: dict) -> tuple:
    use_cases_list = record.get("useCases") or []
    primary_use_case = use_cases_list[0] if use_cases_list else None
    uc_info = use_cases_map.get(primary_use_case, {}) if primary_use_case else {}
    return (
        record.get("id"),
        record.get("insertDate"),
        record.get("codResponse"),
        record.get("result"),
        record.get("msgId"),
        record.get("ticketId"),
        json.dumps(use_cases_list),
        record.get("typeEvent"),
        record.get("systemOrigin"),
        json.dumps(record.get("microService") or []),
        uc_info.get("network_value"),
        uc_info.get("vendor"),
    )


def upsert_records_batch(conn, records: list, use_cases_map: dict):
    """Inserts/updates a batch of records in a single transaction."""
    if not records:
        return
    rows = [_record_to_row(r, use_cases_map) for r in records]
    cursor = conn.cursor()
    cursor.executemany(_UPSERT_SQL, rows)
    conn.commit()
    cursor.close()


def get_last_sync_date(conn) -> str | None:
    cursor = conn.cursor()
    cursor.execute("SELECT last_sync_date FROM sync_state ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()
    cursor.close()
    if row:
        return row[0].strftime("%Y-%m-%dT%H:%M")
    return None


def set_last_sync_date(conn, date_str: str):
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO sync_state (last_sync_date) VALUES (%s)",
        (date_str,),
    )
    conn.commit()
    cursor.close()
    logger.info("Updated last_sync_date to %s", date_str)


def count_existing_ids(conn, record_ids: list) -> int:
    """Returns how many of the given record IDs already exist in history_io."""
    if not record_ids:
        return 0
    placeholders = ",".join(["%s"] * len(record_ids))
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT COUNT(*) FROM history_io WHERE id IN ({placeholders})",
        record_ids,
    )
    count = cursor.fetchone()[0]
    cursor.close()
    return count


def get_oldest_sync_date(conn) -> str | None:
    """Returns the backward-sync frontier date stored in sync_config."""
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM sync_config WHERE key_name = 'oldest_sync_date'")
    row = cursor.fetchone()
    cursor.close()
    return row[0] if row else None


def set_oldest_sync_date(conn, date_str: str):
    """Persists the backward-sync frontier in sync_config."""
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO sync_config (key_name, value) VALUES ('oldest_sync_date', %s)
        ON DUPLICATE KEY UPDATE value = VALUES(value)
        """,
        (date_str,),
    )
    conn.commit()
    cursor.close()
    logger.info("Updated oldest_sync_date to %s", date_str)


def get_oldest_record_date(conn) -> str | None:
    """Returns the MIN(insert_date) currently in history_io (used to seed backward sync)."""
    cursor = conn.cursor()
    cursor.execute("SELECT MIN(insert_date) FROM history_io")
    row = cursor.fetchone()
    cursor.close()
    if row and row[0]:
        return row[0].strftime("%Y-%m-%dT%H:%M")
    return None
