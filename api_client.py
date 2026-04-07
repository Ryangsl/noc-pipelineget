import logging
import requests
import config

logger = logging.getLogger(__name__)

TIMEOUT = 30


def _auth_headers() -> dict:
    return {"Authorization": f"Bearer {config.API_TOKEN}"}


def get_use_cases() -> dict:
    """Fetches all use cases from GET /core/commons/use-cases."""
    url = f"{config.API_BASE_URL}/core/commons/use-cases"
    logger.info("Fetching use cases from %s", url)

    response = requests.get(url, headers=_auth_headers(), timeout=TIMEOUT)
    response.raise_for_status()

    data = response.json()
    mapping = {}
    for item in data:
        value = item.get("value")
        if not value:
            continue
        network = item.get("network") or {}
        mapping[value] = {
            "label": item.get("label"),
            "network_value": network.get("value"),
            "network_label": network.get("label"),
            "vendor": item.get("vendor"),
        }

    logger.info("Loaded %d use cases", len(mapping))
    return mapping


def fetch_monitoring_page(data_from: str, data_to: str, page: int) -> list:
    """Fetches a single page from POST /core/history-io/monitoring."""
    url = f"{config.API_BASE_URL}/core/history-io/monitoring"
    payload = {
        "dataFrom": data_from,
        "dataTo": data_to,
        "page": page,
        "size": config.PAGE_SIZE,
        "useCase": None,
        "ticketId": None,
        "msgId": None,
        "typeEvent": None,
        "systemOrigin": None,
        "codResponses": None,
        "network": None,
        "vendor": None,
        "resultResponse": None,
        "sortDir": None,
        "sortfield": None,
    }

    logger.debug("POST %s page=%d", url, page)
    response = requests.post(url, json=payload, headers=_auth_headers(), timeout=TIMEOUT)
    response.raise_for_status()

    return response.json().get("content", [])


def fetch_all_monitoring(data_from: str, data_to: str, on_page=None):
    """Iterates all pages and yields each record from the monitoring endpoint.

    on_page: optional callback(page_number, records_in_page) called after each page fetch.
    """
    page = 0
    total = 0
    while True:
        records = fetch_monitoring_page(data_from, data_to, page)
        if not records:
            break
        if on_page:
            on_page(page, len(records))
        for record in records:
            yield record
        total += len(records)
        logger.info("Fetched page %d — %d records so far", page, total)
        if len(records) < config.PAGE_SIZE:
            break
        page += 1
