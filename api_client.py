import logging
import requests
import config

logger = logging.getLogger(__name__)

TIMEOUT = 30


def get_token() -> str:
    """Authenticates via OAuth2 Password Grant and returns the access_token."""
    logger.info("Authenticating at %s", config.API_AUTH_URL)
    response = requests.post(
        config.API_AUTH_URL,
        data={
            "grant_type": "password",
            "client_id": config.API_CLIENT_ID,
            "username": config.API_USERNAME,
            "password": config.API_PASSWORD,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=TIMEOUT,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise ValueError("No access_token in auth response")
    logger.info("Authentication successful")
    return token


def _auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def get_use_cases(token: str) -> dict:
    """Fetches all use cases from GET /core/commons/use-cases.

    Returns a dict mapping useCase value -> {technology, vendor, label, network_label}.
    """
    url = f"{config.API_BASE_URL}/core/commons/use-cases"
    logger.info("Fetching use cases from %s", url)

    response = requests.get(url, headers=_auth_headers(token), timeout=TIMEOUT)
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


def fetch_monitoring_page(token: str, data_from: str, data_to: str, page: int) -> list:
    """Fetches a single page from POST /core/history-io/monitoring.

    Returns the list of records in 'content', or empty list if no more data.
    """
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
    response = requests.post(url, json=payload, headers=_auth_headers(token), timeout=TIMEOUT)
    response.raise_for_status()

    data = response.json()
    return data.get("content", [])


def fetch_all_monitoring(token: str, data_from: str, data_to: str):
    """Iterates all pages and yields each record from the monitoring endpoint."""
    page = 0
    total = 0
    while True:
        records = fetch_monitoring_page(token, data_from, data_to, page)
        if not records:
            break
        for record in records:
            yield record
        total += len(records)
        logger.info("Fetched page %d — %d records so far", page, total)
        if len(records) < config.PAGE_SIZE:
            break
        page += 1
