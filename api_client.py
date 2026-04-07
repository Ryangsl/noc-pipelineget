import logging
import re
import secrets
from urllib.parse import urlparse, parse_qs

import requests
import config

logger = logging.getLogger(__name__)

TIMEOUT = 30


def _extract_csrf(html: str) -> str | None:
    """Extracts the _csrf hidden input value from an HTML login form."""
    match = re.search(
        r'<input[^>]+name=["\']_csrf["\'][^>]+value=["\']([^"\']+)["\']'
        r'|<input[^>]+value=["\']([^"\']+)["\'][^>]+name=["\']_csrf["\']',
        html,
    )
    if match:
        return match.group(1) or match.group(2)
    return None


def _extract_code(url: str) -> str | None:
    """Extracts the `code` query parameter from a redirect URL."""
    parsed = urlparse(url)
    return parse_qs(parsed.query).get("code", [None])[0]


def get_token() -> str:
    """Authenticates via headless OAuth2 Authorization Code flow.

    Flow:
      1. GET /oauth2/authorize  → follows redirects to /login
      2. POST /login with credentials → follows redirects back to redirect_uri?code=...
      3. Extract authorization code from redirect URL
      4. POST /oauth2/token to exchange code for access_token
    """
    session = requests.Session()
    auth_base = config.API_AUTH_BASE_URL
    state = secrets.token_hex(16)

    # Step 1: initiate authorization flow
    authorize_url = f"{auth_base}/oauth2/authorize"
    params = {
        "response_type": "code",
        "client_id": config.API_CLIENT_ID,
        "redirect_uri": config.API_REDIRECT_URI,
        "state": state,
    }
    logger.info("Starting OAuth2 flow: %s", authorize_url)
    resp = session.get(authorize_url, params=params, timeout=TIMEOUT, allow_redirects=True)
    resp.raise_for_status()

    # Step 2: submit login form
    csrf = _extract_csrf(resp.text)
    login_data = {
        "username": config.API_USERNAME,
        "password": config.API_PASSWORD,
    }
    if csrf:
        login_data["_csrf"] = csrf
        logger.debug("CSRF token found")

    login_url = f"{auth_base}/login"
    logger.info("Submitting credentials to %s", login_url)
    resp = session.post(login_url, data=login_data, timeout=TIMEOUT, allow_redirects=False)

    # Step 3: follow redirects manually until we find the authorization code
    code = None
    for _ in range(15):
        location = resp.headers.get("Location", "")
        if not location:
            break
        logger.debug("Redirect → %s", location)

        code = _extract_code(location)
        if code:
            break

        resp = session.get(location, timeout=TIMEOUT, allow_redirects=False)

    if not code:
        raise RuntimeError(
            "Could not extract authorization code from OAuth2 flow. "
            "Check credentials (API_USERNAME / API_PASSWORD) and redirect URI."
        )

    # Step 4: exchange code for token
    token_url = f"{auth_base}/oauth2/token"
    logger.info("Exchanging authorization code for token")
    resp = session.post(
        token_url,
        data={
            "grant_type": "authorization_code",
            "client_id": config.API_CLIENT_ID,
            "code": code,
            "redirect_uri": config.API_REDIRECT_URI,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=TIMEOUT,
    )
    resp.raise_for_status()

    token = resp.json().get("access_token")
    if not token:
        raise ValueError("No access_token in token response")

    logger.info("Authentication successful")
    return token


def _auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def get_use_cases(token: str) -> dict:
    """Fetches all use cases from GET /core/commons/use-cases."""
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
    response = requests.post(url, json=payload, headers=_auth_headers(token), timeout=TIMEOUT)
    response.raise_for_status()

    return response.json().get("content", [])


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
