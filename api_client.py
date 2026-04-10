import getpass
import logging
from pathlib import Path

import requests
import config

logger = logging.getLogger(__name__)

TIMEOUT = 30
_MAX_TOKEN_RETRIES = 3  # How many times to ask for a new token before giving up


def _auth_headers() -> dict:
    # Always reads config.API_TOKEN at call time so in-memory renewals take effect immediately
    return {"Authorization": f"Bearer {config.API_TOKEN}"}


# ---------------------------------------------------------------------------
# Token renewal
# ---------------------------------------------------------------------------

def _prompt_new_token() -> str:
    """Displays renewal instructions and reads a new token from the terminal."""
    print(flush=True)
    print("=" * 62, flush=True)
    print("  TOKEN EXPIRADO — 401 Unauthorized", flush=True)
    print("=" * 62, flush=True)
    print("  Como obter um novo token:", flush=True)
    print("    1. Abra o browser e faça login no sistema", flush=True)
    print("    2. F12  →  aba Rede  →  localizar requisição 'token'", flush=True)
    print("    3. Aba Resposta  →  copiar valor de 'access_token'", flush=True)
    print("       (começa com eyJra..., ~1000 caracteres)", flush=True)
    print("=" * 62, flush=True)

    try:
        token = getpass.getpass("\nCole o novo token Bearer (entrada oculta): ").strip()
    except (EOFError, KeyboardInterrupt):
        print(flush=True)
        raise RuntimeError("Renovação de token cancelada pelo usuário")

    if not token:
        raise RuntimeError("Token não fornecido — impossível continuar")

    return token


def _save_token_to_env(token: str) -> None:
    """Overwrites API_TOKEN in .env so the next run also uses the new token."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        logger.warning(".env não encontrado — token não persistido em disco")
        return
    try:
        from dotenv import set_key
        set_key(str(env_path), "API_TOKEN", token)
        logger.info("Novo token salvo em .env")
    except Exception as exc:
        logger.warning("Não foi possível salvar o token em .env: %s", exc)


def renew_token() -> None:
    """Prompts for a new token, updates config.API_TOKEN in memory, and saves to .env."""
    new_token = _prompt_new_token()
    config.API_TOKEN = new_token
    _save_token_to_env(new_token)
    logger.info("Token renovado — retomando operação")


# ---------------------------------------------------------------------------
# HTTP helper with automatic 401 / token-renewal retry
# ---------------------------------------------------------------------------

def _do_request(method: str, url: str, **kwargs) -> requests.Response:
    """Executes an HTTP request, renewing the token transparently on 401.

    Retries up to _MAX_TOKEN_RETRIES times. Each retry prompts the user for a
    fresh token before re-attempting the exact same request.
    Raises HTTPError for any non-401 failure or after exhausting retries.
    """
    kwargs.setdefault("timeout", TIMEOUT)
    response: requests.Response | None = None

    for attempt in range(1 + _MAX_TOKEN_RETRIES):
        kwargs["headers"] = _auth_headers()
        response = getattr(requests, method)(url, **kwargs)

        if response.status_code != 401:
            response.raise_for_status()
            return response

        if attempt == _MAX_TOKEN_RETRIES:
            break  # Exhausted retries — raise below

        logger.warning(
            "401 Unauthorized (tentativa %d/%d) — token expirado",
            attempt + 1, 1 + _MAX_TOKEN_RETRIES,
        )
        renew_token()

    assert response is not None
    response.raise_for_status()
    return response  # unreachable; raise_for_status always throws for 401


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------

def get_use_cases() -> dict:
    """Fetches all use cases from GET /core/commons/use-cases."""
    url = f"{config.API_BASE_URL}/core/commons/use-cases"
    logger.info("Fetching use cases from %s", url)

    response = _do_request("get", url)
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
    response = _do_request("post", url, json=payload)
    return response.json().get("content", [])


def fetch_pages(data_from: str, data_to: str):
    """Yields (page_number, records_list) for each non-empty page.

    Stops automatically when the API returns an empty page or fewer records
    than PAGE_SIZE (last page). Callers receive full pages and can decide
    whether to stop early based on duplicate detection.
    """
    page = 0
    total = 0
    while True:
        records = fetch_monitoring_page(data_from, data_to, page)
        if not records:
            logger.info("Page %d returned empty — finished fetching %s → %s", page, data_from, data_to)
            break
        total += len(records)
        logger.info("Fetched page %d — %d records (running total: %d)", page, len(records), total)
        yield page, records
        if len(records) < config.PAGE_SIZE:
            break
        page += 1


def fetch_all_monitoring(data_from: str, data_to: str, on_page=None):
    """Iterates all pages and yields each record from the monitoring endpoint.

    on_page: optional callback(page_number, records_in_page) called after each page fetch.
    """
    for page, records in fetch_pages(data_from, data_to):
        if on_page:
            on_page(page, len(records))
        yield from records
