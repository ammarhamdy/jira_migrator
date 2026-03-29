import logging
import time
from typing import Optional
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import requests
from config.settings import CONFIG


log = logging.getLogger(__name__)


def _build_session(email: str, token: str) -> requests.Session:
    """
    Create a requests.Session pre-configured with:
      • Basic Auth (email + API token)
      • Automatic retry on connection errors and 5xx responses
      • JSON content-type header
    """
    session = requests.Session()
    session.auth = (email, token)
    session.headers.update({"Accept": "application/json"})

    retry_strategy = Retry(
        total=CONFIG["MAX_RETRIES"],
        backoff_factor=8,  # 2, 4, 8 … seconds between retries
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET", "POST", "PUT"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


# One session per Jira instance
_src_session: Optional[requests.Session] = None
_tgt_session: Optional[requests.Session] = None


def get_source_session() -> requests.Session:
    global _src_session
    if _src_session is None:
        _src_session = _build_session(CONFIG["SOURCE_EMAIL"], CONFIG["SOURCE_API_TOKEN"])
    return _src_session


def get_target_session() -> requests.Session:
    global _tgt_session
    if _tgt_session is None:
        _tgt_session = _build_session(CONFIG["TARGET_EMAIL"], CONFIG["TARGET_API_TOKEN"])
    return _tgt_session


def _handle_rate_limit(response: requests.Response, attempt: int) -> None:
    """Sleep when the server returns HTTP 429 (Too Many Requests)."""
    if response.status_code == 429:
        wait = CONFIG["RATE_LIMIT_WAIT"] * (attempt + 1)
        log.warning("Rate limited (429). Waiting %d seconds …", wait)
        time.sleep(wait)

