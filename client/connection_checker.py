import logging
import requests


log = logging.getLogger(__name__)


def verify_connection(label: str, base_url: str, session: requests.Session) -> bool:
    """
    Ping /rest/api/3/myself to validate credentials before migrating.
    """
    try:
        resp = session.get(f"{base_url}/rest/api/3/myself", timeout=10)
        if resp.status_code == 200:
            user = resp.json().get("displayName", "unknown")
            log.info("[%s] Connected as: %s", label, user)
            return True
        log.error("[%s] Auth failed (HTTP %d). Check credentials.", label, resp.status_code)
    except Exception as exc:
        log.error("[%s] Could not reach %s: %s", label, base_url, exc)
    return False

