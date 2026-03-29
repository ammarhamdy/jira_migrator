import logging
import re
from typing import Optional
from client.jira_client import get_target_session
from config.settings import CONFIG


log = logging.getLogger(__name__)


def _safe(value: str | None, fallback: str = "") -> str:
    """Return value if non-empty, else fallback."""
    if not value or value.strip() in ("", "nan"):
        return fallback
    return value.strip()


def _parse_attachment_cell(cell_value: str) -> list[dict]:
    """
    Parse a Jira attachment cell.  Each attachment is semicolon-delimited:
        <date>;<user_id>;<filename>;<url>

    Returns a list of dicts with keys: filename, url
    """
    attachments = []
    if not _safe(cell_value):
        return attachments

    # Multiple attachments in a single cell are newline-separated
    for entry in cell_value.splitlines():
        parts = entry.strip().split(";")
        if len(parts) >= 4:
            filename = parts[2].strip()
            url = parts[3].strip()
            if filename and url.startswith("http"):
                attachments.append({"filename": filename, "url": url})
    return attachments


def _collect_all_attachments(row: dict) -> list[dict]:
    """
    Collect attachments from all 'Attachment*' columns in a CSV row.
    Returns a de-duplicated list of {filename, url} dicts.
    """
    seen_urls: set[str] = set()
    results = []
    for key, value in row.items():
        if key.startswith("Attachment") and _safe(value):
            for att in _parse_attachment_cell(value):
                if att["url"] not in seen_urls:
                    seen_urls.add(att["url"])
                    results.append(att)
    return results


def _resolve_assignee_account_id(display_name: str, assignee_id: str) -> Optional[str]:
    """
    Return the Atlassian account ID to use for the assignee in the
    target project.  Strategy:
      1. If the CSV contains an Assignee Id and it looks like a valid
         account ID, return it directly (works when source == target cloud).
      2. Otherwise search the target project's assignable users by
         display name.
      3. Return None if no match found (issue will be unassigned).
    """
    if _safe(assignee_id) and re.match(r"^\d+:[a-f0-9\-]{36}$", assignee_id):
        return assignee_id.strip()

    if not _safe(display_name):
        return None

    session = get_target_session()
    url = f"{CONFIG['TARGET_URL']}/rest/api/3/user/assignable/search"
    params = {
        "project": CONFIG["TARGET_PROJECT_KEY"],
        "query": display_name,
        "maxResults": 5,
    }
    try:
        resp = session.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            users = resp.json()
            if users:
                return users[0]["accountId"]
    except Exception as exc:
        log.debug("Could not resolve assignee '%s': %s", display_name, exc)
    return None
