import logging
import time
from typing import Optional
import requests
from client.field_helber import _safe, _resolve_assignee_account_id
from client.jira_client import _handle_rate_limit, get_target_session
from client.mapping import ISSUE_TYPE_MAP, PRIORITY_MAP
from config.settings import CONFIG


log = logging.getLogger(__name__)


def build_issue_payload(row: dict) -> dict:
    """
    Translate a CSV row into a Jira REST API v3 issue-creation payload.

    Fields mapped:
      - summary         (required)
      - description     (ADF plain-text node)
      - issuetype
      - priority
      - labels          (from Status Category as a label)
      - assignee        (resolved to accountId)
      - duedate
      - story points    (custom field 10016 / 10028)
      - environment
      - epic name       (custom field 10011, if populated)
    """
    summary = _safe(row.get("Summary"), fallback="(no summary)")
    issue_type_raw = _safe(row.get("Issue Type"), "Task")
    issue_type = ISSUE_TYPE_MAP.get(issue_type_raw, issue_type_raw)
    priority_raw = _safe(row.get("Priority"), "Medium")
    priority = PRIORITY_MAP.get(priority_raw, priority_raw)
    description_text = _safe(row.get("Description"))
    environment_text = _safe(row.get("Environment"))
    due_date = _safe(row.get("Due date"))  # e.g. "25/Mar/26"
    assignee_name = _safe(row.get("Assignee"))
    assignee_id_raw = _safe(row.get("Assignee Id"))

    # ── Build Atlassian Document Format (ADF) description ──────
    adf_content = []
    if description_text:
        adf_content.append({
            "type": "paragraph",
            "content": [{"type": "text", "text": description_text}],
        })
    if environment_text:
        adf_content.append({
            "type": "paragraph",
            "content": [
                {"type": "text", "text": "Environment: ",
                 "marks": [{"type": "strong"}]},
                {"type": "text", "text": environment_text},
            ],
        })
    # Add original issue key as a reference note
    orig_key = _safe(row.get("Issue key"))
    if orig_key:
        adf_content.append({
            "type": "paragraph",
            "content": [
                {"type": "text", "text": f"[Migrated from {orig_key}]",
                 "marks": [{"type": "em"}]},
            ],
        })

    adf_description = {
        "version": 1,
        "type": "doc",
        "content": adf_content if adf_content else [
            {"type": "paragraph", "content": []}
        ],
    }

    # ── Core fields ─────────────────────────────────────────────
    fields: dict = {
        "project": {"key": CONFIG["TARGET_PROJECT_KEY"]},
        "summary": summary,
        "issuetype": {"name": issue_type},
        "priority": {"name": priority},
        "description": adf_description,
    }

    # ── Optional: assignee ──────────────────────────────────────
    if assignee_name or assignee_id_raw:
        account_id = _resolve_assignee_account_id(assignee_name, assignee_id_raw)
        if account_id:
            fields["assignee"] = {"accountId": account_id}

    # ── Optional: due date (Jira expects YYYY-MM-DD) ────────────
    if due_date:
        parsed_date = _parse_jira_date(due_date)
        if parsed_date:
            fields["duedate"] = parsed_date

    # ── Optional: labels – tag with source status ───────────────
    status = _safe(row.get("Status"))
    if status and status.lower() not in ("to do", ""):
        fields["labels"] = [status.replace(" ", "_")]

    # ── Optional: story points (try both common custom field IDs) ─
    story_points_raw = _safe(row.get("Custom field (Story Points)")) or \
                       _safe(row.get("Custom field (Story point estimate)"))
    if story_points_raw:
        try:
            sp = float(story_points_raw)
            # customfield_10016 = Story Points in most cloud projects
            fields["customfield_10016"] = sp
        except ValueError:
            pass

    # ── Optional: Epic Name ─────────────────────────────────────
    epic_name = _safe(row.get("Custom field (Epic Name)"))
    if epic_name:
        fields["customfield_10011"] = epic_name  # Epic Name field

    # ── Optional: Start date ────────────────────────────────────
    start_date = _safe(row.get("Custom field (Start date)"))
    if start_date:
        parsed_start = _parse_jira_date(start_date)
        if parsed_start:
            fields["customfield_10015"] = parsed_start  # Start date field ID

    return {"fields": fields}


def _parse_jira_date(date_str: str) -> Optional[str]:
    """
    Convert Jira export date strings to ISO 8601 (YYYY-MM-DD).

    Handles formats like:
      "25/Mar/26 2:14 PM"  →  "2026-03-25"
      "2026-03-25"         →  "2026-03-25"
    """
    import datetime
    for fmt in ("%d/%b/%y %I:%M %p", "%d/%b/%y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            dt = datetime.datetime.strptime(date_str.strip(), fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    log.debug("Could not parse date: %s", date_str)
    return None


def create_issue(row: dict) -> Optional[str]:
    """
    Create a single issue in the target Jira project.

    Returns the new issue key (e.g. "TGT-42") on success, or None on failure.
    """
    session = get_target_session()
    url = f"{CONFIG['TARGET_URL']}/rest/api/3/issue"
    payload = build_issue_payload(row)
    orig_key = _safe(row.get("Issue key"), "?")

    for attempt in range(CONFIG["MAX_RETRIES"]):
        try:
            resp = session.post(url, json=payload, timeout=20)
            _handle_rate_limit(resp, attempt)

            if resp.status_code == 201:
                new_key = resp.json()["key"]
                log.info("  ✓  %s  →  %s  |  %s",
                         orig_key, new_key, row.get("Summary", "")[:60])
                return new_key

            elif resp.status_code == 429:
                continue  # already slept in _handle_rate_limit

            else:
                log.error("  ✗  %s failed (HTTP %d): %s",
                          orig_key, resp.status_code, resp.text[:300])
                return None

        except requests.RequestException as exc:
            log.warning("  ⚠  %s  attempt %d/%d failed: %s",
                        orig_key, attempt + 1, CONFIG["MAX_RETRIES"], exc)
            time.sleep(2 ** attempt)

    log.error("  ✗  %s  exhausted all retries.", orig_key)
    return None