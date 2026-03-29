import time
from pathlib import Path
from typing import Optional
import requests
from requests import Response
from client.jira_client import get_target_session, _handle_rate_limit, get_source_session
from config.settings import CONFIG
import logging


log = logging.getLogger(__name__)


def download_attachment(filename: str, url: str, dest_dir: Path) -> Optional[Path]:
    """
    Download a single attachment from the source Jira instance.

    Returns the local Path of the saved file, or None on failure.
    """
    session = get_source_session()
    dest_path = dest_dir / filename

    # Skip re-download if already present
    if dest_path.exists():
        return dest_path

    for attempt in range(CONFIG["MAX_RETRIES"]):
        try:
            resp: Response = session.get(url, timeout=60, stream=True)
            _handle_rate_limit(resp, attempt)

            if resp.status_code == 200:
                with open(dest_path, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=8192):
                        fh.write(chunk)
                log.debug("    ↓  Downloaded: %s", filename)
                return dest_path

            elif resp.status_code == 429:
                continue

            else:
                log.warning("    ✗  Download failed (%d) for %s", resp.status_code, filename)
                return None

        except requests.RequestException as exc:
            log.warning("    ⚠  Download attempt %d for %s failed: %s",
                        attempt + 1, filename, exc)
            time.sleep(2 ** attempt)

    return None


def download_attachments(attachments: list[dict], dest_dir: Path) -> list[Path]:
    """
    Download all attachments for one issue.  Returns list of local Paths.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    downloaded = []
    for att in attachments:
        path = download_attachment(att["filename"], att["url"], dest_dir)
        if path:
            downloaded.append(path)
    return downloaded


def upload_attachments(issue_key: str, local_files: list[Path]) -> None:
    """
    Upload local files as attachments to an issue in the target Jira.

    Uses multipart/form-data as required by the Jira Attachments API.
    Note: Content-Type must NOT be set on the session for this call;
    requests will set the correct multipart boundary automatically.
    """
    if not local_files:
        return

    session = get_target_session()
    url = f"{CONFIG['TARGET_URL']}/rest/api/3/issue/{issue_key}/attachments"

    for filepath in local_files:
        for attempt in range(CONFIG["MAX_RETRIES"]):
            try:
                with open(filepath, "rb") as fh:
                    # The 'X-Atlassian-Token: no-check' header is mandatory
                    # for attachment uploads to bypass XSRF protection.
                    resp = session.post(
                        url,
                        files={"file": (filepath.name, fh, "application/octet-stream")},
                        headers={"X-Atlassian-Token": "no-check"},
                        timeout=120,
                    )
                _handle_rate_limit(resp, attempt)

                if resp.status_code in (200, 201):
                    log.debug("    ↑  Uploaded: %s → %s", filepath.name, issue_key)
                    break
                elif resp.status_code == 429:
                    continue
                else:
                    log.warning("    ✗  Upload failed (%d) for %s on %s: %s",
                                resp.status_code, filepath.name, issue_key, resp.text[:200])
                    break

            except requests.RequestException as exc:
                log.warning("    ⚠  Upload attempt %d for %s failed: %s",
                            attempt + 1, filepath.name, exc)
                time.sleep(2 ** attempt)

