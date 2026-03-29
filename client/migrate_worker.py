import json
import sys
import time
from pathlib import Path
from client.attachment_handler import download_attachments, upload_attachments
from client.connection_checker import verify_connection
from client.field_helber import _safe, _collect_all_attachments
from client.issue_builder import create_issue
from client.jira_client import get_source_session, get_target_session
from config.settings import CONFIG
import logging
from infrastructure.loader.csv_loader import read_csv


log = logging.getLogger(__name__)


def migrate(csv_file: str) -> None:
    """
    Full migration flow:
      1. Verify connectivity to both Jira instances.
      2. Read issues from the CSV export.
      3. For each issue:
         a. Create the issue in the target project.
         b. Download its attachments from the source.
         c. Upload those attachments to the newly created issue.
      4. Print a summary report.
      :rtype: None
    """
    log.info("=" * 60)
    log.info("  Jira Migration Script")
    log.info("  Source: %s  [%s]", CONFIG["SOURCE_URL"], CONFIG["SOURCE_PROJECT_KEY"])
    log.info("  Target: %s  [%s]", CONFIG["TARGET_URL"], CONFIG["TARGET_PROJECT_KEY"])
    log.info("=" * 60)

    # ── 1. Verify connections ───────────────────────────────────
    src_ok = verify_connection("SOURCE", CONFIG["SOURCE_URL"], get_source_session())
    tgt_ok = verify_connection("TARGET", CONFIG["TARGET_URL"], get_target_session())
    if not (src_ok and tgt_ok):
        log.error("Aborting – fix connectivity issues above.")
        sys.exit(1)

    # ── 2. Read CSV ─────────────────────────────────────────────
    rows = read_csv(csv_file)[1:]
    if not rows:
        log.warning("No issues found in CSV. Exiting.")
        return

    # ── Prepare stats ───────────────────────────────────────────
    key_map: dict[str, str] = {}  # old_key → new_key
    failures: list[str] = []  # old_keys that failed
    attachment_counts: dict[str, int] = {}  # new_key → # attachments uploaded
    attachment_dir = Path(CONFIG["ATTACHMENT_DIR"])

    total = len(rows)
    log.info("Starting migration of %d issues …\n", total)

    # ── 3. Process each issue ───────────────────────────────────
    for idx, row in enumerate(rows, start=1):
        orig_key = _safe(row.get("Issue key"), f"ROW-{idx}")
        summary_preview = _safe(row.get("Summary"), "(no summary)")[:50]

        log.info("[%d/%d] Processing %s  –  %s", idx, total, orig_key, summary_preview)

        # a. Create issue
        new_key = create_issue(row)
        if not new_key:
            failures.append(orig_key)
            log.warning("  Skipping attachments for %s due to creation failure.", orig_key)
            time.sleep(CONFIG["INTER_REQUEST_DELAY"])
            continue

        key_map[orig_key] = new_key

        # b. & c. Attachments
        if not CONFIG["SKIP_ATTACHMENTS"]:
            attachments = _collect_all_attachments(row)
            if attachments:
                log.info("  Downloading %d attachment(s) for %s …", len(attachments), orig_key)
                issue_dir = attachment_dir / orig_key
                local_files = download_attachments(attachments, issue_dir)

                if local_files:
                    log.info("  Uploading %d file(s) to %s …", len(local_files), new_key)
                    upload_attachments(new_key, local_files)
                    attachment_counts[new_key] = len(local_files)

        time.sleep(CONFIG["INTER_REQUEST_DELAY"])

    # ── 4. Summary report ───────────────────────────────────────
    _print_summary(total, key_map, failures, attachment_counts)


def _print_summary(
        total: int,
        key_map: dict[str, str],
        failures: list[str],
        attachment_counts: dict[str, int],
) -> None:
    """Print a formatted migration summary report."""
    succeeded = len(key_map)
    failed = len(failures)
    total_attachments = sum(attachment_counts.values())

    divider = "─" * 60
    log.info("\n%s", divider)
    log.info("  MIGRATION SUMMARY")
    log.info(divider)
    log.info("  Total issues processed : %d", total)
    log.info("  Successfully created   : %d", succeeded)
    log.info("  Failed                 : %d", failed)
    log.info("  Attachments uploaded   : %d", total_attachments)
    log.info(divider)

    if key_map:
        log.info("\n  Issue key mapping (source → target):")
        for old, new in key_map.items():
            att_note = f"  [{attachment_counts[new]} file(s)]" if new in attachment_counts else ""
            log.info("    %-12s  →  %s%s", old, new, att_note)

    if failures:
        log.info("\n  Failed issues (not migrated):")
        for k in failures:
            log.info("    ✗  %s", k)

    # Also write mapping to a JSON file for reference
    report = {
        "summary": {
            "total": total,
            "succeeded": succeeded,
            "failed": failed,
            "attachments_uploaded": total_attachments,
        },
        "key_mapping": key_map,
        "failures": failures,
    }
    report_path = "migration_report.json"
    with open(report_path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False, indent=2)

    log.info("\n  Full mapping saved to: %s", report_path)
    log.info("  Full log saved to    : migration.log")
    log.info(divider + "\n")
