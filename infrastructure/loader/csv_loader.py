"""Handles loading CSV data from files."""

import csv
import logging
import pprint
from pathlib import Path
from typing import Any
from config.settings import DATA_DIR_PATH


log = logging.getLogger(__name__)


def read_csv(filepath: str) -> list[dict]:
    """
    Read a Jira CSV export and return a list of row dicts.

    Handles:
      • UTF-8 BOM (common in Jira exports)
      • Multiple 'Attachment' columns (renamed to Attachment_1, Attachment_2 …)
      • Stripping whitespace from field names
    """
    log.info("Reading CSV: %s", filepath)
    rows = []

    with open(filepath, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)

        # Jira exports duplicate column headers for multi-value fields
        # (e.g. two "Attachment" columns).  csv.DictReader renames them
        # automatically to "Attachment", "Attachment.1", etc.
        # We normalise those to Attachment_1, Attachment_2 here.
        fieldnames = reader.fieldnames or []
        seen: dict[str, int] = {}
        clean_fieldnames = []
        for name in fieldnames:
            name = name.strip()
            if name in seen:
                seen[name] += 1
                clean_fieldnames.append(f"{name}_{seen[name]}")
            else:
                seen[name] = 0
                clean_fieldnames.append(name)
        reader.fieldnames = clean_fieldnames

        for row in reader:
            # Strip whitespace from every value
            clean_row = {k.strip(): (v.strip() if isinstance(v, str) else v)
                         for k, v in row.items()}
            rows.append(clean_row)

    log.info("Loaded %d issues from CSV.", len(rows))
    return rows


class CSVLoader:

    """
    Loads raw CSV data from files.

    Responsibility: File I/O operations only.
    Does NOT validate or parse domain-specific structure.
    """

    @staticmethod
    def load_from_file(file_path: str) -> list[dict[str, Any]]:
        """
        Load CSV data from a file.

        Args:
            file_path: Path to CSV file

        Returns:
            List of rows as dictionaries (keyed by header names)

        Raises:
            FileNotFoundError: If the file doesn't exist
            csv.Error: If the file contains invalid CSV
        """
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(path, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            return [row for row in reader]

    @staticmethod
    def save_to_file(data: list[dict[str, Any]], file_path: str) -> None:
        """
        Save data to a CSV file.

        Args:
            data: List of dictionaries to serialize
            file_path: Output file path
        """
        if not data:
            return

        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = list(data[0].keys())

        with open(path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(data)


if __name__ == "__main__":
    for r in read_csv(f'{DATA_DIR_PATH}/smart-shopper-all-issues.csv'):
        pprint.pprint(r)