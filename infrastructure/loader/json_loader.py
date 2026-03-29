"""Handles loading JSON data from files."""

import json
from pathlib import Path
from typing import Any


class JSONLoader:

    """
    Loads raw JSON data from files.

    Responsibility: File I/O operations only.
    Does NOT validate or parse Jira-specific structure.
    """

    @staticmethod
    def load_from_file(file_path: str) -> Any:
        """
        Load JSON data from a file.

        Args:
            file_path: Path to JSON file

        Returns:
            Parsed JSON data (could be dict, list, etc.)

        Raises:
            FileNotFoundError: If the file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
        path = Path(file_path)

        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    @staticmethod
    def save_to_file(data: Any, file_path: str) -> None:
        """
        Save data to a JSON file.

        Args:
            data: Data to serialize
            file_path: Output file path
        """
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)