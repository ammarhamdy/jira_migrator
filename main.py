import logging
import sys
from pathlib import Path

from client.migrate_worker import migrate
from config.settings import CONFIG

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("migration.log", encoding="utf-8"),
    ],
)


log = logging.getLogger(__name__)


if __name__ == "__main__":
    csv_path = sys.argv[1] if len(sys.argv) > 1 else CONFIG["CSV_FILE"]

    if not Path(csv_path).exists():
        log.error("CSV file not found: %s", csv_path)
        sys.exit(1)

    migrate(csv_path)