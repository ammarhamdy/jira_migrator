import pprint
from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR_PATH = BASE_DIR / "data"
# Jira Data
JIRA_API_TOKEN = os.getenv('JIRA_API_TOKEN')
JIRA_EMAIL = os.getenv('JIRA_EMAIL')
JIRA_BASE_URL = os.getenv('JIRA_BASE_URL')



CONFIG = {
    # ── Source Jira (where the CSV was exported from) ──────────
    "SOURCE_URL": JIRA_BASE_URL,
    "SOURCE_EMAIL": JIRA_EMAIL,
    "SOURCE_API_TOKEN": JIRA_API_TOKEN,

    # ── Target Jira (where issues will be created) ─────────────
    "TARGET_URL": JIRA_BASE_URL,
    "TARGET_EMAIL": JIRA_EMAIL,
    "TARGET_API_TOKEN": JIRA_API_TOKEN,

    # ── Project keys ───────────────────────────────────────────
    "SOURCE_PROJECT_KEY": 'OSS',
    "TARGET_PROJECT_KEY": 'SS',

    # ── CSV file path ──────────────────────────────────────────
    "CSV_FILE": f'{DATA_DIR_PATH}/smart-shopper-all-issues.csv',

    # ── Temporary folder for downloaded attachments ────────────
    "ATTACHMENT_DIR": f'{DATA_DIR_PATH}/attachments',

    # ── Behaviour ──────────────────────────────────────────────
    # Maximum retries on transient HTTP errors / rate limits
    "MAX_RETRIES": 5,
    # Seconds to wait after a 429 (rate-limit) response
    "RATE_LIMIT_WAIT": 30,
    # Delay between issue creations (seconds) to avoid bursting
    "INTER_REQUEST_DELAY": 0.5,
    # Set to True to skip attachment download/upload (dry-run style)
    "SKIP_ATTACHMENTS": False,
}

if __name__ == "__main__":
    pprint.pprint(CONFIG)
