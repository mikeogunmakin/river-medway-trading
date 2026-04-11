"""
Processed files log — tracks which CSV files have already been ingested.

Persisted as a JSON list in .database/bronze/processed_files.json.
"""

import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_LOG_PATH = _REPO_ROOT / ".database" / "bronze" / "processed_files.json"


def load_processed_log() -> set:
    """Return the set of CSV filenames already loaded into bronze."""
    if not PROCESSED_LOG_PATH.exists():
        return set()
    return set(json.loads(PROCESSED_LOG_PATH.read_text()))


def save_processed_log(processed: set) -> None:
    """Persist the updated set of processed filenames."""
    PROCESSED_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROCESSED_LOG_PATH.write_text(json.dumps(sorted(processed), indent=2))
