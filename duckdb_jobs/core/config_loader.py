"""
Configuration loader for DuckDB jobs.
Re-uses the existing config.py / default_config.py from jobs/.
"""

import sys
from pathlib import Path

# Ensure jobs package is importable
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from jobs.default_config import create_config
from jobs.config import get_environment_config

# Default paths
DB_PATH = str(_PROJECT_ROOT / "output" / "igot.duckdb")
WAREHOUSE_DIR = str(_PROJECT_ROOT / "warehouse")
REPORT_DIR = str(_PROJECT_ROOT / "reports")


def load_config():
    """Return the project SimpleConfig object."""
    return create_config(get_environment_config())
