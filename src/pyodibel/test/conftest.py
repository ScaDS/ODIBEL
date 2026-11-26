import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def pytest_collection_modifyitems(config, items):
    unmarked = [
        item.nodeid
        for item in items
        if "spark" in item.fixturenames and item.get_closest_marker("spark") is None
    ]
    if unmarked:
        raise pytest.UsageError(
            "Tests using the spark fixture must be marked @pytest.mark.spark: "
            + ", ".join(unmarked)
        )
