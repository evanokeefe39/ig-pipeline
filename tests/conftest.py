"""Shared test fixtures."""
import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def tmp_data():
    """Temporary data directory that mirrors the real layout."""
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        (root / "bronze" / "datasets").mkdir(parents=True)
        (root / "silver" / "posts").mkdir(parents=True)
        (root / "gold" / "posts").mkdir(parents=True)
        yield root
