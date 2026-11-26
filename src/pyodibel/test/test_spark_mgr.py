"""Spark session master selection.

Each check runs in a subprocess so it can start and stop a JVM without
disturbing a Spark session already living in the pytest process.
"""

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

pytestmark = pytest.mark.spark

ROOT = Path(__file__).resolve().parents[3]


def _run_isolated(code: str) -> None:
    env = os.environ.copy()
    env.pop("MASTER", None)
    env.pop("PYSPARK_SUBMIT_VERSION", None)
    env["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")
    completed = subprocess.run(
        [sys.executable, "-c", textwrap.dedent(code)],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stdout + "\n" + completed.stderr


def test_explicit_master_is_applied():
    _run_isolated(
        """
        from pyodibel.management.spark_mgr import get_spark_session

        spark = get_spark_session("explicit-master", master="local[1]")
        try:
            assert spark.sparkContext.master == "local[1]", spark.sparkContext.master
        finally:
            spark.stop()
        """
    )


def test_existing_context_keeps_its_master():
    _run_isolated(
        """
        from pyspark.sql import SparkSession

        from pyodibel.management.spark_mgr import get_spark_session

        existing = (
            SparkSession.builder.master("local[2]")
            .appName("already-running")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        try:
            spark = get_spark_session("should-inherit", master="local[1]")
            assert spark.sparkContext.master == "local[2]", spark.sparkContext.master
        finally:
            existing.stop()
        """
    )
