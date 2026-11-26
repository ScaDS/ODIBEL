"""Import smoke tests: the base package and entry points work without kgcore."""
import importlib
import subprocess
import sys

import pytest

SUPPORTED_MODULES = [
    "pyodibel",
    "pyodibel.api",
    "pyodibel.cli.main",
    "pyodibel.cache",
    "pyodibel.management.systemkg",
    "pyodibel.operations.rdf.replacer",
    "pyodibel.operations.rdf.nt_io",
    "pyodibel.operations.rdf.reachability",
    "pyodibel.operations.sample",
    "pyodibel.operations.base",
    "pyodibel.benchmark.entity_resolution.data",
    "pyodibel.benchmark.entity_resolution.eval",
]


@pytest.mark.parametrize("name", SUPPORTED_MODULES)
def test_supported_module_imports_without_kgcore(name, monkeypatch):
    monkeypatch.setitem(sys.modules, "kgcore", None)
    monkeypatch.setitem(sys.modules, "kgcore.api", None)
    sys.modules.pop("pyodibel.management.systemkg", None)
    importlib.import_module(name)


def test_systemkg_requires_kgcore_with_clear_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "kgcore", None)
    monkeypatch.setitem(sys.modules, "kgcore.api", None)
    sys.modules.pop("pyodibel.management.systemkg", None)
    systemkg = importlib.import_module("pyodibel.management.systemkg")
    assert systemkg.KnowledgeGraph is None
    with pytest.raises(ImportError, match="kgcore"):
        systemkg.require_kgcore()


def test_all_exports_exist_and_are_not_none():
    import pyodibel

    for name in pyodibel.__all__:
        assert getattr(pyodibel, name, None) is not None, name
    for absent in (
        "WikidataDumpSource",
        "LinkedDataSource",
        "StorageBackend",
        "StorageConfig",
        "PartitionStrategy",
        "PartitionConfig",
    ):
        assert absent not in pyodibel.__all__
        assert not hasattr(pyodibel, absent)


def test_import_does_not_create_log_file(tmp_path):
    subprocess.run(
        [sys.executable, "-c", "import pyodibel, pyodibel.cli.main"],
        cwd=tmp_path,
        check=True,
    )
    assert list(tmp_path.iterdir()) == []
