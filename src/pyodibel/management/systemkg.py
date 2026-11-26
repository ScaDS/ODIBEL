"""
System Knowledge Graph for PyODIBEL (experimental).

Represents odibel actions and implementations as a knowledge graph,
enabling tracking and querying of system operations, data flows, and
component relationships.

This module currently only exposes the optional ``kgcore`` dependency, which is
not installed with ``pyodibel``. Importing the module always works; use
:func:`require_kgcore` to get ``KnowledgeGraph`` or a clear error.
"""

try:
    from kgcore.api import KnowledgeGraph
except ImportError:  # kgcore is not a declared dependency
    KnowledgeGraph = None


def require_kgcore():
    """Return ``kgcore.api.KnowledgeGraph`` or raise an informative ImportError."""
    if KnowledgeGraph is None:
        raise ImportError(
            "pyodibel.management.systemkg requires the external 'kgcore' package, "
            "which is not a pyodibel dependency. Install kgcore to use it."
        )
    return KnowledgeGraph
