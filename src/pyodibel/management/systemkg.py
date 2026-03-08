"""
System Knowledge Graph for PyODIBEL.

Represents odibel actions and implementations as a knowledge graph,
enabling tracking and querying of system operations, data flows, and
component relationships.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from kgcore.api import KnowledgeGraph



# TODO uses an external package, will only be configured here