"""
Backend registry initialization.

Registers default backends and format handlers.
"""

from pyodibel.core.backend import (
    BackendRegistry,
    BackendType,
    get_backend_registry,
)

# Import implementations
try:
    from pyodibel.storage.spark_backend import SparkBackend
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False
    SparkBackend = None

from pyodibel.operations.json_handler import (
    SparkJSONFormatHandler,
    PandasJSONFormatHandler,
    NativeJSONFormatHandler,
)


def register_default_backends():
    """Register default execution backends."""
    registry = get_backend_registry()
    
    if SPARK_AVAILABLE:
        registry.register_backend(BackendType.SPARK, SparkBackend)
    
    # TODO: Register Pandas and Native backends when implemented


def register_default_format_handlers():
    """Register default format handlers."""
    registry = get_backend_registry()
    
    # Register JSON handlers
    registry.register_format_handler("json", SparkJSONFormatHandler)
    registry.register_format_handler("json", PandasJSONFormatHandler)
    registry.register_format_handler("json", NativeJSONFormatHandler)
    
    # Also register jsonl as json
    registry.register_format_handler("jsonl", SparkJSONFormatHandler)


# Auto-register on import
register_default_backends()
register_default_format_handlers()

