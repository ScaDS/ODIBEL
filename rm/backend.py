"""
Execution backend interfaces.

Execution backends provide the computational engine for operations,
supporting different frameworks like Spark, Pandas, or custom utilities.
"""

from abc import ABC, abstractmethod
from typing import Any, Optional, Dict, Protocol
from enum import Enum
from pydantic import BaseModel


class BackendType(str, Enum):
    """Types of execution backends."""
    SPARK = "spark"
    PANDAS = "pandas"
    NATIVE = "native"  # Custom Python utilities
    DASK = "dask"      # Dask for distributed computing


class BackendConfig(BaseModel):
    """Configuration for execution backends."""
    
    backend_type: BackendType
    config: Dict[str, Any] = {}
    auto_init: bool = True


class ExecutionBackend(ABC):
    """
    Abstract base class for execution backends.
    
    Execution backends provide the computational engine for operations.
    Different backends are optimized for different scenarios:
    - Spark: Large-scale distributed processing
    - Pandas: In-memory data manipulation
    - Native: Custom Python utilities for specific formats (e.g., RDF)
    """
    
    def __init__(self, config: BackendConfig):
        """
        Initialize the execution backend.
        
        Args:
            config: Configuration for the backend
        """
        self.config = config
        self._session = None
        if config.auto_init:
            self.initialize()
    
    @abstractmethod
    def initialize(self) -> None:
        """Initialize the backend (e.g., create Spark session)."""
        pass
    
    @abstractmethod
    def shutdown(self) -> None:
        """Shutdown the backend and release resources."""
        pass
    
    @abstractmethod
    def load_data(self, source: Any, format: str, **kwargs) -> Any:
        """
        Load data into the backend's native format.
        
        Args:
            source: Data source (path, URL, or data object)
            format: Format identifier (csv, json, rdf, parquet, etc.)
            **kwargs: Additional format-specific parameters
            
        Returns:
            Data in backend's native format (DataFrame, RDD, etc.)
        """
        pass
    
    @abstractmethod
    def save_data(self, data: Any, target: Any, format: str, **kwargs) -> None:
        """
        Save data from the backend's native format.
        
        Args:
            data: Data in backend's native format
            target: Target location (path, URL, etc.)
            format: Format identifier
            **kwargs: Additional format-specific parameters
        """
        pass
    
    @abstractmethod
    def supports_format(self, format: str) -> bool:
        """
        Check if the backend supports a specific format.
        
        Args:
            format: Format identifier
            
        Returns:
            True if format is supported, False otherwise
        """
        pass
    
    def get_session(self) -> Any:
        """
        Get the backend session (e.g., SparkSession, pandas context).
        
        Returns:
            Backend session object
        """
        return self._session
    
    def __enter__(self):
        """Context manager entry."""
        if self._session is None:
            self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.shutdown()


class FormatHandler(ABC):
    """
    Abstract base class for format handlers.
    
    Format handlers provide format-specific operations for reading,
    writing, and manipulating data in specific formats (CSV, JSON, RDF, etc.).
    """
    
    @abstractmethod
    def read(self, source: Any, backend: ExecutionBackend, **kwargs) -> Any:
        """
        Read data from source using the backend.
        
        Args:
            source: Data source (path, URL, or data object)
            backend: Execution backend to use
            **kwargs: Additional format-specific parameters
            
        Returns:
            Data in backend's native format
        """
        pass
    
    @abstractmethod
    def write(self, data: Any, target: Any, backend: ExecutionBackend, **kwargs) -> None:
        """
        Write data to target using the backend.
        
        Args:
            data: Data in backend's native format
            target: Target location (path, URL, etc.)
            backend: Execution backend to use
            **kwargs: Additional format-specific parameters
        """
        pass
    
    @abstractmethod
    def supports_backend(self, backend_type: BackendType) -> bool:
        """
        Check if the format handler supports a specific backend.
        
        Args:
            backend_type: Type of backend
            
        Returns:
            True if backend is supported, False otherwise
        """
        pass
    
    @property
    @abstractmethod
    def format_name(self) -> str:
        """Get the format name."""
        pass


class BackendRegistry:
    """
    Registry for execution backends and format handlers.
    
    Allows dynamic registration and lookup of backends and format handlers.
    """
    
    def __init__(self):
        self._backends: Dict[BackendType, type[ExecutionBackend]] = {}
        self._format_handlers: Dict[str, list[type[FormatHandler]]] = {}
    
    def register_backend(self, backend_type: BackendType, backend_class: type[ExecutionBackend]):
        """Register an execution backend."""
        self._backends[backend_type] = backend_class
    
    def register_format_handler(self, format_name: str, handler_class: type[FormatHandler]):
        """Register a format handler."""
        if format_name not in self._format_handlers:
            self._format_handlers[format_name] = []
        self._format_handlers[format_name].append(handler_class)
    
    def get_backend(self, backend_type: BackendType, config: BackendConfig) -> ExecutionBackend:
        """Get an instance of a backend."""
        if backend_type not in self._backends:
            raise ValueError(f"Backend type '{backend_type}' not registered")
        return self._backends[backend_type](config)
    
    def get_format_handler(self, format_name: str, backend_type: BackendType) -> FormatHandler:
        """Get a format handler for a format and backend."""
        if format_name not in self._format_handlers:
            raise ValueError(f"Format '{format_name}' not registered")
        
        handlers = self._format_handlers[format_name]
        for handler_class in handlers:
            handler = handler_class()
            if handler.supports_backend(backend_type):
                return handler
        
        raise ValueError(f"No handler for format '{format_name}' with backend '{backend_type}'")


# Global registry instance
_backend_registry = BackendRegistry()


def get_backend_registry() -> BackendRegistry:
    """Get the global backend registry."""
    return _backend_registry

