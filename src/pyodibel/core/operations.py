"""
Abstract interfaces for data operations.

Data operations provide transformations, filtering, joins, and format conversions
on data, supporting operations using Spark, Pandas, or custom utilities.
"""

from abc import ABC, abstractmethod
from typing import Any, Iterator, Optional, Dict, Callable, Protocol
from pydantic import BaseModel

from pyodibel.core.backend import ExecutionBackend, BackendType, BackendConfig, FormatHandler


class OperationConfig(BaseModel):
    """Base configuration for data operations."""
    
    name: str
    description: Optional[str] = None
    parameters: Dict[str, Any] = {}
    backend: Optional[BackendConfig] = None  # Execution backend configuration
    input_format: Optional[str] = None       # Input format (csv, json, rdf, etc.)
    output_format: Optional[str] = None      # Output format (csv, json, rdf, etc.)


class DataOperation(ABC):
    """
    Abstract base class for data operations.
    
    Data operations can be applied to data to:
    - Transform data (mapping, restructuring)
    - Filter data (by characteristics, predicates, etc.)
    - Join/merge/fuse data from multiple sources
    - Convert between formats (JSON ↔ CSV ↔ RDF)
    
    Operations can use different execution backends (Spark, Pandas, Native)
    and work with different data formats (CSV, JSON, RDF, etc.).
    """
    
    def __init__(self, config: OperationConfig, backend: Optional[ExecutionBackend] = None):
        """
        Initialize the data operation.
        
        Args:
            config: Configuration for the operation
            backend: Optional execution backend (if None, will be created from config)
        """
        self.config = config
        self._backend = backend
        self._format_handler: Optional[FormatHandler] = None
    
    def _get_backend(self) -> Optional[ExecutionBackend]:
        """Get or create the execution backend."""
        if self._backend is None and self.config.backend:
            from pyodibel.core.backend import get_backend_registry
            registry = get_backend_registry()
            self._backend = registry.get_backend(
                self.config.backend.backend_type,
                self.config.backend
            )
        return self._backend
    
    def _get_format_handler(self, format_name: str) -> Optional[FormatHandler]:
        """Get format handler for a specific format."""
        backend = self._get_backend()
        if backend is None:
            return None
        
        from pyodibel.core.backend import get_backend_registry
        registry = get_backend_registry()
        try:
            return registry.get_format_handler(format_name, backend.config.backend_type)
        except ValueError:
            return None
    
    @abstractmethod
    def apply(self, data: Any, **kwargs) -> Any:
        """
        Apply the operation to data.
        
        Args:
            data: Input data (format depends on operation type)
            **kwargs: Additional operation-specific parameters
            
        Returns:
            Transformed data
        """
        pass
    
    def apply_with_backend(self, data: Any, backend: Optional[ExecutionBackend] = None, **kwargs) -> Any:
        """
        Apply the operation using a specific backend.
        
        This method handles format conversion and backend selection automatically.
        
        Args:
            data: Input data
            backend: Optional execution backend (uses configured backend if None)
            **kwargs: Additional operation-specific parameters
            
        Returns:
            Transformed data
        """
        # Use provided backend or get from config
        op_backend = backend or self._get_backend()
        
        # If no backend, fall back to native apply
        if op_backend is None:
            return self.apply(data, **kwargs)
        
        # Load data into backend format if needed
        input_format = self.config.input_format or kwargs.get('input_format')
        if input_format:
            handler = self._get_format_handler(input_format)
            if handler:
                data = handler.read(data, op_backend, **kwargs)
        
        # Apply operation (data is now in backend's native format)
        result = self._apply_backend(data, op_backend, **kwargs)
        
        # Convert output format if needed
        output_format = self.config.output_format or kwargs.get('output_format')
        if output_format and output_format != input_format:
            handler = self._get_format_handler(output_format)
            if handler:
                # Note: actual conversion depends on implementation
                # This is a placeholder for the pattern
                pass
        
        return result
    
    def _apply_backend(self, data: Any, backend: ExecutionBackend, **kwargs) -> Any:
        """
        Apply the operation using a specific backend.
        
        This method is called by apply_with_backend after data is loaded
        into the backend's native format. Subclasses should override this
        to provide backend-specific implementations.
        
        Default implementation falls back to apply().
        
        Args:
            data: Data in backend's native format
            backend: Execution backend
            **kwargs: Additional parameters
            
        Returns:
            Result in backend's native format
        """
        # Default: fall back to native apply
        # Subclasses should override for backend-specific optimizations
        return self.apply(data, **kwargs)
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the operation.
        
        Returns:
            Dictionary containing metadata
        """
        return {
            "name": self.config.name,
            "description": self.config.description,
            "parameters": self.config.parameters
        }


class TransformOperation(DataOperation):
    """
    Abstract base class for transformation operations.
    
    Transform operations modify the structure or content of data,
    such as mapping fields, restructuring, or enriching data.
    """
    
    @abstractmethod
    def transform(self, data: Any, mapping: Optional[Dict[str, Any]] = None, **kwargs) -> Any:
        """
        Transform data according to a mapping or transformation rules.
        
        Args:
            data: Input data to transform
            mapping: Optional mapping configuration
            **kwargs: Additional transformation parameters
            
        Returns:
            Transformed data
        """
        pass


class FilterOperation(DataOperation):
    """
    Abstract base class for filtering operations.
    
    Filter operations select or exclude data based on criteria,
    such as entity types, attribute values, or predicates.
    """
    
    @abstractmethod
    def filter(self, data: Any, criteria: Dict[str, Any], **kwargs) -> Any:
        """
        Filter data based on criteria.
        
        Args:
            data: Input data to filter
            criteria: Filter criteria (format depends on implementation)
            **kwargs: Additional filter parameters
            
        Returns:
            Filtered data
        """
        pass


class JoinOperation(DataOperation):
    """
    Abstract base class for join/merge/fusion operations.
    
    Join operations combine data from multiple sources, supporting
    various join strategies (inner, outer, left, right, etc.).
    """
    
    @abstractmethod
    def join(self, *datasets: Any, strategy: str = "inner", on: Optional[str] = None, **kwargs) -> Any:
        """
        Join multiple datasets.
        
        Args:
            *datasets: Variable number of datasets to join
            strategy: Join strategy ('inner', 'outer', 'left', 'right', 'fusion')
            on: Optional key/field to join on
            **kwargs: Additional join parameters
            
        Returns:
            Joined dataset
        """
        pass


class ConvertOperation(DataOperation):
    """
    Abstract base class for format conversion operations.
    
    Convert operations transform data between different formats:
    - JSON ↔ CSV ↔ RDF
    - Other format conversions as needed
    """
    
    @abstractmethod
    def convert(self, data: Any, from_format: str, to_format: str, **kwargs) -> Any:
        """
        Convert data from one format to another.
        
        Args:
            data: Input data
            from_format: Source format identifier
            to_format: Target format identifier
            **kwargs: Additional conversion parameters
            
        Returns:
            Data in the target format
        """
        pass
    
    @abstractmethod
    def supports_conversion(self, from_format: str, to_format: str) -> bool:
        """
        Check if a format conversion is supported.
        
        Args:
            from_format: Source format identifier
            to_format: Target format identifier
            
        Returns:
            True if conversion is supported, False otherwise
        """
        pass

