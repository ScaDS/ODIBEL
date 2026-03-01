"""
Operations interface for PyODIBEL.

The Operations interface defines operations on entities, such as joins,
filters, transformations, etc. Implementations can work with different
data structures (e.g., Spark DataFrames, RDF triples, etc.).
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Protocol
from dataclasses import dataclass

from pyodibel.api.entity import Entity


@dataclass
class OperationConfig:
    """Configuration for a data operation."""
    operation_type: str
    parameters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}


class Operation(ABC):
    """
    Interface to define operations on entities.
    
    Operations can be applied to entities or collections of entities
    to perform transformations, joins, filters, aggregations, etc.
    """
    
    def __init__(self, config: OperationConfig):
        """
        Initialize an operation.
        
        Args:
            config: Operation configuration
        """
        self.config = config
    
    @abstractmethod
    def execute(self, *inputs: Any) -> Any:
        """
        Execute the operation on the given inputs.
        
        Args:
            *inputs: Input data (entities, collections, etc.)
            
        Returns:
            Result of the operation
        """
        pass
    
    @abstractmethod
    def validate_inputs(self, *inputs: Any) -> bool:
        """
        Validate that the inputs are suitable for this operation.
        
        Args:
            *inputs: Input data to validate
            
        Returns:
            True if inputs are valid, False otherwise
        """
        pass
    
    def get_config(self) -> OperationConfig:
        """Get the operation configuration."""
        return self.config
    
    def __repr__(self) -> str:
        return f"Operation(type={self.config.operation_type})"


class EntityOperation(Operation):
    """
    Base class for operations that work directly with Entity objects.
    """
    
    @abstractmethod
    def execute(self, *inputs: Any) -> Any:
        """Execute operation on entity inputs."""
        pass


class BatchOperation(Operation):
    """
    Base class for operations that work on collections/batches of entities.
    """
    
    @abstractmethod
    def execute(self, *inputs: Any) -> Any:
        """Execute operation on batch inputs."""
        pass

