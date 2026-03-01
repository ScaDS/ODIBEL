"""
Data source interface for PyODIBEL.

The Source interface provides a typed interface to read in and process data
in a specific structure, enabling access to data from various sources
(SPARQL endpoints, REST APIs, web pages, data dumps, etc.).
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterator, Optional, Protocol
from dataclasses import dataclass

from pyodibel.api.entity import Entity


@dataclass
class SourceConfig:
    """Configuration for a data source."""
    name: str
    source_type: str
    location: str
    format: Optional[str] = None
    parameters: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = {}


class Source(ABC):
    """
    Interface to read in and process data in a specific structure.
    
    Provides a typed interface to access data regardless of the underlying
    source format or location.
    """
    
    def __init__(self, config: SourceConfig):
        """
        Initialize a data source.
        
        Args:
            config: Source configuration
        """
        self.config = config
    
    @abstractmethod
    def read_entities(self) -> Iterator[Entity]:
        """
        Read entities from the source.
        
        Yields:
            Entity objects from the source
        """
        pass
    
    @abstractmethod
    def read_raw(self) -> Iterator[Any]:
        """
        Read raw data from the source.
        
        Yields:
            Raw data records from the source
        """
        pass
    
    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """
        Get the schema/structure of the data source.
        
        Returns:
            Dictionary describing the schema
        """
        pass
    
    @abstractmethod
    def validate(self) -> bool:
        """
        Validate that the source is accessible and properly configured.
        
        Returns:
            True if source is valid, False otherwise
        """
        pass
    
    def get_config(self) -> SourceConfig:
        """Get the source configuration."""
        return self.config
    
    def __repr__(self) -> str:
        return f"Source(name={self.config.name}, type={self.config.source_type})"

