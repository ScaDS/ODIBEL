"""
Abstract interfaces for data sources.

Data sources represent different ways to access and ingest data into the system,
including SPARQL endpoints, REST APIs, web pages, and data dumps.
"""

from abc import ABC, abstractmethod
from typing import Any, Iterator, Optional, Dict, Protocol
from pathlib import Path
from pydantic import BaseModel


class DataSourceConfig(BaseModel):
    """Base configuration for data sources."""
    
    name: str
    description: Optional[str] = None
    metadata: Dict[str, Any] = {}


class DataSource(ABC):
    """
    Abstract base class for data sources.
    
    A data source provides a way to fetch data from various origins:
    - SPARQL endpoints
    - REST APIs
    - Web pages (HTML or RDF via content negotiation)
    - Data dumps (JSON, XML, RDF, SQL)
    """
    
    def __init__(self, config: DataSourceConfig):
        """
        Initialize the data source.
        
        Args:
            config: Configuration for the data source
        """
        self.config = config
    
    @abstractmethod
    def fetch(self, **kwargs) -> Iterator[Any]:
        """
        Fetch data from the source.
        
        Returns:
            Iterator over data items (triples, records, documents, etc.)
        """
        pass
    
    @abstractmethod
    def supports_format(self, format: str) -> bool:
        """
        Check if the data source supports a specific format.
        
        Args:
            format: Format identifier (e.g., 'rdf', 'json', 'csv', 'xml')
            
        Returns:
            True if the format is supported, False otherwise
        """
        pass
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the data source.
        
        Returns:
            Dictionary containing metadata
        """
        return {
            "name": self.config.name,
            "description": self.config.description,
            **self.config.metadata
        }


class StreamingDataSource(DataSource):
    """
    Abstract base class for data sources that support streaming.
    
    Streaming sources can fetch data incrementally, which is useful
    for large datasets that don't fit in memory.
    """
    
    @abstractmethod
    def fetch_stream(self, chunk_size: int = 1000, **kwargs) -> Iterator[Any]:
        """
        Fetch data in streaming chunks.
        
        Args:
            chunk_size: Number of items per chunk
            **kwargs: Additional source-specific parameters
            
        Returns:
            Iterator over data chunks
        """
        pass


class BatchDataSource(DataSource):
    """
    Abstract base class for data sources that support batch operations.
    
    Batch sources can fetch multiple items at once, which is more
    efficient for sources that support bulk operations.
    """
    
    @abstractmethod
    def fetch_batch(self, batch_size: int = 100, **kwargs) -> Iterator[list[Any]]:
        """
        Fetch data in batches.
        
        Args:
            batch_size: Number of items per batch
            **kwargs: Additional source-specific parameters
            
        Returns:
            Iterator over batches of data items
        """
        pass

