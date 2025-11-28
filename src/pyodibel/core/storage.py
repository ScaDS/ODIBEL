"""
Abstract interfaces for storage backends.

Storage backends provide persistent storage for ingested data, supporting
different storage mechanisms like file systems, databases, and object storage.
"""

from abc import ABC, abstractmethod
from typing import Any, Iterator, Optional, Dict, List
from pathlib import Path
from pydantic import BaseModel


class StorageConfig(BaseModel):
    """Base configuration for storage backends."""
    
    name: str
    base_path: Optional[Path] = None
    metadata: Dict[str, Any] = {}


class StorageBackend(ABC):
    """
    Abstract base class for storage backends.
    
    Storage backends provide persistent storage for data, supporting:
    - File-based storage (local or network file systems)
    - Database storage (SQL databases via SQLAlchemy)
    - Object storage (S3-compatible storage)
    """
    
    def __init__(self, config: StorageConfig):
        """
        Initialize the storage backend.
        
        Args:
            config: Configuration for the storage backend
        """
        self.config = config
    
    @abstractmethod
    def save(self, data: Any, partition: Optional[str] = None, **kwargs) -> str:
        """
        Save data to storage.
        
        Args:
            data: Data to save (format depends on implementation)
            partition: Optional partition identifier
            **kwargs: Additional storage-specific parameters
            
        Returns:
            Identifier or path to the saved data
        """
        pass
    
    @abstractmethod
    def load(self, identifier: str, **kwargs) -> Any:
        """
        Load data from storage.
        
        Args:
            identifier: Identifier or path to the data
            **kwargs: Additional storage-specific parameters
            
        Returns:
            Loaded data
        """
        pass
    
    @abstractmethod
    def exists(self, identifier: str) -> bool:
        """
        Check if data exists in storage.
        
        Args:
            identifier: Identifier or path to check
            
        Returns:
            True if data exists, False otherwise
        """
        pass
    
    @abstractmethod
    def delete(self, identifier: str) -> bool:
        """
        Delete data from storage.
        
        Args:
            identifier: Identifier or path to delete
            
        Returns:
            True if deletion was successful, False otherwise
        """
        pass
    
    @abstractmethod
    def list(self, partition: Optional[str] = None, **kwargs) -> Iterator[str]:
        """
        List available data identifiers.
        
        Args:
            partition: Optional partition to filter by
            **kwargs: Additional storage-specific parameters
            
        Returns:
            Iterator over data identifiers
        """
        pass
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the storage backend.
        
        Returns:
            Dictionary containing metadata
        """
        return {
            "name": self.config.name,
            "base_path": str(self.config.base_path) if self.config.base_path else None,
            **self.config.metadata
        }


class PartitionedStorage(StorageBackend):
    """
    Abstract base class for storage backends that support partitioning.
    
    Partitioned storage organizes data into logical partitions, which can
    be based on entities, attributes, types, or custom strategies.
    """
    
    @abstractmethod
    def create_partition(self, partition_id: str, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Create a new partition.
        
        Args:
            partition_id: Unique identifier for the partition
            metadata: Optional metadata for the partition
            
        Returns:
            True if partition was created, False otherwise
        """
        pass
    
    @abstractmethod
    def list_partitions(self) -> List[str]:
        """
        List all available partitions.
        
        Returns:
            List of partition identifiers
        """
        pass
    
    @abstractmethod
    def get_partition_metadata(self, partition_id: str) -> Dict[str, Any]:
        """
        Get metadata for a partition.
        
        Args:
            partition_id: Partition identifier
            
        Returns:
            Dictionary containing partition metadata
        """
        pass


class TableStorage(StorageBackend):
    """
    Abstract base class for storage backends that support table/record management.
    
    Table storage organizes data into tables with records, similar to
    relational databases or structured data formats.
    """
    
    @abstractmethod
    def create_table(self, table_name: str, schema: Optional[Dict[str, Any]] = None) -> bool:
        """
        Create a new table.
        
        Args:
            table_name: Name of the table
            schema: Optional schema definition
            
        Returns:
            True if table was created, False otherwise
        """
        pass
    
    @abstractmethod
    def insert_records(self, table_name: str, records: List[Dict[str, Any]]) -> int:
        """
        Insert records into a table.
        
        Args:
            table_name: Name of the table
            records: List of records to insert
            
        Returns:
            Number of records inserted
        """
        pass
    
    @abstractmethod
    def query_records(self, table_name: str, filters: Optional[Dict[str, Any]] = None) -> Iterator[Dict[str, Any]]:
        """
        Query records from a table.
        
        Args:
            table_name: Name of the table
            filters: Optional filters to apply
            
        Returns:
            Iterator over matching records
        """
        pass

