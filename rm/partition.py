"""
Abstract interfaces for partitioning strategies.

Partitioning strategies organize data into logical partitions based on
different criteria: entities, attributes, types, or custom strategies.
"""

from abc import ABC, abstractmethod
from typing import Any, Iterator, Optional, Dict, List, Set
from pydantic import BaseModel


class PartitionConfig(BaseModel):
    """Base configuration for partitioning strategies."""
    
    name: str
    description: Optional[str] = None
    parameters: Dict[str, Any] = {}


class PartitionStrategy(ABC):
    """
    Abstract base class for partitioning strategies.
    
    Partitioning strategies organize data into logical partitions:
    - Entity-centric: Partition by entity identifiers
    - Attribute-centric: Partition by attribute/property
    - Type-centric: Partition by entity types/classes
    """
    
    def __init__(self, config: PartitionConfig):
        """
        Initialize the partitioning strategy.
        
        Args:
            config: Configuration for the partitioning strategy
        """
        self.config = config
    
    @abstractmethod
    def partition(self, data: Any, **kwargs) -> Dict[str, Any]:
        """
        Partition data according to the strategy.
        
        Args:
            data: Input data to partition
            **kwargs: Additional partitioning parameters
            
        Returns:
            Dictionary mapping partition identifiers to partitioned data
        """
        pass
    
    @abstractmethod
    def get_partition_key(self, item: Any, **kwargs) -> str:
        """
        Get the partition key for a data item.
        
        Args:
            item: Data item to get partition key for
            **kwargs: Additional parameters
            
        Returns:
            Partition key/identifier
        """
        pass
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the partitioning strategy.
        
        Returns:
            Dictionary containing metadata
        """
        return {
            "name": self.config.name,
            "description": self.config.description,
            "parameters": self.config.parameters
        }


class EntityPartitionStrategy(PartitionStrategy):
    """
    Abstract base class for entity-centric partitioning.
    
    Entity-centric partitioning groups data by entity identifiers,
    ensuring all data related to the same entity is in the same partition.
    """
    
    @abstractmethod
    def extract_entity_id(self, item: Any) -> str:
        """
        Extract entity identifier from a data item.
        
        Args:
            item: Data item
            
        Returns:
            Entity identifier
        """
        pass
    
    def get_partition_key(self, item: Any, **kwargs) -> str:
        """Get partition key based on entity ID."""
        return self.extract_entity_id(item)


class AttributePartitionStrategy(PartitionStrategy):
    """
    Abstract base class for attribute-centric partitioning.
    
    Attribute-centric partitioning groups data by attribute/property,
    useful for organizing data by specific attributes or properties.
    """
    
    @abstractmethod
    def extract_attribute(self, item: Any) -> str:
        """
        Extract attribute/property identifier from a data item.
        
        Args:
            item: Data item
            
        Returns:
            Attribute/property identifier
        """
        pass
    
    def get_partition_key(self, item: Any, **kwargs) -> str:
        """Get partition key based on attribute."""
        return self.extract_attribute(item)


class TypePartitionStrategy(PartitionStrategy):
    """
    Abstract base class for type-centric partitioning.
    
    Type-centric partitioning groups data by entity types/classes,
    organizing data according to ontological types or categories.
    """
    
    @abstractmethod
    def extract_type(self, item: Any) -> str:
        """
        Extract type/class identifier from a data item.
        
        Args:
            item: Data item
            
        Returns:
            Type/class identifier
        """
        pass
    
    def get_partition_key(self, item: Any, **kwargs) -> str:
        """Get partition key based on type."""
        return self.extract_type(item)

