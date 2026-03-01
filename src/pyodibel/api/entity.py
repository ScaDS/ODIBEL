"""
Entity interface for PyODIBEL.

The Entity class is the main class to define and wrap other entities,
providing a unified interface for working with entities across different
data sources and formats.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Set
from dataclasses import dataclass, field


@dataclass
class EntityMetadata:
    """Metadata associated with an entity."""
    source: Optional[str] = None
    source_id: Optional[str] = None
    confidence: Optional[float] = None
    properties: Dict[str, Any] = field(default_factory=dict)


class Entity(ABC):
    """
    Main class to define and wrap other entities.
    
    Entities represent identifiable objects in the data integration context.
    They provide a typed interface to access entity data regardless of
    the underlying data source or format.
    """
    
    def __init__(
        self,
        identifier: str,
        metadata: Optional[EntityMetadata] = None,
        **kwargs
    ):
        """
        Initialize an entity.
        
        Args:
            identifier: Unique identifier for the entity
            metadata: Optional metadata associated with the entity
            **kwargs: Additional entity-specific attributes
        """
        self.identifier = identifier
        self.metadata = metadata or EntityMetadata()
        self._attributes = kwargs
    
    @abstractmethod
    def get_properties(self) -> Dict[str, Any]:
        """
        Get all properties of the entity.
        
        Returns:
            Dictionary mapping property names to values
        """
        pass
    
    @abstractmethod
    def get_property(self, name: str, default: Any = None) -> Any:
        """
        Get a specific property by name.
        
        Args:
            name: Property name
            default: Default value if property doesn't exist
            
        Returns:
            Property value or default
        """
        pass
    
    @abstractmethod
    def has_property(self, name: str) -> bool:
        """
        Check if entity has a specific property.
        
        Args:
            name: Property name
            
        Returns:
            True if property exists, False otherwise
        """
        pass
    
    def get_identifier(self) -> str:
        """Get the entity identifier."""
        return self.identifier
    
    def get_metadata(self) -> EntityMetadata:
        """Get entity metadata."""
        return self.metadata
    
    def __repr__(self) -> str:
        return f"Entity(identifier={self.identifier}, source={self.metadata.source})"
    
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Entity):
            return False
        return self.identifier == other.identifier
    
    def __hash__(self) -> int:
        return hash(self.identifier)


class SimpleEntity(Entity):
    """
    Simple concrete implementation of Entity.
    
    Stores properties in a dictionary for easy use in development and testing.
    """
    
    def __init__(
        self,
        identifier: str,
        properties: Optional[Dict[str, Any]] = None,
        metadata: Optional[EntityMetadata] = None,
        **kwargs
    ):
        """
        Initialize a simple entity.
        
        Args:
            identifier: Unique identifier for the entity
            properties: Dictionary of entity properties
            metadata: Optional metadata associated with the entity
            **kwargs: Additional properties (merged with properties dict)
        """
        super().__init__(identifier, metadata, **kwargs)
        self._properties = properties or {}
        self._properties.update(kwargs)
    
    def get_properties(self) -> Dict[str, Any]:
        """Get all properties of the entity."""
        return self._properties.copy()
    
    def get_property(self, name: str, default: Any = None) -> Any:
        """Get a specific property by name."""
        return self._properties.get(name, default)
    
    def has_property(self, name: str) -> bool:
        """Check if entity has a specific property."""
        return name in self._properties
    
    def set_property(self, name: str, value: Any) -> None:
        """Set a property value."""
        self._properties[name] = value
    
    def remove_property(self, name: str) -> None:
        """Remove a property."""
        if name in self._properties:
            del self._properties[name]

