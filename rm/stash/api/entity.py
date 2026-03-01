"""
Base entity abstraction for data sources.

This module provides a base DataEntity class that all data source entities
should extend, providing a consistent API for accessing entity data.
"""

from abc import ABC
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class DataEntity(BaseModel, ABC):
    """
    Base class for all data entities returned by data sources.
    
    This provides a common interface for entities from different sources,
    allowing for type-safe access and consistent API across sources.
    
    All entity classes should extend this base class and add source-specific
    fields and methods.
    """
    
    id: str = Field(..., description="Unique identifier for the entity")
    source: str = Field(..., description="Source system or dataset name")
    
    # Note: Subclasses can override these fields to make them optional/computed
    
    class Config:
        """Pydantic configuration."""
        arbitrary_types_allowed = True
        frozen = False  # Allow mutation for compatibility
    
    def get_property(self, key: str, default: Any = None) -> Any:
        """
        Get a property value by key.
        
        Args:
            key: Property key
            default: Default value if key not found
            
        Returns:
            Property value or default
        """
        # Default implementation - subclasses should override
        return getattr(self, key, default)
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert entity to dictionary.
        
        Returns:
            Dictionary representation of the entity
        """
        return self.model_dump(exclude_none=True)
    
    def __str__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}(id={self.id}, source={self.source})"
    
    def __repr__(self) -> str:
        """Developer representation."""
        return self.__str__()

