"""
Gradoop entity model.

Represents entities from Gradoop/FAMER entity resolution results.
"""

from typing import Dict, Any, Optional, Union
from pydantic import Field, model_validator

from pyodibel.api.entity import DataEntity


class GradoopEntity(DataEntity):
    """
    Entity from Gradoop/FAMER entity resolution results.
    
    Represents a single entity with its properties, cluster information,
    and source metadata.
    """
    
    iri: str = Field(..., description="Entity IRI (identifier)")
    resource: str = Field(..., description="Source resource/dataset name")
    cluster_id: Optional[str] = Field(None, description="Cluster identifier if entity is clustered")
    properties: Dict[str, Any] = Field(default_factory=dict, description="Entity properties as key-value pairs")
    
    # Override base class fields - make them optional and set from iri/resource
    id: Optional[str] = Field(None, description="Entity ID (computed from iri)")
    source: Optional[str] = Field(None, description="Source name (computed from resource)")
    
    @model_validator(mode='before')
    @classmethod
    def _set_base_fields_before(cls, data: Union[Dict, Any]) -> Dict:
        """Set id and source from iri and resource before validation."""
        if isinstance(data, dict):
            # If iri/resource are provided but id/source are not, set them
            if 'iri' in data and 'id' not in data:
                data['id'] = data['iri']
            if 'resource' in data and 'source' not in data:
                data['source'] = data['resource']
        return data
    
    @model_validator(mode='after')
    def _set_base_fields_after(self):
        """Ensure id and source are set from iri and resource."""
        if not self.id:
            object.__setattr__(self, 'id', self.iri)
        if not self.source:
            object.__setattr__(self, 'source', self.resource)
        return self
    
    def get_property(self, key: str, default: Any = None) -> Any:
        """
        Get a property value by key.
        
        Args:
            key: Property key
            default: Default value if key not found
            
        Returns:
            Property value or default
        """
        return self.properties.get(key, default)
    
    def has_property(self, key: str) -> bool:
        """
        Check if entity has a specific property.
        
        Args:
            key: Property key
            
        Returns:
            True if property exists
        """
        return key in self.properties
    
    def get_all_properties(self) -> Dict[str, Any]:
        """
        Get all properties as a dictionary.
        
        Returns:
            Dictionary of all properties
        """
        return self.properties.copy()
    
    def __str__(self) -> str:
        """String representation."""
        prop_count = len(self.properties)
        cluster_info = f", cluster={self.cluster_id}" if self.cluster_id else ""
        return f"GradoopEntity(iri={self.iri}, resource={self.resource}{cluster_info}, props={prop_count})"
    
    def __repr__(self) -> str:
        """Developer representation."""
        return self.__str__()

