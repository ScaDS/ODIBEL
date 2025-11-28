"""
Abstract interfaces for entity clustering.

Entity clustering groups records that refer to the same real-world entity
in different representations from different sources.
"""

from abc import ABC, abstractmethod
from typing import Any, Iterator, Optional, Dict, List, Set
from pydantic import BaseModel


class ClusteringConfig(BaseModel):
    """Configuration for entity clustering."""
    
    name: str
    description: Optional[str] = None
    parameters: Dict[str, Any] = {}


class EntityClustering(ABC):
    """
    Abstract base class for entity clustering strategies.
    
    Entity clustering groups records that refer to the same real-world entity,
    which is essential for data integration tasks like entity matching.
    """
    
    def __init__(self, config: ClusteringConfig):
        """
        Initialize the clustering strategy.
        
        Args:
            config: Configuration for clustering
        """
        self.config = config
    
    @abstractmethod
    def cluster(self, records: List[Any], **kwargs) -> Dict[str, List[Any]]:
        """
        Cluster records into groups representing the same entity.
        
        Args:
            records: List of records to cluster
            **kwargs: Additional clustering parameters
            
        Returns:
            Dictionary mapping cluster IDs to lists of records
        """
        pass
    
    @abstractmethod
    def get_cluster_id(self, record: Any) -> Optional[str]:
        """
        Get the cluster ID for a record.
        
        Args:
            record: Record to get cluster ID for
            
        Returns:
            Cluster ID if record is clustered, None otherwise
        """
        pass
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Get metadata about the clustering strategy.
        
        Returns:
            Dictionary containing metadata
        """
        return {
            "name": self.config.name,
            "description": self.config.description,
            "parameters": self.config.parameters
        }

