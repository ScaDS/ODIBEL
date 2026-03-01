"""
Entity cluster management for PyODIBEL.

Manages clusters of entities, enabling operations on groups of related
entities such as entity resolution, fusion, and deduplication.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field

from pyodibel.api.entity import Entity


@dataclass
class EntityCluster:
    """Represents a cluster of entities."""
    cluster_id: str
    entities: List[Entity]
    representative: Optional[Entity] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def size(self) -> int:
        """Get the size of the cluster."""
        return len(self.entities)
    
    def get_entity_ids(self) -> Set[str]:
        """Get all entity identifiers in the cluster."""
        return {e.identifier for e in self.entities}
    
    def contains(self, entity: Entity) -> bool:
        """Check if cluster contains an entity."""
        return entity.identifier in self.get_entity_ids()


class ClusterManager(ABC):
    """
    Interface for managing entity clusters.
    
    Provides operations for creating, updating, querying, and merging
    clusters of entities.
    """
    
    @abstractmethod
    def create_cluster(self, cluster_id: str, entities: List[Entity]) -> EntityCluster:
        """
        Create a new cluster.
        
        Args:
            cluster_id: Unique identifier for the cluster
            entities: List of entities to include in the cluster
            
        Returns:
            Created EntityCluster
        """
        pass
    
    @abstractmethod
    def get_cluster(self, cluster_id: str) -> Optional[EntityCluster]:
        """
        Get a cluster by ID.
        
        Args:
            cluster_id: ID of the cluster to retrieve
            
        Returns:
            EntityCluster or None if not found
        """
        pass
    
    @abstractmethod
    def find_cluster(self, entity: Entity) -> Optional[EntityCluster]:
        """
        Find the cluster containing an entity.
        
        Args:
            entity: Entity to search for
            
        Returns:
            EntityCluster containing the entity, or None
        """
        pass
    
    @abstractmethod
    def add_entity(self, cluster_id: str, entity: Entity) -> None:
        """
        Add an entity to a cluster.
        
        Args:
            cluster_id: ID of the cluster
            entity: Entity to add
        """
        pass
    
    @abstractmethod
    def remove_entity(self, cluster_id: str, entity: Entity) -> None:
        """
        Remove an entity from a cluster.
        
        Args:
            cluster_id: ID of the cluster
            entity: Entity to remove
        """
        pass
    
    @abstractmethod
    def merge_clusters(self, cluster_id1: str, cluster_id2: str) -> EntityCluster:
        """
        Merge two clusters.
        
        Args:
            cluster_id1: ID of the first cluster
            cluster_id2: ID of the second cluster
            
        Returns:
            Merged EntityCluster
        """
        pass
    
    @abstractmethod
    def get_all_clusters(self) -> List[EntityCluster]:
        """
        Get all clusters.
        
        Returns:
            List of all EntityCluster objects
        """
        pass
    
    @abstractmethod
    def delete_cluster(self, cluster_id: str) -> None:
        """
        Delete a cluster.
        
        Args:
            cluster_id: ID of the cluster to delete
        """
        pass


class InMemoryClusterManager(ClusterManager):
    """
    In-memory implementation of ClusterManager.
    
    Simple implementation for development and testing.
    """
    
    def __init__(self):
        """Initialize an empty cluster manager."""
        self.clusters: Dict[str, EntityCluster] = {}
        self.entity_to_cluster: Dict[str, str] = {}
    
    def create_cluster(self, cluster_id: str, entities: List[Entity]) -> EntityCluster:
        """Create a new cluster."""
        if cluster_id in self.clusters:
            raise ValueError(f"Cluster {cluster_id} already exists")
        
        cluster = EntityCluster(cluster_id=cluster_id, entities=entities)
        self.clusters[cluster_id] = cluster
        
        # Update entity-to-cluster mapping
        for entity in entities:
            self.entity_to_cluster[entity.identifier] = cluster_id
        
        return cluster
    
    def get_cluster(self, cluster_id: str) -> Optional[EntityCluster]:
        """Get a cluster by ID."""
        return self.clusters.get(cluster_id)
    
    def find_cluster(self, entity: Entity) -> Optional[EntityCluster]:
        """Find the cluster containing an entity."""
        cluster_id = self.entity_to_cluster.get(entity.identifier)
        if cluster_id:
            return self.clusters.get(cluster_id)
        return None
    
    def add_entity(self, cluster_id: str, entity: Entity) -> None:
        """Add an entity to a cluster."""
        if cluster_id not in self.clusters:
            raise ValueError(f"Cluster {cluster_id} does not exist")
        
        cluster = self.clusters[cluster_id]
        if not cluster.contains(entity):
            cluster.entities.append(entity)
            self.entity_to_cluster[entity.identifier] = cluster_id
    
    def remove_entity(self, cluster_id: str, entity: Entity) -> None:
        """Remove an entity from a cluster."""
        if cluster_id not in self.clusters:
            raise ValueError(f"Cluster {cluster_id} does not exist")
        
        cluster = self.clusters[cluster_id]
        cluster.entities = [e for e in cluster.entities if e.identifier != entity.identifier]
        if entity.identifier in self.entity_to_cluster:
            del self.entity_to_cluster[entity.identifier]
    
    def merge_clusters(self, cluster_id1: str, cluster_id2: str) -> EntityCluster:
        """Merge two clusters."""
        cluster1 = self.clusters.get(cluster_id1)
        cluster2 = self.clusters.get(cluster_id2)
        
        if not cluster1 or not cluster2:
            raise ValueError("Both clusters must exist")
        
        # Merge entities
        merged_entities = cluster1.entities.copy()
        for entity in cluster2.entities:
            if not cluster1.contains(entity):
                merged_entities.append(entity)
        
        # Create merged cluster
        merged_cluster = EntityCluster(
            cluster_id=cluster_id1,
            entities=merged_entities,
            metadata={**cluster1.metadata, **cluster2.metadata}
        )
        
        # Update mappings
        self.clusters[cluster_id1] = merged_cluster
        del self.clusters[cluster_id2]
        
        for entity in merged_entities:
            self.entity_to_cluster[entity.identifier] = cluster_id1
        
        return merged_cluster
    
    def get_all_clusters(self) -> List[EntityCluster]:
        """Get all clusters."""
        return list(self.clusters.values())
    
    def delete_cluster(self, cluster_id: str) -> None:
        """Delete a cluster."""
        if cluster_id not in self.clusters:
            raise ValueError(f"Cluster {cluster_id} does not exist")
        
        cluster = self.clusters[cluster_id]
        for entity in cluster.entities:
            if entity.identifier in self.entity_to_cluster:
                del self.entity_to_cluster[entity.identifier]
        
        del self.clusters[cluster_id]

