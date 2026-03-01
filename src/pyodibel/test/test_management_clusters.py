"""
Tests for entity cluster management.
"""

import pytest

from pyodibel.api.entity import SimpleEntity
from pyodibel.management.clusters import (
    EntityCluster,
    ClusterManager,
    InMemoryClusterManager
)


class TestEntityCluster:
    """Tests for EntityCluster."""
    
    def test_create_cluster(self):
        """Test creating a cluster."""
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        cluster = EntityCluster("cluster_1", [entity1, entity2])
        
        assert cluster.cluster_id == "cluster_1"
        assert len(cluster.entities) == 2
        assert cluster.representative is None
    
    def test_cluster_size(self):
        """Test getting cluster size."""
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        entity3 = SimpleEntity("e3")
        cluster = EntityCluster("cluster_1", [entity1, entity2, entity3])
        
        assert cluster.size() == 3
    
    def test_get_entity_ids(self):
        """Test getting entity IDs from cluster."""
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        cluster = EntityCluster("cluster_1", [entity1, entity2])
        
        ids = cluster.get_entity_ids()
        assert "e1" in ids
        assert "e2" in ids
        assert len(ids) == 2
    
    def test_contains(self):
        """Test checking if cluster contains an entity."""
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        entity3 = SimpleEntity("e3")
        cluster = EntityCluster("cluster_1", [entity1, entity2])
        
        assert cluster.contains(entity1) is True
        assert cluster.contains(entity2) is True
        assert cluster.contains(entity3) is False


class TestInMemoryClusterManager:
    """Tests for InMemoryClusterManager."""
    
    def test_create_cluster(self):
        """Test creating a cluster."""
        manager = InMemoryClusterManager()
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        
        cluster = manager.create_cluster("cluster_1", [entity1, entity2])
        
        assert cluster.cluster_id == "cluster_1"
        assert len(cluster.entities) == 2
        assert manager.get_cluster("cluster_1") == cluster
    
    def test_create_duplicate_cluster(self):
        """Test that creating duplicate cluster raises error."""
        manager = InMemoryClusterManager()
        entity1 = SimpleEntity("e1")
        
        manager.create_cluster("cluster_1", [entity1])
        
        with pytest.raises(ValueError, match="already exists"):
            manager.create_cluster("cluster_1", [entity1])
    
    def test_get_cluster(self):
        """Test getting a cluster by ID."""
        manager = InMemoryClusterManager()
        entity1 = SimpleEntity("e1")
        
        created = manager.create_cluster("cluster_1", [entity1])
        retrieved = manager.get_cluster("cluster_1")
        
        assert retrieved == created
        assert manager.get_cluster("nonexistent") is None
    
    def test_find_cluster(self):
        """Test finding cluster containing an entity."""
        manager = InMemoryClusterManager()
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        entity3 = SimpleEntity("e3")
        
        cluster1 = manager.create_cluster("cluster_1", [entity1, entity2])
        cluster2 = manager.create_cluster("cluster_2", [entity3])
        
        found = manager.find_cluster(entity1)
        assert found == cluster1
        
        found = manager.find_cluster(entity3)
        assert found == cluster2
        
        entity4 = SimpleEntity("e4")
        assert manager.find_cluster(entity4) is None
    
    def test_add_entity(self):
        """Test adding an entity to a cluster."""
        manager = InMemoryClusterManager()
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        
        cluster = manager.create_cluster("cluster_1", [entity1])
        assert len(cluster.entities) == 1
        
        manager.add_entity("cluster_1", entity2)
        assert len(cluster.entities) == 2
        assert cluster.contains(entity2)
    
    def test_add_entity_to_nonexistent_cluster(self):
        """Test that adding entity to nonexistent cluster raises error."""
        manager = InMemoryClusterManager()
        entity1 = SimpleEntity("e1")
        
        with pytest.raises(ValueError, match="does not exist"):
            manager.add_entity("nonexistent", entity1)
    
    def test_remove_entity(self):
        """Test removing an entity from a cluster."""
        manager = InMemoryClusterManager()
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        
        cluster = manager.create_cluster("cluster_1", [entity1, entity2])
        assert len(cluster.entities) == 2
        
        manager.remove_entity("cluster_1", entity2)
        assert len(cluster.entities) == 1
        assert not cluster.contains(entity2)
        assert cluster.contains(entity1)
    
    def test_merge_clusters(self):
        """Test merging two clusters."""
        manager = InMemoryClusterManager()
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        entity3 = SimpleEntity("e3")
        
        cluster1 = manager.create_cluster("cluster_1", [entity1, entity2])
        cluster2 = manager.create_cluster("cluster_2", [entity3])
        
        merged = manager.merge_clusters("cluster_1", "cluster_2")
        
        assert merged.cluster_id == "cluster_1"
        assert len(merged.entities) == 3
        assert manager.get_cluster("cluster_2") is None
        assert manager.get_cluster("cluster_1") == merged
    
    def test_merge_clusters_with_overlap(self):
        """Test merging clusters with overlapping entities."""
        manager = InMemoryClusterManager()
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        entity3 = SimpleEntity("e3")
        
        cluster1 = manager.create_cluster("cluster_1", [entity1, entity2])
        cluster2 = manager.create_cluster("cluster_2", [entity2, entity3])  # entity2 overlaps
        
        merged = manager.merge_clusters("cluster_1", "cluster_2")
        
        # Should have 3 unique entities
        assert len(merged.entities) == 3
        assert all(e in merged.entities for e in [entity1, entity2, entity3])
    
    def test_get_all_clusters(self):
        """Test getting all clusters."""
        manager = InMemoryClusterManager()
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        
        cluster1 = manager.create_cluster("cluster_1", [entity1])
        cluster2 = manager.create_cluster("cluster_2", [entity2])
        
        all_clusters = manager.get_all_clusters()
        assert len(all_clusters) == 2
        assert cluster1 in all_clusters
        assert cluster2 in all_clusters
    
    def test_delete_cluster(self):
        """Test deleting a cluster."""
        manager = InMemoryClusterManager()
        entity1 = SimpleEntity("e1")
        
        cluster = manager.create_cluster("cluster_1", [entity1])
        assert manager.get_cluster("cluster_1") is not None
        
        manager.delete_cluster("cluster_1")
        assert manager.get_cluster("cluster_1") is None
        assert manager.find_cluster(entity1) is None
    
    def test_delete_nonexistent_cluster(self):
        """Test that deleting nonexistent cluster raises error."""
        manager = InMemoryClusterManager()
        
        with pytest.raises(ValueError, match="does not exist"):
            manager.delete_cluster("nonexistent")

