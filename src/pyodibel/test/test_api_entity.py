"""
Tests for the Entity API.
"""

import pytest

from pyodibel.api.entity import Entity, EntityMetadata, SimpleEntity


class TestEntityMetadata:
    """Tests for EntityMetadata dataclass."""
    
    def test_create_metadata(self):
        """Test creating metadata with default values."""
        metadata = EntityMetadata()
        assert metadata.source is None
        assert metadata.source_id is None
        assert metadata.confidence is None
        assert metadata.properties == {}
    
    def test_create_metadata_with_values(self):
        """Test creating metadata with specific values."""
        metadata = EntityMetadata(
            source="test_source",
            source_id="123",
            confidence=0.95,
            properties={"key": "value"}
        )
        assert metadata.source == "test_source"
        assert metadata.source_id == "123"
        assert metadata.confidence == 0.95
        assert metadata.properties == {"key": "value"}


class TestSimpleEntity:
    """Tests for SimpleEntity implementation."""
    
    def test_create_entity(self):
        """Test creating a simple entity."""
        entity = SimpleEntity("entity_1", {"name": "Test Entity"})
        assert entity.identifier == "entity_1"
        assert entity.get_property("name") == "Test Entity"
    
    def test_get_properties(self):
        """Test getting all properties."""
        entity = SimpleEntity("entity_1", {"name": "Test", "age": 30})
        properties = entity.get_properties()
        assert properties == {"name": "Test", "age": 30}
        # Ensure it returns a copy
        properties["new"] = "value"
        assert "new" not in entity.get_properties()
    
    def test_get_property(self):
        """Test getting a specific property."""
        entity = SimpleEntity("entity_1", {"name": "Test"})
        assert entity.get_property("name") == "Test"
        assert entity.get_property("nonexistent") is None
        assert entity.get_property("nonexistent", "default") == "default"
    
    def test_has_property(self):
        """Test checking if entity has a property."""
        entity = SimpleEntity("entity_1", {"name": "Test"})
        assert entity.has_property("name") is True
        assert entity.has_property("nonexistent") is False
    
    def test_set_property(self):
        """Test setting a property."""
        entity = SimpleEntity("entity_1")
        entity.set_property("name", "Test")
        assert entity.get_property("name") == "Test"
    
    def test_remove_property(self):
        """Test removing a property."""
        entity = SimpleEntity("entity_1", {"name": "Test", "age": 30})
        entity.remove_property("age")
        assert entity.has_property("name") is True
        assert entity.has_property("age") is False
    
    def test_entity_equality(self):
        """Test entity equality based on identifier."""
        entity1 = SimpleEntity("entity_1")
        entity2 = SimpleEntity("entity_1")
        entity3 = SimpleEntity("entity_2")
        
        assert entity1 == entity2
        assert entity1 != entity3
        assert entity1 != "not_an_entity"
    
    def test_entity_hash(self):
        """Test entity hashing."""
        entity1 = SimpleEntity("entity_1")
        entity2 = SimpleEntity("entity_1")
        entity3 = SimpleEntity("entity_2")
        
        assert hash(entity1) == hash(entity2)
        assert hash(entity1) != hash(entity3)
    
    def test_entity_repr(self):
        """Test entity string representation."""
        metadata = EntityMetadata(source="test_source")
        entity = SimpleEntity("entity_1", metadata=metadata)
        repr_str = repr(entity)
        assert "entity_1" in repr_str
        assert "test_source" in repr_str
    
    def test_entity_with_metadata(self):
        """Test entity with custom metadata."""
        metadata = EntityMetadata(
            source="test_source",
            source_id="123",
            confidence=0.9
        )
        entity = SimpleEntity("entity_1", metadata=metadata)
        assert entity.get_metadata().source == "test_source"
        assert entity.get_metadata().source_id == "123"
        assert entity.get_metadata().confidence == 0.9
    
    def test_entity_with_kwargs(self):
        """Test entity creation with kwargs."""
        entity = SimpleEntity("entity_1", name="Test", age=30)
        assert entity.get_property("name") == "Test"
        assert entity.get_property("age") == 30
    
    def test_entity_identifier(self):
        """Test getting entity identifier."""
        entity = SimpleEntity("entity_1")
        assert entity.get_identifier() == "entity_1"

