"""
Tests for base entity operations.
"""

import pytest

from pyodibel.api.entity import SimpleEntity
from pyodibel.api.operations import OperationConfig
from pyodibel.operations.base.entity_ops import (
    FilterOperation,
    ProjectOperation,
    JoinOperation
)


class TestFilterOperation:
    """Tests for FilterOperation."""
    
    def test_filter_entities(self):
        """Test filtering entities based on criteria."""
        entity1 = SimpleEntity("e1", {"name": "Alice", "age": 30})
        entity2 = SimpleEntity("e2", {"name": "Bob", "age": 25})
        entity3 = SimpleEntity("e3", {"name": "Alice", "age": 35})
        
        config = OperationConfig(
            operation_type="filter",
            parameters={"criteria": {"name": "Alice"}}
        )
        operation = FilterOperation(config)
        
        filtered = operation.execute([entity1, entity2, entity3])
        
        assert len(filtered) == 2
        assert entity1 in filtered
        assert entity3 in filtered
        assert entity2 not in filtered
    
    def test_filter_multiple_criteria(self):
        """Test filtering with multiple criteria."""
        entity1 = SimpleEntity("e1", {"name": "Alice", "age": 30})
        entity2 = SimpleEntity("e2", {"name": "Alice", "age": 25})
        entity3 = SimpleEntity("e3", {"name": "Bob", "age": 30})
        
        config = OperationConfig(
            operation_type="filter",
            parameters={"criteria": {"name": "Alice", "age": 30}}
        )
        operation = FilterOperation(config)
        
        filtered = operation.execute([entity1, entity2, entity3])
        
        assert len(filtered) == 1
        assert entity1 in filtered
    
    def test_filter_empty_result(self):
        """Test filtering with no matches."""
        entity1 = SimpleEntity("e1", {"name": "Alice"})
        
        config = OperationConfig(
            operation_type="filter",
            parameters={"criteria": {"name": "Bob"}}
        )
        operation = FilterOperation(config)
        
        filtered = operation.execute([entity1])
        assert len(filtered) == 0
    
    def test_validate_inputs(self):
        """Test input validation."""
        config = OperationConfig(operation_type="filter", parameters={})
        operation = FilterOperation(config)
        
        entity1 = SimpleEntity("e1")
        assert operation.validate_inputs([entity1]) is True
        assert operation.validate_inputs([]) is False
        assert operation.validate_inputs() is False


class TestProjectOperation:
    """Tests for ProjectOperation."""
    
    def test_project_properties(self):
        """Test projecting specific properties."""
        entity1 = SimpleEntity("e1", {"name": "Alice", "age": 30, "city": "NYC"})
        entity2 = SimpleEntity("e2", {"name": "Bob", "age": 25})
        
        config = OperationConfig(
            operation_type="project",
            parameters={"properties": ["name", "age"]}
        )
        operation = ProjectOperation(config)
        
        projected = operation.execute([entity1, entity2])
        
        assert len(projected) == 2
        assert projected[0] == {"name": "Alice", "age": 30}
        assert projected[1] == {"name": "Bob", "age": 25}
    
    def test_project_missing_properties(self):
        """Test projecting properties that don't exist."""
        entity1 = SimpleEntity("e1", {"name": "Alice"})
        
        config = OperationConfig(
            operation_type="project",
            parameters={"properties": ["name", "age"]}
        )
        operation = ProjectOperation(config)
        
        projected = operation.execute([entity1])
        
        assert len(projected) == 1
        assert projected[0] == {"name": "Alice"}  # age not included
    
    def test_project_empty_properties(self):
        """Test projecting with empty property list."""
        entity1 = SimpleEntity("e1", {"name": "Alice"})
        
        config = OperationConfig(
            operation_type="project",
            parameters={"properties": []}
        )
        operation = ProjectOperation(config)
        
        projected = operation.execute([entity1])
        
        assert len(projected) == 1
        assert projected[0] == {}
    
    def test_validate_inputs(self):
        """Test input validation."""
        config = OperationConfig(operation_type="project", parameters={})
        operation = ProjectOperation(config)
        
        entity1 = SimpleEntity("e1")
        assert operation.validate_inputs([entity1]) is True


class TestJoinOperation:
    """Tests for JoinOperation."""
    
    def test_inner_join(self):
        """Test inner join operation."""
        entity1 = SimpleEntity("e1", {"key": "A"})
        entity2 = SimpleEntity("e2", {"key": "B"})
        entity3 = SimpleEntity("e3", {"key": "A"})
        
        left_entities = [entity1, entity2]
        right_entities = [entity3]
        
        config = OperationConfig(
            operation_type="join",
            parameters={"join_key": "key", "join_type": "inner"}
        )
        operation = JoinOperation(config)
        
        results = operation.execute(left_entities, right_entities)
        
        assert len(results) == 1
        assert results[0]["left"] == entity1
        assert results[0]["right"] == entity3
    
    def test_join_on_identifier(self):
        """Test join on identifier."""
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        entity3 = SimpleEntity("e1")  # Same identifier as entity1
        
        left_entities = [entity1]
        right_entities = [entity2, entity3]
        
        config = OperationConfig(
            operation_type="join",
            parameters={"join_key": "identifier", "join_type": "inner"}
        )
        operation = JoinOperation(config)
        
        results = operation.execute(left_entities, right_entities)
        
        assert len(results) == 1
        assert results[0]["left"] == entity1
        assert results[0]["right"] == entity3
    
    def test_left_join(self):
        """Test left join operation."""
        entity1 = SimpleEntity("e1", {"key": "A"})
        entity2 = SimpleEntity("e2", {"key": "B"})
        entity3 = SimpleEntity("e3", {"key": "A"})
        
        left_entities = [entity1, entity2]
        right_entities = [entity3]
        
        config = OperationConfig(
            operation_type="join",
            parameters={"join_key": "key", "join_type": "left"}
        )
        operation = JoinOperation(config)
        
        results = operation.execute(left_entities, right_entities)
        
        assert len(results) == 2
        # entity1 matches entity3
        assert any(r["left"] == entity1 and r["right"] == entity3 for r in results)
        # entity2 has no match, should have None right
        assert any(r["left"] == entity2 and r["right"] is None for r in results)
    
    def test_join_multiple_matches(self):
        """Test join with multiple matches."""
        entity1 = SimpleEntity("e1", {"key": "A"})
        entity2 = SimpleEntity("e2", {"key": "A"})
        entity3 = SimpleEntity("e3", {"key": "A"})
        
        left_entities = [entity1]
        right_entities = [entity2, entity3]
        
        config = OperationConfig(
            operation_type="join",
            parameters={"join_key": "key", "join_type": "inner"}
        )
        operation = JoinOperation(config)
        
        results = operation.execute(left_entities, right_entities)
        
        assert len(results) == 2  # entity1 matches both entity2 and entity3
    
    def test_validate_inputs(self):
        """Test input validation."""
        config = OperationConfig(operation_type="join", parameters={})
        operation = JoinOperation(config)
        
        entity1 = SimpleEntity("e1")
        entity2 = SimpleEntity("e2")
        
        assert operation.validate_inputs([entity1], [entity2]) is True
        assert operation.validate_inputs([entity1]) is False  # Need at least 2 inputs

