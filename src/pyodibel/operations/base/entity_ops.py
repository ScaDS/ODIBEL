"""
Base entity operations for PyODIBEL.

Operations that work directly with Entity objects and collections.
"""

from typing import Any, Dict, List, Optional

from pyodibel.api.entity import Entity
from pyodibel.api.operations import EntityOperation, OperationConfig


class FilterOperation(EntityOperation):
    """
    Filter entities based on criteria.
    """
    
    def __init__(self, config: OperationConfig):
        super().__init__(config)
        self.filter_criteria = config.parameters.get("criteria", {})
    
    def execute(self, *inputs: Any) -> List[Entity]:
        """Filter entities based on criteria."""
        if not inputs:
            return []
        
        entities = inputs[0] if isinstance(inputs[0], list) else list(inputs)
        filtered = []
        
        for entity in entities:
            if self._matches_criteria(entity):
                filtered.append(entity)
        
        return filtered
    
    def _matches_criteria(self, entity: Entity) -> bool:
        """Check if entity matches filter criteria."""
        for key, value in self.filter_criteria.items():
            if not entity.has_property(key):
                return False
            if entity.get_property(key) != value:
                return False
        return True
    
    def validate_inputs(self, *inputs: Any) -> bool:
        """Validate inputs are entities."""
        if not inputs:
            return False
        if not isinstance(inputs[0], list):
            return False
        if len(inputs[0]) == 0:
            return False
        return all(isinstance(e, Entity) for e in inputs[0])


class ProjectOperation(EntityOperation):
    """
    Project specific properties from entities.
    """
    
    def __init__(self, config: OperationConfig):
        super().__init__(config)
        self.properties = config.parameters.get("properties", [])
    
    def execute(self, *inputs: Any) -> List[Dict[str, Any]]:
        """Project properties from entities."""
        if not inputs:
            return []
        
        entities = inputs[0] if isinstance(inputs[0], list) else list(inputs)
        projected = []
        
        for entity in entities:
            result = {}
            for prop in self.properties:
                if entity.has_property(prop):
                    result[prop] = entity.get_property(prop)
            projected.append(result)
        
        return projected
    
    def validate_inputs(self, *inputs: Any) -> bool:
        """Validate inputs are entities."""
        if not inputs:
            return False
        return all(isinstance(e, Entity) for e in inputs[0] if isinstance(inputs[0], list))


class JoinOperation(EntityOperation):
    """
    Join entities based on matching properties.
    """
    
    def __init__(self, config: OperationConfig):
        super().__init__(config)
        self.join_key = config.parameters.get("join_key", "identifier")
        self.join_type = config.parameters.get("join_type", "inner")
    
    def execute(self, *inputs: Any) -> List[Dict[str, Entity]]:
        """Join entities from multiple collections."""
        if len(inputs) < 2:
            return []
        
        left_entities = inputs[0] if isinstance(inputs[0], list) else list(inputs[0])
        right_entities = inputs[1] if isinstance(inputs[1], list) else list(inputs[1])
        
        # Build index for right entities
        right_index: Dict[str, List[Entity]] = {}
        for entity in right_entities:
            key = self._get_join_key(entity)
            if key not in right_index:
                right_index[key] = []
            right_index[key].append(entity)
        
        # Perform join
        results = []
        for left_entity in left_entities:
            key = self._get_join_key(left_entity)
            if key in right_index:
                for right_entity in right_index[key]:
                    results.append({"left": left_entity, "right": right_entity})
            elif self.join_type == "left":
                results.append({"left": left_entity, "right": None})
        
        return results
    
    def _get_join_key(self, entity: Entity) -> Any:
        """Get the join key value from an entity."""
        if self.join_key == "identifier":
            return entity.identifier
        return entity.get_property(self.join_key)
    
    def validate_inputs(self, *inputs: Any) -> bool:
        """Validate inputs are entity collections."""
        if len(inputs) < 2:
            return False
        return all(
            isinstance(e, Entity) for collection in inputs[:2]
            for e in (collection if isinstance(collection, list) else [collection])
        )

