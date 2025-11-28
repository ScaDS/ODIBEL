# RDF Operations Migration Plan

## Overview

This document outlines the plan to migrate and integrate the legacy `rdf_ops` code (`/src/pyodibel/ops/rdf_ops`) into the new PyODIBEL architecture.

## Current State Analysis

### Files in `rdf_ops/`:

1. **`filter.py`** - RDF graph filtering operations
2. **`construct.py`** - RDF graph construction and entity mapping
3. **`cluster.py`** - Entity matching and clustering (`MatchCluster`)
4. **`extract.py`** - Subgraph extraction
5. **`filehashstore.py`** - File-based hash storage for RDF (`FileHashStore`, `FileHashStore2`)
6. **`inference.py`** - Type inference/enrichment
7. **`replacer.py`** - Namespace replacement operations
8. **`shade.py`** - URI shading/hashing operations
9. **`utils.py`** - RDF to JSON conversion utilities
10. **`rdf_flow.py`** - Abstract RDF flow classes (minimal)

## Migration Strategy

### 1. **Operations** → `operations/rdf/`

Move RDF-specific operations to `operations/rdf/`:

#### `operations/rdf/filter.py`
- Migrate from `rdf_ops/filter.py`:
  - `filter_subject()` → `RDFSubjectFilter`
  - `filter_predicate()` → `RDFPredicateFilter`
  - `filter_type_statement()` → `RDFTypeFilter`
  - `filter_graph_by_namespaces()` → `RDFNamespaceFilter`
  - `union_graphs()` → `RDFUnionOperation`

**Implementation:**
```python
from pyodibel.core.operations import FilterOperation
from rdflib import Graph

class RDFPredicateFilter(FilterOperation):
    def _apply_backend(self, data: Graph, backend, **kwargs):
        predicate = kwargs.get('predicate')
        filtered = Graph()
        for s, p, o in data:
            if str(p) == predicate:
                filtered.add((s, p, o))
        return filtered
```

#### `operations/rdf/transform.py`
- Migrate from `rdf_ops/construct.py`:
  - `direct_map_triple_fast()` → `RDFDirectMappingTransform`
  - `construct_graph_from_root_uris()` → `RDFGraphConstructionTransform`
- Migrate from `rdf_ops/replacer.py`:
  - `replace_namespace()` → `RDFNamespaceReplacer`
  - `replace_to_namespace()` → `RDFNamespaceMapper`
- Migrate from `rdf_ops/shade.py`:
  - `shade_graph()` → `RDFURIShader`
  - `reshade_hash_uri_graph()` → `RDFURIReshader`

#### `operations/rdf/extract.py`
- Migrate from `rdf_ops/extract.py`:
  - `extract_subgraph_recursive()` → `RDFSubgraphExtractor`

#### `operations/rdf/inference.py`
- Migrate from `rdf_ops/inference.py`:
  - `enrich_type_information()` → `RDFTypeEnricher`

### 2. **Link Management** → `link/`

#### `link/clustering.py`
- Migrate from `rdf_ops/cluster.py`:
  - `MatchCluster` class → `EntityMatchCluster` (implements `EntityClustering`)
  - `load_matches()` → Helper function
  - `is_match()` → Helper function

**Implementation:**
```python
from pyodibel.core.link import EntityClustering, ClusteringConfig
from pyodibel.ops.rdf_ops.cluster import MatchCluster

class EntityMatchCluster(EntityClustering):
    def __init__(self, config: ClusteringConfig):
        super().__init__(config)
        self._match_cluster = MatchCluster()
    
    def cluster(self, records: List[Any], **kwargs) -> Dict[str, List[Any]]:
        # Use MatchCluster internally
        ...
```

### 3. **Storage** → `storage/rdf_file.py`

#### `storage/rdf_file.py`
- Migrate from `rdf_ops/filehashstore.py`:
  - `FileHashStore` → `RDFFileHashStorage` (implements `StorageBackend`)
  - `FileHashStore2` → Enhanced version with better interface

**Implementation:**
```python
from pyodibel.core.storage import StorageBackend, StorageConfig
from rdflib import Graph

class RDFFileHashStorage(StorageBackend):
    def __init__(self, config: StorageConfig):
        super().__init__(config)
        # Use FileHashStore internally
    
    def save(self, data: Graph, partition: Optional[str] = None, **kwargs):
        uri = kwargs.get('uri')  # Required for hash-based storage
        # Use FileHashStore.store()
    
    def load(self, identifier: str, **kwargs) -> Graph:
        # Use FileHashStore.retrieve()
```

### 4. **Format Handlers** → `operations/rdf_handler.py`

#### `operations/rdf_handler.py`
- Create RDF format handler for Native backend:
  - Implement `FormatHandler` interface
  - Use RDFLib for reading/writing RDF

**Implementation:**
```python
from pyodibel.core.backend import FormatHandler, BackendType
from rdflib import Graph

class RDFFormatHandler(FormatHandler):
    @property
    def format_name(self) -> str:
        return "rdf"
    
    def supports_backend(self, backend_type: BackendType) -> bool:
        return backend_type == BackendType.NATIVE
    
    def read(self, source: Any, backend: Any, **kwargs) -> Graph:
        format = kwargs.get('format', 'nt')
        return Graph().parse(source, format=format)
    
    def write(self, data: Graph, target: Any, backend: Any, **kwargs) -> None:
        format = kwargs.get('format', 'nt')
        data.serialize(destination=target, format=format)
```

### 5. **Utilities** → `utils/rdf.py`

#### `utils/rdf.py`
- Consolidate utility functions:
  - `hash_uri()` from multiple files → Single implementation
  - `read_uris_from_file()` → Single implementation
  - `build_recursive_json()` from `utils.py` → RDF to JSON converter
  - Common RDF operations

### 6. **Convert Operations** → `operations/convert.py`

#### `operations/convert.py`
- Migrate from `rdf_ops/utils.py`:
  - `build_recursive_json()` → `RDFToJSONConverter` (implements `ConvertOperation`)

## Migration Steps

### Phase 1: Create New Structure (Non-Breaking)
1. Create new directories:
   - `operations/rdf/`
   - `storage/rdf_file.py`
   - `utils/rdf.py`
2. Create abstract implementations that wrap legacy code
3. Keep legacy code in place for backward compatibility

### Phase 2: Implement New Interfaces
1. Implement operations using new interfaces
2. Implement storage backend using new interfaces
3. Implement format handlers
4. Add tests for new implementations

### Phase 3: Update Dependencies
1. Update code that uses `rdf_ops` to use new interfaces
2. Add deprecation warnings to legacy code
3. Update documentation

### Phase 4: Remove Legacy Code
1. Remove `ops/rdf_ops/` directory
2. Update all imports
3. Final testing

## Example: Filter Operation Migration

### Before (Legacy):
```python
from pyodibel.ops.rdf_ops.filter import filter_predicate

graph = Graph()
filtered = filter_predicate(graph, "http://example.org/name")
```

### After (New Architecture):
```python
from pyodibel.core.operations import OperationConfig
from pyodibel.core.backend import BackendConfig, BackendType
from pyodibel.operations.rdf.filter import RDFPredicateFilter

config = OperationConfig(
    name="filter_predicate",
    backend=BackendConfig(backend_type=BackendType.NATIVE),
    input_format="rdf",
    output_format="rdf"
)

filter_op = RDFPredicateFilter(config)
result = filter_op.apply_with_backend(
    data=graph,
    predicate="http://example.org/name"
)
```

## Benefits of Migration

1. **Consistency**: All operations follow the same interface
2. **Extensibility**: Easy to add new backends (e.g., Spark for RDF)
3. **Composability**: Operations can be chained together
4. **Testability**: Easier to test with mock backends
5. **Documentation**: Clear separation of concerns

## Backward Compatibility

During migration, maintain backward compatibility by:
1. Keeping legacy imports working (with deprecation warnings)
2. Providing adapter functions that wrap new implementations
3. Gradual migration of dependent code

## Testing Strategy

1. **Unit Tests**: Test each migrated operation independently
2. **Integration Tests**: Test operations with storage backends
3. **Regression Tests**: Ensure legacy functionality still works
4. **Performance Tests**: Compare old vs new implementations

## Timeline

- **Week 1-2**: Phase 1 - Create structure and wrappers
- **Week 3-4**: Phase 2 - Implement new interfaces
- **Week 5**: Phase 3 - Update dependencies
- **Week 6**: Phase 4 - Remove legacy code

## Notes

- Some code (like `FileHashStore`) is actively used in `resources/movie-multi-source-kg/generate.py`
- Need to ensure no breaking changes during migration
- Consider creating a compatibility layer for existing code

