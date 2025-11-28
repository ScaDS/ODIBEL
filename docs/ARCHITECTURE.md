# PyODIBEL Architecture

## Overview

PyODIBEL is designed with a clean, modular architecture that separates concerns and provides abstract interfaces for all major components. This design allows for flexible implementations while maintaining a consistent API.

## Architecture Principles

1. **Interface-Based Design**: All major components implement abstract base classes, ensuring consistent APIs
2. **Separation of Concerns**: Clear boundaries between data sources, storage, operations, and benchmark construction
3. **Composability**: Components can be combined to build complex pipelines
4. **Extensibility**: Easy to add new implementations without modifying existing code
5. **Format Agnostic**: Support for multiple data formats (RDF, JSON, CSV, XML, etc.)

## Core Components

### 1. Data Sources (`core/data_source.py`)

Abstract interfaces for connecting to various data sources:

- **`DataSource`**: Base interface for all data sources
  - `fetch()`: Fetch data from the source
  - `supports_format()`: Check format support
  
- **`StreamingDataSource`**: For sources that support streaming
  - `fetch_stream()`: Fetch data in chunks
  
- **`BatchDataSource`**: For sources that support batch operations
  - `fetch_batch()`: Fetch data in batches

**Planned Implementations:**
- `SPARQLSource`: Query SPARQL endpoints
- `RESTSource`: Fetch from REST APIs
- `WebSource`: Retrieve web pages via content negotiation
- `DumpSource`: Process large data dumps

### 2. Storage Backends (`core/storage.py`)

Abstract interfaces for persistent storage:

- **`StorageBackend`**: Base interface for storage
  - `save()`: Save data to storage
  - `load()`: Load data from storage
  - `exists()`: Check if data exists
  - `delete()`: Delete data
  - `list()`: List available data
  
- **`PartitionedStorage`**: For storage with partitioning support
  - `create_partition()`: Create a partition
  - `list_partitions()`: List partitions
  - `get_partition_metadata()`: Get partition metadata
  
- **`TableStorage`**: For table/record-based storage
  - `create_table()`: Create a table
  - `insert_records()`: Insert records
  - `query_records()`: Query records

**Planned Implementations:**
- `FileStorage`: File-based storage
- `DatabaseStorage`: SQL database storage
- `S3Storage`: S3-compatible object storage

### 3. Data Operations (`core/operations.py`)

Abstract interfaces for data transformations:

- **`DataOperation`**: Base interface for operations
  - `apply()`: Apply the operation
  
- **`TransformOperation`**: For data transformations
  - `transform()`: Transform data with mapping
  
- **`FilterOperation`**: For filtering data
  - `filter()`: Filter by criteria
  
- **`JoinOperation`**: For joining/merging data
  - `join()`: Join multiple datasets
  
- **`ConvertOperation`**: For format conversion
  - `convert()`: Convert between formats
  - `supports_conversion()`: Check conversion support

**Planned Implementations:**
- Transform operations (mapping, restructuring)
- Filter operations (by type, attribute, predicate)
- Join operations (inner, outer, fusion)
- Convert operations (JSON ↔ CSV ↔ RDF)

### 4. Partitioning Strategies (`core/partition.py`)

Abstract interfaces for data partitioning:

- **`PartitionStrategy`**: Base interface for partitioning
  - `partition()`: Partition data
  - `get_partition_key()`: Get partition key for item
  
- **`EntityPartitionStrategy`**: Entity-centric partitioning
  - `extract_entity_id()`: Extract entity ID
  
- **`AttributePartitionStrategy`**: Attribute-centric partitioning
  - `extract_attribute()`: Extract attribute
  
- **`TypePartitionStrategy`**: Type-centric partitioning
  - `extract_type()`: Extract type

**Planned Implementations:**
- Entity-based partitioning
- Attribute-based partitioning
- Type-based partitioning

### 5. Benchmark Construction (`core/benchmark.py`)

Abstract interfaces for benchmark creation:

- **`BenchmarkBuilder`**: Base interface for building benchmarks
  - `create_split()`: Create dataset splits
  - `generate_ground_truth()`: Generate ground-truth
  - `build()`: Build complete benchmark
  
- **`Benchmark`**: Represents a complete benchmark
  - `get()`: Get data for a split (Dataset.get(...) interface)
  - `save()`: Save benchmark to disk
  - `load()`: Load benchmark from disk

**Planned Implementations:**
- Train/test/validation splits
- Stratified splits
- Temporal splits
- Entity matching ground-truth
- Entity linking ground-truth

### 6. Link Management (`link/`)

Abstract interfaces for entity linking and matching:

- **`EntityClustering`**: Cluster records referring to same entity
  - `cluster()`: Cluster records
  - `get_cluster_id()`: Get cluster ID for record
  
- **`EntityMatching`**: Match entities across sources
  - `match()`: Match entities between datasets
  - `find_matches()`: Find matches for single entity

**Planned Implementations:**
- Clustering strategies
- Matching strategies
- Link validation

## Data Flow

```
Data Sources → Storage → Operations → Partitioning → Benchmark Construction
     ↓              ↓          ↓          ↓            ↓                  ↓
  SPARQL      Transform    File/DB    Filter      Entity/Attr/Type    Splits
  REST        Validate     S3         Join        Partitioning         Ground Truth
  Web         Convert       ...        Convert     ...
  Dumps       ...
```

## Usage Example

```python
from pyodibel import DataSource, StorageBackend, BenchmarkBuilder
from pyodibel.core import DataSourceConfig, StorageConfig, BenchmarkConfig
from pyodibel.core.benchmark import SplitConfig, GroundTruthConfig

# 1. Configure components
source_config = DataSourceConfig(name="dbpedia", metadata={"endpoint": "https://dbpedia.org/sparql"})
storage_config = StorageConfig(name="file", base_path=Path("/data/storage"))
benchmark_config = BenchmarkConfig(name="movie_benchmark", version="1.0")

# 2. Create instances (implementations to be added)
# source = SPARQLSource(source_config)
# storage = FileStorage(storage_config)
# builder = BenchmarkBuilder(benchmark_config)

# 3. Fetch data from source
# data = source.fetch(query="SELECT * WHERE { ?s ?p ?o } LIMIT 1000")

# 4. Store data
# storage.save(data, partition="entity")

# 5. Build benchmark
# split_config = SplitConfig(train_ratio=0.8, test_ratio=0.2)
# gt_config = GroundTruthConfig(method="entity_matching")
# benchmark = builder.build(data, split_config, gt_config)

# 6. Access benchmark data
# train_data = benchmark.get(SplitType.TRAIN)
# test_data = benchmark.get(SplitType.TEST)
```

## Extension Points

The architecture provides several extension points:

1. **New Data Sources**: Implement `DataSource` interface
2. **New Storage Backends**: Implement `StorageBackend` interface
3. **New Operations**: Implement `DataOperation` or specific operation interfaces
4. **New Partitioning Strategies**: Implement `PartitionStrategy` interface
5. **New Benchmark Builders**: Implement `BenchmarkBuilder` interface
6. **New Link Management**: Implement `EntityClustering` or `EntityMatching` interfaces

## Next Steps

1. Implement concrete data source classes (SPARQL, REST, Web, Dump)
2. Implement storage backends (File, Database, S3)
3. Implement data operations (Transform, Filter, Join, Convert)
4. Implement partitioning strategies (Entity, Attribute, Type)
5. Implement benchmark construction (Splits, Ground Truth)
6. Implement link management (Clustering, Matching)
7. Add utilities for common operations (RDF, format conversion)
8. Create CLI interface for common workflows

