# PyODIBEL Package Structure

## Overview

PyODIBEL is organized into modular components that support the complete benchmark generation pipeline, from data source connections to benchmark construction.

## Package Structure

```
pyodibel/
├── __init__.py                 # Package initialization and public API
├── core/                       # Core abstractions and interfaces
│   ├── __init__.py
│   ├── data_source.py          # Abstract data source interfaces
│   ├── storage.py               # Abstract storage interfaces
│   ├── operations.py           # Abstract operation interfaces
│   ├── partition.py             # Abstract partitioning interfaces
│   └── benchmark.py             # Abstract benchmark interfaces
├── source/                     # Data source implementations
│   ├── __init__.py
│   ├── sparql.py               # SPARQL endpoint data source
│   ├── rest.py                 # REST API data source
│   ├── linked_data.py          # Linked Data source (HTTP with content negotiation for RDF)
│   ├── web.py                  # Web page data source (HTML/RDF via content negotiation)
│   └── dump.py                 # Data dump source (JSON, XML, RDF, SQL)
├── storage/                    # Storage implementations
│   ├── __init__.py
│   ├── file.py                 # File-based storage
│   ├── database.py             # Database storage (SQLAlchemy-based)
│   └── s3.py                   # S3-compatible object storage
├── partition/                  # Partitioning strategies
│   ├── __init__.py
│   ├── entity.py               # Entity-centric partitioning
│   ├── attribute.py            # Attribute-centric partitioning
│   └── type.py                 # Type-centric partitioning
├── operations/                 # Data operations
│   ├── __init__.py
│   ├── transform.py            # Data transformation operations
│   ├── filter.py               # Filtering operations
│   ├── join.py                 # Join/merge/fusion operations
│   └── convert.py              # Format conversion (JSON ↔ CSV ↔ RDF)
├── benchmark/                  # Benchmark construction
│   ├── __init__.py
│   ├── split.py                # Dataset splitting strategies
│   └── ground_truth.py         # Ground-truth generation
├── link/                       # Link management (entity matching/clustering)
│   ├── __init__.py
│   ├── clustering.py           # Entity clustering strategies
│   └── matching.py              # Entity matching strategies
├── cli/                        # Command-line interface
│   ├── __init__.py
│   └── main.py                 # Main CLI entry point
└── utils/                      # Utility functions
    ├── __init__.py
    ├── rdf.py                  # RDF utilities
    └── format.py               # Format utilities
```

## Component Descriptions

### Core (`core/`)

Contains abstract base classes and protocols that define the interfaces for all major components. These interfaces ensure consistency and allow for different implementations while maintaining a common API.

**Key Interfaces:**
- `DataSource`: Base interface for all data sources
- `StorageBackend`: Base interface for storage implementations
- `DataOperation`: Base interface for data transformations
- `PartitionStrategy`: Base interface for partitioning strategies
- `BenchmarkBuilder`: Base interface for benchmark construction

### Source (`source/`)

Implements data source connections for various sources:
- **SPARQL**: Query SPARQL endpoints for RDF data
- **REST**: Fetch data from REST APIs
- **Web**: Retrieve data from web pages via HTTP with content negotiation
- **Dump**: Process large data dumps (Wikidata, Wikipedia, etc.)

### Storage (`storage/`)

Provides storage backends for managing ingested data:
- **File**: Local or network file system storage
- **Database**: SQL database storage via SQLAlchemy
- **S3**: Object storage for cloud deployments

### Partition (`partition/`)

Implements different partitioning strategies:
- **Entity-centric**: Partition by entity identifiers
- **Attribute-centric**: Partition by attribute/property
- **Type-centric**: Partition by entity types/classes

### Operations (`operations/`)

Data manipulation operations:
- **Transform**: Apply transformations to data
- **Filter**: Filter data by various criteria
- **Join**: Merge data from multiple sources
- **Convert**: Convert between data formats

### Benchmark (`benchmark/`)

Benchmark-specific functionality:
- **Split**: Create train/test/validation splits
- **Ground Truth**: Generate ground-truth annotations for evaluation

### Link (`link/`)

Entity linking and matching:
- **Clustering**: Group records referring to the same entity
- **Matching**: Match entities across different sources

## Design Principles

1. **Interface-based Design**: All major components implement abstract interfaces, allowing for flexible implementations
2. **Composability**: Components can be combined to build complex pipelines
3. **Format Agnostic**: Support for multiple data formats (RDF, JSON, CSV, etc.)
4. **Extensibility**: Easy to add new data sources, storage backends, or operations
5. **Reproducibility**: All operations are deterministic and traceable

## Usage Pattern

```python
from pyodibel import DataSource, StorageBackend, BenchmarkBuilder
from pyodibel.source import SPARQLSource
from pyodibel.storage import FileStorage
from pyodibel.benchmark import TrainTestSplit

# 1. Connect to data source
source = SPARQLSource(endpoint="https://dbpedia.org/sparql")
data = source.fetch(query="SELECT * WHERE { ?s ?p ?o } LIMIT 100")

# 2. Store data
storage = FileStorage(base_path="/path/to/storage")
storage.save(data, partition="entity")

# 3. Build benchmark
builder = BenchmarkBuilder(storage=storage)
benchmark = builder.create(
    split_strategy=TrainTestSplit(train=0.8, test=0.2),
    ground_truth_generator=EntityMatchingGT()
)
```

