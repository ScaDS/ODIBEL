# PyODIBEL

**Open Data Integration Benchmark Evaluation Lab**

PyODIBEL is a Python library for generating and evaluating benchmark datasets for data integration tasks—such as entity matching and entity linking—across different domains. These benchmarks are built from Linked Open Data (LOD) sources and large public dumps such as Wikipedia and Wikidata.

## Features

- **Multi-Source Data Ingestion**: Support for SPARQL endpoints, REST APIs, web pages, and data dumps
- **Flexible Storage**: File-based, database, or S3-compatible storage backends
- **Multiple Execution Backends**: Operations can use Spark, Pandas, or native Python utilities
- **Format Support**: Work with CSV, JSON, RDF, and other formats seamlessly
- **Partitioning Strategies**: Entity-centric, attribute-centric, and type-centric partitioning
- **Benchmark Construction**: Create reproducible datasets with splits and ground-truth annotations
- **Entity Linking**: Support for entity matching and clustering across sources

## Installation

```bash
# Install from source
pip install -e .

# Or install with optional dependencies
pip install -e ".[test]"
```

## Quick Start

```python
from pyodibel import DataSource, StorageBackend, BenchmarkBuilder
from pyodibel.core import DataSourceConfig, StorageConfig, BenchmarkConfig
from pyodibel.core.backend import BackendConfig, BackendType
from pyodibel.core.operations import OperationConfig
from pyodibel.source import WikidataDumpSource
from pathlib import Path

# 1. Configure a data source
source_config = DataSourceConfig(
    name="dbpedia",
    metadata={"endpoint": "https://dbpedia.org/sparql"}
)

# 2. Configure storage
storage_config = StorageConfig(
    name="file",
    base_path=Path("/data/storage")
)

# 3. Configure operations with backend
operation_config = OperationConfig(
    name="filter_persons",
    backend=BackendConfig(backend_type=BackendType.SPARK),
    input_format="csv",
    output_format="csv"
)

# 4. Build a benchmark
benchmark_config = BenchmarkConfig(
    name="movie_benchmark",
    version="1.0"
)
```

## Architecture

PyODIBEL follows a modular, interface-based design that separates concerns and allows for flexible implementations:

### Core Components

1. **Data Sources** (`core/data_source.py`)
   - Abstract interfaces for ingesting data from various sources
   - Supports SPARQL, REST APIs, web pages, and data dumps

2. **Storage Backends** (`core/storage.py`)
   - Abstract interfaces for persistent storage
   - Supports file systems, databases, and object storage

3. **Execution Backends** (`core/backend.py`)
   - Support for multiple computational engines (Spark, Pandas, Native)
   - Format handlers bridge formats and backends

4. **Data Operations** (`core/operations.py`)
   - Transformations, filtering, joins, and format conversions
   - Backend-agnostic operations that work with any execution engine

5. **Partitioning** (`core/partition.py`)
   - Entity-centric, attribute-centric, and type-centric strategies

6. **Benchmark Construction** (`core/benchmark.py`)
   - Dataset splitting and ground-truth generation
   - Reproducible benchmark creation

### Package Structure

```
pyodibel/
├── core/              # Abstract interfaces and base classes
├── source/            # Data source implementations
├── storage/           # Storage backend implementations
├── operations/        # Data operation implementations
├── partition/         # Partitioning strategy implementations
├── benchmark/         # Benchmark construction implementations
├── link/              # Entity linking and matching
├── cli/               # Command-line interface
└── utils/             # Utility functions
```

## Usage Examples

### Example 1: Filter Operation with Spark and CSV

```python
from pyodibel.core.operations import FilterOperation, OperationConfig
from pyodibel.core.backend import BackendConfig, BackendType

class MyFilterOperation(FilterOperation):
    def _apply_backend(self, data, backend, **kwargs):
        criteria = kwargs.get('criteria', {})
        # data is already a Spark DataFrame
        for key, value in criteria.items():
            data = data.filter(data[key] == value)
        return data

config = OperationConfig(
    name="filter_persons",
    backend=BackendConfig(backend_type=BackendType.SPARK),
    input_format="csv",
    output_format="csv"
)

filter_op = MyFilterOperation(config)
result = filter_op.apply_with_backend(
    data="/path/to/data.csv",
    criteria={"type": "Person"}
)
```

### Example 2: Format Conversion

```python
from pyodibel.core.operations import ConvertOperation

config = OperationConfig(
    name="csv_to_json",
    backend=BackendConfig(backend_type=BackendType.PANDAS),
    input_format="csv",
    output_format="json"
)

converter = ConvertOperation(config)
result = converter.convert(
    data="/path/to/data.csv",
    from_format="csv",
    to_format="json"
)
```

## Documentation

- **[Package Structure](docs/PACKAGE_STRUCTURE.md)**: Detailed package organization
- **[Architecture](docs/ARCHITECTURE.md)**: Architecture principles and design patterns
- **[Backend and Format Design](docs/BACKEND_AND_FORMAT_DESIGN.md)**: How backends and formats work together
- **[Operations Examples](docs/OPERATIONS_EXAMPLES.md)**: Concrete examples for different backends and formats

## Data Sources

PyODIBEL supports data from:

- **SPARQL Endpoints**: Query RDF data from SPARQL endpoints
- **REST APIs**: Fetch data from RESTful services
- **Linked Data**: Consume and crawl RDF graphs via HTTP with content negotiation
- **Web Pages**: Retrieve data via HTTP with content negotiation (HTML/RDF)
- **Data Dumps**: Process large dumps (Wikidata, Wikipedia, etc.) in JSON, XML, RDF, or SQL formats

## Execution Backends

Operations can use different execution backends:

- **Spark**: For large-scale distributed processing
- **Pandas**: For in-memory data manipulation
- **Native**: Custom Python utilities (e.g., RDFLib for RDF operations)

## Format Support

Format handlers provide seamless support for:

- **CSV**: With Spark, Pandas, or native Python
- **JSON**: With Spark, Pandas, or native Python
- **RDF**: With native Python (RDFLib) or custom backends
- **Parquet**: With Spark or Pandas
- **Other formats**: Extensible through format handlers

## Benchmark Generation Pipeline

1. **Data Access & Ingestion**: Fetch data from various sources
2. **Management Layer**: Partition and store data
3. **Data Operations**: Transform, filter, join, and convert data
4. **Benchmark Construction**: Create splits and generate ground-truth

## Resources

The `resources/` directory contains previously generated datasets:

- **DBpedia Temporal Knowledge Graph**: Temporal KG with triple lifespans
- **Movie Multi-Source KG**: Integration benchmark for movie data

## Development

```bash
# Install development dependencies
pip install -e ".[test]"

# Run tests
pytest

# Run linting
# (configure your preferred linter)
```

## Requirements

- Python >= 3.8
- See `pyproject.toml` for full dependency list

## Contributing

This project is in active development. The current focus is on:

1. Implementing concrete data source classes
2. Implementing storage backends
3. Implementing data operations with backend support
4. Implementing partitioning strategies
5. Implementing benchmark construction

## License

See `LICENSE` file for details.

## Citation

If you use PyODIBEL in your research, please cite:

```bibtex
@software{pyodibel,
  title = {PyODIBEL: Open Data Integration Benchmark Evaluation Lab},
  author = {Odibel Team},
  year = {2025},
  url = {https://github.com/your-org/odibel}
}
```

## Contact

For questions or contributions, please contact the Odibel Team at team@odibel.org.
