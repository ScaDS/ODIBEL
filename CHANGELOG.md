# Changelog

All notable changes to PyODIBEL will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

#### Data Sources
- **Wikidata Dump Source** (`source/wikidata/dump.py`)
  - Added `WikidataDumpSource` for ingesting large Wikidata JSON dumps using Spark
  - Supports both JSON array and JSONL formats
  - Implements streaming ingestion with chunking support
  - Supports filtering and field selection during ingestion
  - Automatic format detection (JSON vs JSONL)
  - Backend-agnostic design with Spark as default backend

- **Wikidata Entity Models** (`source/wikidata/entity.py`)
  - Added Pydantic models for Wikidata entities:
    - `WikidataEntity`: Main entity model with id, type, labels, descriptions, aliases, claims, sitelinks
    - `WikidataLabel`, `WikidataDescription`, `WikidataAlias`: Supporting models
    - `WikidataClaim`, `WikidataSnak`: For representing Wikidata statements
    - `WikidataSiteLink`: For Wikipedia inter-language links
  - Helper methods: `get_label()`, `get_description()`, `get_claims()`
  - Full validation and type safety with Pydantic

- **Linked Data Source** (`source/linked_data/source.py`)
  - Added `LinkedDataSource` for consuming and crawling RDF graphs via HTTP
  - Supports HTTP content negotiation for RDF formats (RDF/XML, Turtle, N-Triples, JSON-LD)
  - Recursive crawling with configurable depth limits
  - Rate limiting support
  - Domain and predicate-based filtering for link following
  - Follows Linked Data principles and best practices

- **Source Module Reorganization**
  - Reorganized source module into subdirectories by source type:
    - `source/wikidata/`: Wikidata-related sources and models
    - `source/linked_data/`: Linked Data (RDF) sources
    - `source/sparql/`: Placeholder for future SPARQL endpoint sources
    - `source/rest/`: Placeholder for future REST API sources
    - `source/web/`: Placeholder for future web page sources
  - Maintains backward compatibility through `source/__init__.py`

#### Storage Backends
- **Spark Backend** (`storage/spark_backend.py`)
  - Added `SparkBackend` implementation of `ExecutionBackend`
  - Supports multiple formats: JSON, JSONL, CSV, Parquet, Text
  - Configurable Spark session with memory and partition settings
  - Context manager support for resource cleanup
  - Lazy initialization with auto-init option

#### Format Handlers
- **JSON Format Handlers** (`operations/json_handler.py`)
  - Added `SparkJSONFormatHandler` for Spark backend
  - Added `PandasJSONFormatHandler` for Pandas backend
  - Added `NativeJSONFormatHandler` for native Python backend
  - Supports both JSON arrays and JSONL (JSON Lines) formats
  - Automatic format detection

#### Core Infrastructure
- **Backend Registry** (`core/backend_registry.py`)
  - Added backend and format handler registration system
  - Automatic discovery and registration of backends
  - Format handler lookup by format name and backend type

- **Backend Abstraction** (`core/backend.py`)
  - Enhanced `ExecutionBackend` interface with format handler support
  - Added `FormatHandler` interface for format-specific read/write operations
  - Support for multiple backend types: Spark, Pandas, Native, Dask

#### Testing
- **Wikidata Dump Tests** (`test/source/test_wikidata_dump.py`)
  - Comprehensive test suite for Wikidata dump ingestion
  - Tests for Spark backend initialization and data loading
  - Tests for JSON format handlers
  - Tests for WikidataDumpSource with various configurations
  - Tests for filtering and streaming operations
  - Tests for entity model structure and validation
  - Tests for specific entity value assertions (Q23, Q24)
  - Java availability checking with SDKMAN support
  - Slow test markers for Spark-dependent tests

#### Documentation
- **Package Structure Documentation** (`docs/PACKAGE_STRUCTURE.md`)
  - Detailed documentation of package organization
  - Module descriptions and responsibilities
  - Import examples and usage patterns

- **Wikidata Ingestion Guide** (`docs/WIKIDATA_INGESTION.md`)
  - Complete guide for ingesting Wikidata JSON dumps
  - Examples for basic ingestion, streaming, and filtering
  - Configuration options and best practices

- **Linked Data Source Guide** (`docs/LINKED_DATA_SOURCE.md`)
  - Guide for using Linked Data source
  - Examples for single URI fetching and recursive crawling
  - Configuration options for rate limiting and filtering

- **Operations Structure Proposal** (`docs/OPERATIONS_STRUCTURE_PROPOSAL.md`)
  - Proposed structure for operations module
  - Design principles and implementation strategies
  - Examples for backend-agnostic and backend-specific operations

- **Operations Usability Analysis** (`docs/OPERATIONS_USABILITY_ANALYSIS.md`)
  - Usability analysis of operations module design
  - Comparison of different API approaches
  - Recommendations for high-level and advanced APIs

- **RDF Operations Migration Plan** (`docs/RDF_OPS_MIGRATION_PLAN.md`)
  - Plan for migrating legacy RDF operations
  - Mapping of old code to new architecture

### Changed

#### Source Module
- **Breaking Change**: Reorganized source module structure
  - Moved `wikidata_dump.py` → `wikidata/dump.py`
  - Moved `wikidata_entity.py` → `wikidata/entity.py`
  - Moved `mapping.py` → `wikidata/mapping.py`
  - Moved `linked_data.py` → `linked_data/source.py`
  - **Note**: Backward compatibility maintained through `source/__init__.py`
  - Direct imports from submodules also supported: `from pyodibel.source.wikidata import ...`

#### Wikidata Entity Model
- Fixed `WikidataAlias.value` type from `List[str]` to `str` to match actual Wikidata dump structure
- Added proper Pydantic validation for all entity fields

#### Spark Backend
- Fixed initialization order issue where `_spark` was set to `None` after parent initialization
- Now sets `_spark = None` before calling `super().__init__()` to preserve auto-initialization

#### Wikidata Dump Source
- Refactored to avoid SparkContext serialization errors
  - Made `_parse_json_line_to_dict` and `_check_filter_dict` static methods
  - Moved Pydantic model creation to driver side (after `collect()`)
  - Operations in Spark transformations now use pure Python functions only
- Improved error handling for invalid JSON lines
- Better support for both JSON array and JSONL formats

### Fixed

#### Spark Integration
- **Fixed**: `PySparkRuntimeError: [CONTEXT_ONLY_VALID_ON_DRIVER]` when creating Pydantic models in Spark transformations
  - Solution: Parse JSON to dicts in Spark transformations, create Pydantic models on driver side
  - Made parsing functions static to avoid capturing `self` (which contains SparkContext)

#### Backend Initialization
- **Fixed**: `SparkBackend` initialization failing when `auto_init=True`
  - Solution: Set `_spark = None` before calling `super().__init__()` to preserve initialized session

#### Test Infrastructure
- **Fixed**: Tests skipping due to Java availability issues
  - Added Java version checking with SDKMAN support
  - Clear error messages with instructions for setting up Java via SDKMAN
  - Session-scoped fixture for Java availability check

### Technical Details

#### Architecture Improvements
- **Separation of Concerns**: Clear separation between:
  - Data sources (ingestion)
  - Storage backends (execution engines)
  - Format handlers (format ↔ backend bridge)
  - Operations (data transformations)

- **Backend Abstraction**: Operations work with any backend through the `ExecutionBackend` interface
- **Format Handlers**: Reusable format handlers bridge formats and backends
- **Static Methods**: Used in Spark transformations to avoid serialization issues

#### Code Quality
- All tests passing (21 tests for Wikidata dump source)
- Proper error handling and logging
- Type hints throughout
- Pydantic models for data validation
- Comprehensive test coverage

### Migration Guide

#### For Users of Source Module

**Old way** (still works):
```python
from pyodibel.source import WikidataDumpSource, WikidataEntity
```

**New way** (also works):
```python
from pyodibel.source.wikidata import WikidataDumpSource, WikidataEntity
```

**No breaking changes** - backward compatibility maintained!

### Dependencies

- **PySpark**: Required for Spark backend (optional dependency)
- **Java 17+**: Required for PySpark (can be managed via SDKMAN)
- **Pydantic**: Required for entity models and configuration
- **RDFLib**: Required for Linked Data source
- **requests**: Required for HTTP requests in Linked Data source

### Known Issues

- None currently known

### Future Work

- [ ] Implement SPARQL endpoint source
- [ ] Implement REST API source
- [ ] Implement web page source
- [ ] Migrate legacy RDF operations to new architecture
- [ ] Add more format handlers (CSV, Parquet, RDF)
- [ ] Implement filter, transform, join, and convert operations
- [ ] Add high-level API for operations (see `docs/OPERATIONS_USABILITY_ANALYSIS.md`)
- [ ] Add more comprehensive examples
- [ ] Performance optimizations for large-scale processing

