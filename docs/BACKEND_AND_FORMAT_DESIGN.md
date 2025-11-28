# Backend and Format Design

## Overview

PyODIBEL operations support multiple execution backends (Spark, Pandas, Native) and data formats (CSV, JSON, RDF) through a flexible architecture that separates concerns and allows for easy extension.

## Architecture

### Components

1. **Execution Backends** (`core/backend.py`)
   - Provide the computational engine (Spark, Pandas, Native)
   - Handle data loading/saving in their native formats
   - Manage sessions and resources

2. **Format Handlers** (`core/backend.py`)
   - Provide format-specific read/write operations
   - Bridge between formats and backends
   - Support multiple backends per format

3. **Operations** (`core/operations.py`)
   - Use backends and format handlers
   - Provide unified interface regardless of backend/format
   - Handle automatic format conversion

## Design Pattern

The design uses a **Strategy Pattern** combined with **Adapter Pattern**:

```
Operation
  ├── ExecutionBackend (Strategy)
  │   ├── SparkBackend
  │   ├── PandasBackend
  │   └── NativeBackend
  │
  └── FormatHandler (Adapter)
      ├── CSVHandler (supports Spark, Pandas, Native)
      ├── JSONHandler (supports Spark, Pandas, Native)
      └── RDFHandler (supports Native, custom backends)
```

## Usage Examples

### Example 1: Using Spark with CSV

```python
from pyodibel.core.operations import FilterOperation
from pyodibel.core.backend import BackendConfig, BackendType

# Configure operation with Spark backend
config = OperationConfig(
    name="filter_by_type",
    backend=BackendConfig(
        backend_type=BackendType.SPARK,
        config={"master": "local[*]"}
    ),
    input_format="csv",
    output_format="csv"
)

# Create operation
filter_op = MyFilterOperation(config)

# Apply with automatic format handling
result = filter_op.apply_with_backend(
    data="/path/to/data.csv",
    criteria={"type": "Person"}
)
# Result is automatically saved as CSV using Spark
```

### Example 2: Using Pandas with JSON

```python
config = OperationConfig(
    name="transform_json",
    backend=BackendConfig(backend_type=BackendType.PANDAS),
    input_format="json",
    output_format="json"
)

transform_op = MyTransformOperation(config)

# Data is automatically loaded as Pandas DataFrame
result = transform_op.apply_with_backend(
    data="/path/to/data.json",
    mapping={"old_field": "new_field"}
)
```

### Example 3: Using Native Backend for RDF

```python
config = OperationConfig(
    name="filter_rdf",
    backend=BackendConfig(backend_type=BackendType.NATIVE),
    input_format="rdf",
    output_format="rdf"
)

filter_op = MyRDFFilterOperation(config)

# Uses RDFLib for RDF operations
result = filter_op.apply_with_backend(
    data="/path/to/data.nt",
    criteria={"predicate": "http://example.org/name"}
)
```

### Example 4: Format Conversion

```python
config = OperationConfig(
    name="convert_csv_to_json",
    backend=BackendConfig(backend_type=BackendType.PANDAS),
    input_format="csv",
    output_format="json"
)

convert_op = ConvertOperation(config)

# Automatically converts CSV to JSON using Pandas
result = convert_op.apply_with_backend(
    data="/path/to/data.csv",
    target="/path/to/output.json"
)
```

## Implementation Structure

### Execution Backend Implementation

```python
from pyodibel.core.backend import ExecutionBackend, BackendConfig, BackendType

class SparkBackend(ExecutionBackend):
    def initialize(self):
        from pyspark.sql import SparkSession
        self._session = SparkSession.builder \
            .appName(self.config.config.get("app_name", "PyODIBEL")) \
            .master(self.config.config.get("master", "local[*]")) \
            .getOrCreate()
    
    def load_data(self, source, format, **kwargs):
        if format == "csv":
            return self._session.read.csv(source, **kwargs)
        elif format == "json":
            return self._session.read.json(source, **kwargs)
        elif format == "parquet":
            return self._session.read.parquet(source, **kwargs)
        # ...
    
    def supports_format(self, format):
        return format in ["csv", "json", "parquet", "orc"]
```

### Format Handler Implementation

```python
from pyodibel.core.backend import FormatHandler, BackendType

class CSVFormatHandler(FormatHandler):
    @property
    def format_name(self):
        return "csv"
    
    def supports_backend(self, backend_type):
        return backend_type in [BackendType.SPARK, BackendType.PANDAS, BackendType.NATIVE]
    
    def read(self, source, backend, **kwargs):
        if backend.config.backend_type == BackendType.SPARK:
            return backend.get_session().read.csv(source, **kwargs)
        elif backend.config.backend_type == BackendType.PANDAS:
            import pandas as pd
            return pd.read_csv(source, **kwargs)
        else:  # Native
            import csv
            with open(source, 'r') as f:
                return list(csv.DictReader(f))
    
    def write(self, data, target, backend, **kwargs):
        if backend.config.backend_type == BackendType.SPARK:
            data.write.csv(target, **kwargs)
        elif backend.config.backend_type == BackendType.PANDAS:
            data.to_csv(target, **kwargs)
        else:  # Native
            import csv
            with open(target, 'w') as f:
                writer = csv.DictWriter(f, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
```

### Operation Implementation

```python
from pyodibel.core.operations import FilterOperation, OperationConfig

class MyFilterOperation(FilterOperation):
    def _apply_backend(self, data, backend, **kwargs):
        criteria = kwargs.get('criteria', {})
        
        if backend.config.backend_type == BackendType.SPARK:
            # Spark DataFrame operations
            for key, value in criteria.items():
                data = data.filter(data[key] == value)
            return data
        
        elif backend.config.backend_type == BackendType.PANDAS:
            # Pandas DataFrame operations
            for key, value in criteria.items():
                data = data[data[key] == value]
            return data
        
        else:  # Native
            # Native Python operations
            return [row for row in data if all(row.get(k) == v for k, v in criteria.items())]
    
    def filter(self, data, criteria, **kwargs):
        # Fallback for when no backend is specified
        return [row for row in data if all(row.get(k) == v for k, v in criteria.items())]
```

## Registration

Backends and format handlers are registered in a global registry:

```python
from pyodibel.core.backend import get_backend_registry, BackendType

registry = get_backend_registry()

# Register backends
registry.register_backend(BackendType.SPARK, SparkBackend)
registry.register_backend(BackendType.PANDAS, PandasBackend)
registry.register_backend(BackendType.NATIVE, NativeBackend)

# Register format handlers
registry.register_format_handler("csv", CSVFormatHandler)
registry.register_format_handler("json", JSONFormatHandler)
registry.register_format_handler("rdf", RDFFormatHandler)
```

## Benefits

1. **Flexibility**: Operations can use any backend/format combination
2. **Extensibility**: Easy to add new backends or formats
3. **Separation of Concerns**: Backend logic separate from format logic
4. **Automatic Handling**: Operations handle format conversion automatically
5. **Backward Compatibility**: Operations can work without backends (native mode)

## Backend Support Matrix

| Format | Spark | Pandas | Native |
|--------|-------|--------|--------|
| CSV    | ✅    | ✅     | ✅     |
| JSON   | ✅    | ✅     | ✅     |
| RDF    | ⚠️*   | ❌     | ✅     |
| Parquet| ✅    | ✅     | ❌     |
| ORC    | ✅    | ❌     | ❌     |

*RDF with Spark requires custom handling (e.g., converting to DataFrame first)

## Future Extensions

- **Dask Backend**: For distributed computing with Dask
- **Polars Backend**: For high-performance DataFrame operations
- **Custom Format Handlers**: For domain-specific formats
- **Backend Chaining**: Use multiple backends in a pipeline
- **Format Inference**: Automatically detect formats

