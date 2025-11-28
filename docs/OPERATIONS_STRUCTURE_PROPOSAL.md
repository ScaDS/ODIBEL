# Operations Module Structure Proposal

## Overview

The operations module needs to support:
- **Operation Types**: Transform, Filter, Join, Convert
- **Formats**: CSV, JSON, RDF, XML, Parquet, etc.
- **Backends**: Spark, Pandas, Native

## Proposed Structure

```
operations/
├── __init__.py                    # Public API exports
├── format_handlers/              # Format handlers (infrastructure)
│   ├── __init__.py
│   ├── json_handler.py          # JSON format handlers (Spark, Pandas, Native)
│   ├── csv_handler.py           # CSV format handlers
│   ├── rdf_handler.py           # RDF format handlers
│   └── parquet_handler.py       # Parquet format handlers
├── filter/                       # Filter operations
│   ├── __init__.py
│   ├── base.py                  # Base filter operation (backend-agnostic)
│   ├── csv/                     # CSV filter implementations
│   │   ├── __init__.py
│   │   ├── generic.py          # Backend-agnostic CSV filter
│   │   ├── spark.py            # Spark-optimized CSV filter (if needed)
│   │   └── pandas.py          # Pandas-optimized CSV filter (if needed)
│   ├── json/                    # JSON filter implementations
│   │   ├── __init__.py
│   │   └── generic.py
│   └── rdf/                     # RDF filter implementations
│       ├── __init__.py
│       └── rdf_filter.py       # RDF-specific (Graph-based)
├── transform/                    # Transform operations
│   ├── __init__.py
│   ├── base.py
│   ├── csv/
│   │   ├── __init__.py
│   │   ├── generic.py
│   │   └── spark.py            # Spark-specific optimizations
│   ├── json/
│   └── rdf/
├── join/                         # Join/merge operations
│   ├── __init__.py
│   ├── base.py
│   ├── csv/
│   ├── json/
│   └── rdf/
└── convert/                      # Format conversion operations
    ├── __init__.py
    ├── base.py
    └── converters.py            # Format converters
```

## Design Principles

### 1. **Operation Type First**
Operations are primarily organized by what they do (filter, transform, join, convert), not by format or backend.

### 2. **Format Handlers Separate**
Format handlers (`format_handlers/`) are infrastructure that bridges formats and backends. They're not operations themselves.

### 3. **Backend-Agnostic by Default, Backend-Specific When Needed**

**Default**: Most operations should be **backend-agnostic** and work with any backend via the abstraction:

```python
# filter/csv/generic.py
class CSVFilterOperation(FilterOperation):
    """Backend-agnostic CSV filter - works with Spark, Pandas, or Native."""
    
    def _apply_backend(self, data, backend, **kwargs):
        # Use format handler to read (handles backend differences)
        format_handler = self._get_format_handler("csv")
        df = format_handler.read(data, backend)
        
        # Apply filter using backend-agnostic DataFrame operations
        criteria = kwargs.get('criteria', {})
        filtered = self._filter_dataframe(df, criteria, backend)
        
        return filtered
    
    def _filter_dataframe(self, df, criteria, backend):
        """Filter using backend-agnostic operations."""
        # Works with Spark DataFrame, Pandas DataFrame, or native dict/list
        # Backend abstraction provides common interface
        for key, value in criteria.items():
            if backend.config.backend_type == BackendType.SPARK:
                df = df.filter(df[key] == value)
            elif backend.config.backend_type == BackendType.PANDAS:
                df = df[df[key] == value]
            else:
                # Native implementation
                df = [row for row in df if row.get(key) == value]
        return df
```

**When Needed**: Create backend-specific implementations for optimizations or special features:

```python
# filter/csv/spark.py
class SparkCSVFilterOperation(CSVFilterOperation):
    """Spark-optimized CSV filter with advanced features."""
    
    def _apply_backend(self, data, backend, **kwargs):
        # Use Spark-specific optimizations
        # - Predicate pushdown
        # - Partition pruning
        # - Broadcast joins for filter criteria
        df = backend._spark.read.csv(data, header=True, inferSchema=True)
        
        # Spark-specific optimizations
        df = df.filter(df["type"] == "Person")
        df = df.hint("broadcast")  # Spark-specific
        
        return df
```

### 4. **Format-Specific When Needed**
Some operations need format-specific implementations due to semantic differences (RDF graphs vs. tabular data).

## Implementation Strategy

### Option A: Backend-Agnostic Generic Operations (Recommended)

**Most operations should be backend-agnostic** because:
- Format handlers already handle backend differences
- Backend abstraction provides common DataFrame-like interface
- Reduces code duplication
- Easier to maintain

```python
# filter/csv/generic.py
class CSVFilterOperation(FilterOperation):
    """Works with any backend via abstraction."""
    
    def _apply_backend(self, data, backend, **kwargs):
        # Format handler handles backend differences
        handler = self._get_format_handler("csv")
        df = handler.read(data, backend)
        
        # Use backend's common interface
        filtered = self._apply_filter_logic(df, kwargs['criteria'], backend)
        
        return filtered
```

### Option B: Backend-Specific When Needed

Create backend-specific implementations **only when**:
- Performance optimizations are critical
- Backend-specific features are required
- The operation logic differs significantly

```python
# filter/csv/spark.py - Only if Spark-specific optimizations needed
class SparkCSVFilterOperation(CSVFilterOperation):
    """Spark-optimized version with predicate pushdown, etc."""
    pass

# filter/csv/pandas.py - Only if Pandas-specific optimizations needed  
class PandasCSVFilterOperation(CSVFilterOperation):
    """Pandas-optimized version with vectorized operations."""
    pass
```

## Recommended Approach

**Use Option A (Backend-Agnostic) by default**, with Option B (Backend-Specific) as exceptions:

1. **Start with generic operations** that work with all backends
2. **Add backend-specific implementations** only when:
   - Performance profiling shows significant gains
   - Backend-specific features are required
   - The abstraction doesn't cover the use case

## Example: CSV Filter with Pandas or Spark

```python
# filter/csv/generic.py
class CSVFilterOperation(FilterOperation):
    """Backend-agnostic CSV filter."""
    
    def _apply_backend(self, data, backend, **kwargs):
        # Get format handler (automatically selects Spark/Pandas/Native handler)
        handler = self._get_format_handler("csv")
        
        # Read CSV - handler uses correct backend method
        df = handler.read(data, backend)
        
        # Apply filter criteria
        criteria = kwargs.get('criteria', {})
        filtered = self._filter(df, criteria, backend)
        
        return filtered
    
    def _filter(self, df, criteria, backend):
        """Filter using backend-agnostic approach."""
        # The backend abstraction provides a common interface
        # Format handlers already converted to backend's native format
        
        if backend.config.backend_type == BackendType.SPARK:
            # df is already a Spark DataFrame
            for key, value in criteria.items():
                df = df.filter(df[key] == value)
        elif backend.config.backend_type == BackendType.PANDAS:
            # df is already a Pandas DataFrame
            for key, value in criteria.items():
                df = df[df[key] == value]
        else:
            # Native: df is a list of dicts
            df = [row for row in df 
                  if all(row.get(k) == v for k, v in criteria.items())]
        
        return df
```

**Key Insight**: The format handler already converted the CSV to the backend's native format (Spark DataFrame, Pandas DataFrame, or list of dicts). The operation just needs to work with that format using the backend's API.

## Alternative: Backend Registry Pattern

We could also use a registry pattern to automatically select the best implementation:

```python
# filter/csv/__init__.py
from pyodibel.core.backend import BackendType

# Registry of implementations
_implementations = {
    BackendType.SPARK: SparkCSVFilterOperation,  # If exists
    BackendType.PANDAS: PandasCSVFilterOperation,  # If exists
    BackendType.NATIVE: CSVFilterOperation,  # Generic fallback
}

def get_csv_filter_operation(backend_type: BackendType):
    """Get the best CSV filter implementation for the backend."""
    return _implementations.get(backend_type, CSVFilterOperation)
```

## Final Recommendation

**Structure:**
```
filter/
├── base.py                    # Abstract base class
├── csv/
│   ├── __init__.py
│   ├── generic.py            # Backend-agnostic (works with all)
│   ├── spark.py              # Spark-specific (only if needed)
│   └── pandas.py             # Pandas-specific (only if needed)
```

**Strategy:**
1. **Default**: Implement `generic.py` that works with all backends
2. **Optimize**: Add `spark.py` or `pandas.py` only when profiling shows benefits
3. **Registry**: Use a registry pattern to automatically select the best implementation

This gives us:
- ✅ Simplicity: Most operations are backend-agnostic
- ✅ Flexibility: Can add backend-specific optimizations when needed
- ✅ Maintainability: Less code duplication
- ✅ Performance: Can optimize hot paths when needed
