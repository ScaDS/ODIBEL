# Operations Module Usability Analysis

## Current Proposal Structure

```
operations/
├── filter/
│   ├── csv/
│   │   ├── generic.py      # Backend-agnostic
│   │   ├── spark.py        # Spark-specific
│   │   └── pandas.py       # Pandas-specific
│   └── json/
└── transform/
```

## Usability Concerns

### 1. **Import Complexity**

**Problem**: Users need to know the exact path:
```python
from pyodibel.operations.filter.csv import CSVFilterOperation
from pyodibel.operations.filter.csv.spark import SparkCSVFilterOperation
```

**Better**: Simpler, more discoverable imports:
```python
from pyodibel.operations import FilterOperation
# Or even better:
from pyodibel.operations.filter import CSVFilter
```

### 2. **Backend Selection Confusion**

**Problem**: Users might not know whether to use `generic.py` or `spark.py`:
- When should they use generic vs backend-specific?
- What if they want to switch backends later?

**Better**: Auto-select based on backend config:
```python
# User doesn't need to choose - system auto-selects
filter_op = FilterOperation.create(
    config=OperationConfig(backend=BackendConfig(backend_type=BackendType.SPARK)),
    format="csv"
)
# Automatically uses SparkCSVFilterOperation if available, else generic
```

### 3. **Mental Model Complexity**

**Problem**: Users think in terms of:
- "I want to filter CSV data"
- Not: "I want to filter CSV data with Spark backend using generic implementation"

**Better**: Match user mental model:
```python
# Simple, intuitive API
filter_op = CSVFilter(criteria={"type": "Person"})
result = filter_op.apply(data="/path/to/data.csv", backend=spark_backend)
```

## Recommended Usability Improvements

### Option A: Factory Pattern with Auto-Selection (Recommended)

```python
# operations/filter/__init__.py
from pyodibel.core.backend import BackendType

class FilterOperationFactory:
    """Factory that auto-selects the best implementation."""
    
    @staticmethod
    def create(format: str, backend_type: BackendType = None):
        """Create filter operation, auto-selecting best implementation."""
        if format == "csv":
            if backend_type == BackendType.SPARK:
                # Try Spark-specific first
                try:
                    from .csv.spark import SparkCSVFilter
                    return SparkCSVFilter()
                except ImportError:
                    pass
            # Fall back to generic
            from .csv.generic import CSVFilter
            return CSVFilter()
        # ... other formats

# User-facing API
def create_filter(format: str, backend_type: BackendType = None):
    """Create a filter operation for the specified format."""
    return FilterOperationFactory.create(format, backend_type)
```

**Usage:**
```python
from pyodibel.operations.filter import create_filter

# Simple - auto-selects best implementation
filter_op = create_filter("csv", backend_type=BackendType.SPARK)
result = filter_op.apply(data, criteria={"type": "Person"})
```

### Option B: High-Level API with Backend Abstraction

```python
# operations/__init__.py
from pyodibel.core.operations import OperationConfig
from pyodibel.core.backend import BackendConfig, BackendType

def filter_data(
    data: Any,
    criteria: Dict[str, Any],
    format: str = None,
    backend: BackendConfig = None,
    **kwargs
) -> Any:
    """
    High-level filter function - most users will use this.
    
    Args:
        data: Input data (path or data object)
        criteria: Filter criteria
        format: Data format (auto-detected if None)
        backend: Backend config (defaults to Pandas)
        **kwargs: Additional parameters
    
    Returns:
        Filtered data
    """
    # Auto-detect format if not specified
    if format is None:
        format = _detect_format(data)
    
    # Default to Pandas if no backend specified
    if backend is None:
        backend = BackendConfig(backend_type=BackendType.PANDAS)
    
    # Create operation
    config = OperationConfig(
        name="filter",
        backend=backend,
        input_format=format,
        output_format=format
    )
    
    # Auto-select best implementation
    filter_op = FilterOperationFactory.create(format, backend.backend_type)
    filter_op.config = config
    
    return filter_op.apply(data, criteria=criteria, **kwargs)
```

**Usage:**
```python
from pyodibel.operations import filter_data

# Super simple - everything is automatic
result = filter_data(
    data="/path/to/data.csv",
    criteria={"type": "Person"},
    backend=BackendConfig(backend_type=BackendType.SPARK)
)
```

### Option C: Class-Based with Smart Defaults

```python
# operations/filter/csv/__init__.py
class CSVFilter(FilterOperation):
    """
    CSV filter operation - automatically selects best backend implementation.
    
    Usage:
        filter_op = CSVFilter(backend=spark_backend)
        result = filter_op.apply(data, criteria={"type": "Person"})
    """
    
    def __init__(self, backend: ExecutionBackend = None, **kwargs):
        # Auto-select implementation based on backend
        if backend and backend.config.backend_type == BackendType.SPARK:
            # Use Spark-optimized if available
            try:
                from .spark import SparkCSVFilter
                self.__class__ = SparkCSVFilter
                SparkCSVFilter.__init__(self, backend, **kwargs)
                return
            except ImportError:
                pass
        
        # Use generic implementation
        from .generic import GenericCSVFilter
        self.__class__ = GenericCSVFilter
        GenericCSVFilter.__init__(self, backend, **kwargs)
```

**Usage:**
```python
from pyodibel.operations.filter.csv import CSVFilter

# Simple - auto-selects implementation
filter_op = CSVFilter(backend=spark_backend)
result = filter_op.apply(data, criteria={"type": "Person"})
```

## Comparison: Usability vs Flexibility

| Approach | Usability | Flexibility | Complexity |
|----------|-----------|-------------|------------|
| **Direct imports** (`from .csv.spark import ...`) | ⭐⭐ Low | ⭐⭐⭐⭐⭐ High | ⭐⭐⭐⭐⭐ High |
| **Factory pattern** | ⭐⭐⭐⭐ Good | ⭐⭐⭐⭐ Good | ⭐⭐⭐ Medium |
| **High-level API** | ⭐⭐⭐⭐⭐ Excellent | ⭐⭐⭐ Medium | ⭐⭐ Low |
| **Class-based with auto-select** | ⭐⭐⭐⭐ Good | ⭐⭐⭐⭐ Good | ⭐⭐⭐ Medium |

## Recommendation: Hybrid Approach

**Provide both simple and advanced APIs:**

### 1. **High-Level API** (for 80% of users)
```python
from pyodibel.operations import filter_data, transform_data, join_data

# Simple, automatic
result = filter_data(
    data="/path/to/data.csv",
    criteria={"type": "Person"},
    backend=BackendConfig(backend_type=BackendType.SPARK)
)
```

### 2. **Class-Based API** (for advanced users)
```python
from pyodibel.operations.filter import CSVFilter

# More control, still simple
filter_op = CSVFilter(backend=spark_backend)
result = filter_op.apply(data, criteria={"type": "Person"})
```

### 3. **Direct Implementation** (for power users)
```python
from pyodibel.operations.filter.csv.spark import SparkCSVFilter

# Maximum control
filter_op = SparkCSVFilter(backend=spark_backend)
result = filter_op.apply(data, criteria={"type": "Person"})
```

## Key Usability Principles

### 1. **Progressive Disclosure**
- Start simple (high-level API)
- Allow advanced usage when needed
- Don't force users to understand internals

### 2. **Sensible Defaults**
- Auto-detect format when possible
- Default to Pandas (most common)
- Auto-select best implementation

### 3. **Clear Naming**
- `CSVFilter` not `CSVFilterOperation`
- `filter_data()` not `apply_filter_operation()`
- Match user mental model

### 4. **Error Messages**
```python
# Good error message
ValueError: "No CSV filter implementation available for Spark backend. 
Available backends: Pandas, Native. Install pyspark for Spark support."

# Bad error message
ImportError: cannot import name 'SparkCSVFilter'
```

## Final Structure Recommendation

```
operations/
├── __init__.py                    # High-level API (filter_data, transform_data, etc.)
├── format_handlers/              # Infrastructure
├── filter/
│   ├── __init__.py               # Factory and class-based API
│   ├── base.py
│   ├── csv/
│   │   ├── __init__.py          # CSVFilter class (auto-selects)
│   │   ├── generic.py
│   │   ├── spark.py
│   │   └── pandas.py
│   └── json/
└── transform/
```

**Benefits:**
- ✅ Simple API for common cases
- ✅ Flexible API for advanced cases
- ✅ Auto-selection reduces cognitive load
- ✅ Clear progression from simple to advanced
- ✅ Maintains all flexibility for power users

