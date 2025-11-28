# Ingest Tests

Tests for data ingestion functionality.

## Running Tests

The Python virtual environment is located at `../.venv` relative to the project root.

### Prerequisites

For Spark tests, you need:
- **PySpark**: `pip install pyspark`
- **Java 17+**: If using SDKMAN, run `sdk use java 17.0.15-tem` before running tests

### Running Tests

```bash
# Activate the virtual environment
source ../.venv/bin/activate

# If using SDKMAN, set Java version first
sdk use java 17.0.15-tem

# Run all source tests
pytest src/pyodibel/test/source/

# Run only Wikidata dump tests
pytest src/pyodibel/test/source/test_wikidata_dump.py

# Run tests with verbose output
pytest -v src/pyodibel/test/source/test_wikidata_dump.py

# Run only fast tests (skip slow Spark tests)
pytest src/pyodibel/test/source/test_wikidata_dump.py

# Run slow tests (requires PySpark and Java)
pytest --run-slow src/pyodibel/test/source/test_wikidata_dump.py
```

### Troubleshooting

**Java not found errors:**
- If using SDKMAN: `sdk use java 17.0.15-tem` before running tests
- Verify Java: `java -version` should show Java 17 or higher
- Set JAVA_HOME if needed: `export JAVA_HOME=$(sdk home java 17.0.15-tem)`

**PySpark import errors:**
- Install PySpark: `pip install pyspark`
- Verify installation: `python -c "import pyspark; print(pyspark.__version__)"`

## Test Data

The Wikidata dump test uses the file:
- `data/wikidata-20251103-all.json-1-100.json`

This file should contain up to 100 Wikidata entities in JSON format.

## Requirements

For full test coverage, install:
- `pyspark` - Required for Spark backend tests
- `pytest` - Test framework

Tests will automatically skip if PySpark is not installed.

