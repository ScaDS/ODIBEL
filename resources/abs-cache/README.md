# DBpedia → RocksDB Pipeline

Two scripts for building a RocksDB key-value store that maps DBpedia entity names to their abstracts — either sourced directly from DBpedia NT dumps or fetched live from the Wikipedia API.

---

## Scripts

### `load_abstracts.py` — NT dump → RocksDB

Reads a DBpedia abstracts NT file and writes entity→abstract pairs into RocksDB. Supports two write modes:

- **`http`** — writes via a REST server using parallel threads. Faster for remote or containerized RocksDB setups.
- **`direct`** — writes directly into a local RocksDB instance via `rocksdbpy`. No server required, suitable for bulk imports on the same machine.

```
usage: load_abstracts.py [-h] --file FILE [--mode {http,direct}]
                         [--host HOST] [--port PORT] [--db DB]
                         [--batch BATCH] [--workers WORKERS] [--encoding ENCODING]

options:
  --file FILE          Path to NT file
  --mode {http,direct} Write mode: http (via REST server) or direct (rocksdbpy) (default: http)
  --host HOST          Server host, http mode only (default: localhost)
  --port PORT          Server port, http mode only (default: 5000)
  --db DB              Path to RocksDB directory, required for direct mode
  --batch BATCH        Batch size, http mode only (default: 500)
  --workers WORKERS    Parallel threads, http mode only (default: 8)
  --encoding ENCODING  File encoding (default: utf-8)
```

**Examples:**
```bash
# HTTP mode
python load_abstracts.py --file long_abstracts_en.ttl --mode http --host localhost --port 5000 --workers 16

# Direct mode
python load_abstracts.py --file long_abstracts_en.ttl --mode direct --db /data/rocksdb
```

---

### `get_from_wikipedia.py` — NT/HDFS → Wikipedia API → RocksDB

Reads DBpedia NT triples from HDFS (or a local path) using Spark, extracts unique entity names, and fetches their abstracts from the live Wikipedia API. Skips entities already present in the database. Supports the same two write modes as `load_abstracts.py`.

Use this script to fill gaps not covered by a NT dump, or when working with a Spark/HDFS environment.

```
usage: get_from_wikipedia.py [-h] --file FILE [--mode {http,direct}]
                             [--host HOST] [--port PORT] [--db DB]

options:
  --file FILE          HDFS or local path to NT file
  --mode {http,direct} Write mode: http (via REST server) or direct (rocksdbpy) (default: http)
  --host HOST          RocksDB server host, http mode only (default: localhost)
  --port PORT          RocksDB server port, http mode only (default: 5000)
  --db DB              Path to RocksDB directory, required for direct mode
```

**Examples:**
```bash
# HTTP mode
python get_from_wikipedia.py --file hdfs:///data/dbpedia.nt --mode http --host localhost --port 5000

# Direct mode
python get_from_wikipedia.py --file hdfs:///data/dbpedia.nt --mode direct --db /data/rocksdb
```

---

## Typical workflow

```
1. Bulk import from NT dump (fast):
   load_abstracts.py

2. Fill remaining gaps via Wikipedia API:
   get_from_wikipedia.py
```

`get_from_wikipedia.py` automatically skips entities already in the database, so it is safe to run after a bulk import to top up any missing entries.