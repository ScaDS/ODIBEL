# Experiments

## Dataset Status

- Total abstracts loaded: 3,544,364
- Database size: 3.2GB
---

## Domain Coverage

### Processed Domains

- biology 
  - total=3,158,562 written=? not_found=? — ?s
- creativeworks 
  - total=15,017,960 written=? not_found=? — ?s
- events 
  - total=2,650,918 written=134,788 not_found=4055 — 7045.8s

### Missing Domains

- geography 
  - total=871,726
- infrastructure 
  - total=2,416,216
- organizations 
  - total=1,726,942
- people 
  - total=3,165,944
- politics 
  - total=85,922
- science 
  - total=78,610
- sports 
  - total=781,757
- technology 
  - total=82,474


---

## Performance

### RocksDB

- Read throughput (ops/sec): 80,033
- Write throughput (ops/sec): 216,715

### RocksDB HTTP API

- Throughput (ops/sec): 40

### Wikipedia Fetch

- Requests per second: 10

---

## System Setup

### Database

The RocksDB database is located at:

```
/local/d1/docker/rocksdb/database
````

#### HTTP Mode

To run RocksDB with the HTTP API:

```bash
cd /local/d1/docker/rocksdb
make docker_build
make docker_run
````

#### Direct Mode

Alternatively, you can access the database directly (without the HTTP layer) by using:

```
/local/d1/docker/rocksdb/database
```

as the database path.

