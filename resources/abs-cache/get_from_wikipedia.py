import argparse
import logging
import sys
import time
from collections import Counter

import requests
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

WIKIPEDIA_API = "https://en.wikipedia.org/w/api.php?format=json&action=query&prop=extracts&exintro=&explaintext=&titles=%s"
MIN_INTERVAL = 1.0 / 10

base_url = "http://%s:%d" % ("localhost", 5000)


def batch_iterator(iterator, batch_size=20):
    batch = []
    for item in iterator:
        if item:
            batch.append(item)
            if len(batch) >= batch_size:
                yield batch
                batch = []
    if batch:
        yield batch


def parse_subject(iterator):
    for line in iterator:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if not line.startswith("<http://dbpedia.org/resource/"):
            continue
        end = line.index(">", 1)
        uri = line[1:end]
        yield uri.rsplit("/", 1)[-1]


def make_session():
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=requests.adapters.Retry(
            total=3,
            backoff_factor=1.0,
            status_forcelist=[429, 500, 502, 503, 504],
        )
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    s.headers.update({"User-Agent": "WikipediaEnricher/1.0 (theo.hahn@uni-leipzig.de)"})
    return s


def already_in_db_http(session, entity):
    try:
        r = session.get(
            "%s/db/%s" % (base_url, requests.utils.quote(entity, safe="")),
            timeout=5,
        )
        return r.status_code == 200
    except Exception as e:
        log.error("Http exception: %s", e)
        return False


def already_in_db_direct(db, entity):
    try:
        return db.get(entity.encode()) is not None
    except Exception as e:
        log.error("Db exception: %s", e)
        return False


def fetch_wikipedia_abstracts(session, entities):
    try:
        titles = "|".join(requests.utils.quote(e, safe="") for e in entities)
        r = session.get(WIKIPEDIA_API % titles, timeout=10)

        if r.status_code != 200:
            log.warning("HTTP %s for batch starting with: %s", r.status_code, entities[:3])
            return {}

        pages = r.json().get("query", {}).get("pages", {})
        return {
            page["title"]: page["extract"]
            for page in pages.values()
            if page.get("title") and page.get("extract")
        }
    except Exception as e:
        log.error("Batch fetch exception: %s", e)
        return {}


def write_to_db_http(session, entity, abstract):
    try:
        r = session.put(
            "%s/db/%s" % (base_url, requests.utils.quote(entity, safe="")),
            json={"value": abstract},
            timeout=10,
        )
        return r.status_code in (200, 201)
    except Exception:
        return False


def write_to_db_direct(db, entity, abstract):
    try:
        db.set(entity.encode(), abstract.encode())
        return True
    except Exception:
        return False


def _run_partition(iterator, already_in_db_fn, write_fn):
    session = make_session()
    last_request = 0.0
    last_logged_skip = 0

    counters = {"written": 0, "skip": 0, "not_found": 0, "error": 0}
    failed_entities = []
    batch_count = 0
    entity_count = 0

    for batch in batch_iterator(iterator, batch_size=20):

        original_size = len(batch)
        batch = [e for e in batch if not already_in_db_fn(e)]
        counters["skip"] += original_size - len(batch)

        if counters["skip"] - last_logged_skip >= 10000:
            log.info("DB lookup progress: %d entities skipped (already in db)", counters["skip"])
            last_logged_skip = counters["skip"]

        if not batch:
            continue

        elapsed = time.time() - last_request
        if elapsed < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed)
        last_request = time.time()

        batch_count += 1
        entity_count += len(batch)

        if batch_count % 10 == 0:
            log.info(
                "Wikipedia: %d batches sent, %d entities fetched "
                "(written=%d skipped=%d not_found=%d errors=%d)",
                batch_count, entity_count,
                counters["written"], counters["skip"],
                counters["not_found"], counters["error"],
            )

        abstracts = fetch_wikipedia_abstracts(session, batch)

        for entity in batch:
            abstract = abstracts.get(entity) or abstracts.get(entity.replace("_", " "))
            if not abstract:
                counters["not_found"] += 1
                failed_entities.append(entity)
                continue
            if write_fn(entity, abstract):
                counters["written"] += 1
            else:
                counters["error"] += 1
                failed_entities.append(entity)
    log.info(
        "Partition done: %d batches, %d entities fetched "
        "(written=%d skipped=%d not_found=%d errors=%d)",
        batch_count, entity_count,
        counters["written"], counters["skip"],
        counters["not_found"], counters["error"],
    )
    return {"counters": counters, "failed": failed_entities}


def process_partition_http(iterator):
    session = make_session()
    yield _run_partition(
        iterator,
        already_in_db_fn=lambda e: already_in_db_http(session, e),
        write_fn=lambda e, a: write_to_db_http(session, e, a),
    )


def make_process_partition_direct(db_path):
    def process_partition_direct(iterator):
        import rocksdbpy

        try:
            opts = rocksdbpy.Option()
            opts.create_if_missing(True)
            db = rocksdbpy.open(db_path, opts)
        except Exception as e:
            log.error("Failed to open RocksDB in executor: %s", e)
            yield {"counters": {"written": 0, "skip": 0, "not_found": 0, "error": 0}, "failed": []}
            return

        try:
            yield _run_partition(
                iterator,
                already_in_db_fn=lambda e: already_in_db_direct(db, e),
                write_fn=lambda e, a: write_to_db_direct(db, e, a),
            )
        finally:
            db.close()

    return process_partition_direct


def main():
    parser = argparse.ArgumentParser(description="NT/HDFS -> Wikipedia -> RocksDB enricher")
    parser.add_argument("--file", required=True,             help="HDFS or local path to NT file")
    parser.add_argument("--host", default="localhost",       help="RocksDB server host (http mode)")
    parser.add_argument("--port", type=int, default=5000,   help="RocksDB server port (http mode)")
    parser.add_argument("--mode", choices=["http", "direct"], default="http",
                        help="Write mode: 'http' (via REST server) or 'direct' (rocksdbpy)")
    parser.add_argument("--db",   default=None,              help="Path to RocksDB directory (direct mode)")
    args = parser.parse_args()

    if args.mode == "direct" and not args.db:
        log.error("--db is required when using --mode direct")
        sys.exit(1)

    global base_url
    base_url = "http://%s:%d" % (args.host, args.port)

    start = time.time()

    if args.mode == "http":
        try:
            r = requests.get("%s/health" % base_url, timeout=5)
            r.raise_for_status()
            log.info("RocksDB server reachable: %s", r.json())
        except Exception as e:
            log.error("RocksDB server not reachable: %s", e)
            sys.exit(1)
    else:
        try:
            import rocksdbpy
            opts = rocksdbpy.Option()
            opts.create_if_missing(True)
            db = rocksdbpy.open(args.db, opts)
            log.info("RocksDB opened (validation): %s", args.db)
            db.close()
        except Exception as e:
            log.error("Failed to open RocksDB: %s", e)
            sys.exit(1)

    spark = (
        SparkSession.builder
        .appName("NT Wikipedia Enricher")
        .config("spark.driver.memory", "32g")
        .config("spark.executor.memory", "16g")
        .getOrCreate()
    )

    rdd = (
        spark.sparkContext
        .textFile(args.file)
        .mapPartitions(parse_subject)
        .filter(lambda x: x is not None and "__" not in x)
        .distinct(1)
    )

    if args.mode == "http":
        results = rdd.mapPartitions(process_partition_http).collect()
    else:
        results = rdd.mapPartitions(make_process_partition_direct(args.db)).collect()

    counters = Counter()
    failed_all = []
    for r in results:
        counters.update(r["counters"])
        failed_all.extend(r["failed"])

    spark.stop()

    log.info(
        "Done! written=%d skipped=%d not_found=%d errors=%d — %.1fs total",
        counters["written"], counters["skip"],
        counters["not_found"], counters["error"],
        time.time() - start,
    )


if __name__ == "__main__":
    main()