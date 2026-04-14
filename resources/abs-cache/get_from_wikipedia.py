import argparse
import logging
import sys
import time

import requests
from pyspark.sql import SparkSession

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

WIKIPEDIA_API = "https://en.wikipedia.org/api/rest_v1/page/summary/%s"
MIN_INTERVAL = 1.0 / 10


def parse_subject(line):
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if not line.startswith("<http://dbpedia.org/resource/"):
        return None
    end = line.index(">", 1)
    uri = line[1:end]
    return uri.rsplit("/", 1)[-1]


def make_session():
    s = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        max_retries=requests.adapters.Retry(total=3, backoff_factor=1.0)
    )
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def already_in_db(session, entity, base_url):
    try:
        r = session.get(
            "%s/db/%s" % (base_url, requests.utils.quote(entity, safe="")),
            timeout=5,
        )
        return r.status_code == 200
    except Exception:
        return False


def fetch_wikipedia_abstract(session, entity):
    try:
        r = session.get(
            WIKIPEDIA_API % requests.utils.quote(entity, safe=""),
            timeout=10,
        )
        if r.status_code == 200:
            return r.json().get("extract", "") or None
        return None
    except Exception:
        return None


def write_to_db(session, entity, abstract, base_url):
    try:
        r = session.put(
            "%s/db/%s" % (base_url, requests.utils.quote(entity, safe="")),
            json={"value": abstract},
            timeout=10,
        )
        return r.status_code in (200, 201)
    except Exception:
        return False


def main():
    parser = argparse.ArgumentParser(description="NT/HDFS -> Wikipedia -> RocksDB enricher")
    parser.add_argument("--file",    required=True,          help="HDFS or local path to NT file")
    parser.add_argument("--host",    default="localhost",     help="RocksDB server host")
    parser.add_argument("--port",    type=int, default=5000,  help="RocksDB server port")
    args = parser.parse_args()

    base_url = "http://%s:%d" % (args.host, args.port)

    try:
        r = requests.get("%s/health" % base_url, timeout=5)
        r.raise_for_status()
        log.info("RocksDB server reachable: %s", r.json())
    except Exception as e:
        log.error("RocksDB server not reachable: %s", e)
        sys.exit(1)

    spark = SparkSession.builder \
        .appName("NT Wikipedia Enricher") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    log.info("Extracting unique entities from: %s", args.file)

    entity_iterator = (
        spark.sparkContext
        .textFile(args.file)
        .mapPartitions(parse_subject)
        .filter(lambda x: x is not None)
        .distinct()
        .toLocalIterator()
    )

    session = make_session()
    counters = {"written": 0, "skip": 0, "not_found": 0, "error": 0}
    start = time.time()
    last_request = 0.0

    for entity in entity_iterator:
        if already_in_db(session, entity, base_url):
            counters["skip"] += 1
            continue

        # rate limit: ensure at least MIN_INTERVAL between wikipedia requests
        elapsed_since_last = time.time() - last_request
        if elapsed_since_last < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed_since_last)

        last_request = time.time()
        abstract = fetch_wikipedia_abstract(session, entity)

        if not abstract:
            counters["not_found"] += 1
            continue

        if write_to_db(session, entity, abstract, base_url):
            counters["written"] += 1
        else:
            counters["error"] += 1

        total = sum(counters.values())
        if total % 100 == 0:
            elapsed = time.time() - start
            rate = total / elapsed if elapsed > 0 else 0
            log.info(
                "progress: processed=%d written=%d skipped=%d not_found=%d errors=%d rate=%.1f/s",
                total, counters["written"], counters["skip"],
                counters["not_found"], counters["error"], rate,
            )

    spark.stop()
    elapsed = time.time() - start
    log.info(
        "Done! written=%d skipped=%d not_found=%d errors=%d - %.1f seconds",
        counters["written"], counters["skip"],
        counters["not_found"], counters["error"], elapsed,
    )


if __name__ == "__main__":
    main()
