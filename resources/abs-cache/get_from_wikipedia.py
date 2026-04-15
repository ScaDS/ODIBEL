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
MIN_INTERVAL = 1.0

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

    s.headers.update({
        "User-Agent": "WikipediaEnricher/1.0 (theo.hahn@uni-leipzig.de)"
    })

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


def fetch_wikipedia_abstracts(session, entities):
    try:
        titles = "|".join(requests.utils.quote(e, safe="") for e in entities)

        url = WIKIPEDIA_API % titles

        r = session.get(url, timeout=10)

        if r.status_code != 200:
            log.warning("HTTP %s for batch %s", r.status_code, entities[:3])
            return {}

        data = r.json()
        pages = data.get("query", {}).get("pages", {})

        results = {}

        for page in pages.values():
            title = page.get("title")
            extract = page.get("extract")

            if title and extract:
                results[title] = extract

        return results

    except Exception as e:
        log.error("Batch exception: %s", e)
        return {}


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

def process_partition(iterator):
    session = make_session()
    last_request = 0.0

    counters = {"written": 0, "skip": 0, "not_found": 0, "error": 0}
    failed_entities = []

    for batch in batch_iterator(iterator, batch_size=20):

        batch = [e for e in batch if not already_in_db(session, e, base_url)]

        counters["skip"] += (len(batch) - len(batch))

        if not batch:
            continue

        elapsed = time.time() - last_request
        if elapsed < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - elapsed)

        last_request = time.time()

        abstracts = fetch_wikipedia_abstracts(session, batch)

        for entity in batch:
            abstract = abstracts.get(entity) or abstracts.get(entity.replace("_", " "))

            if not abstract:
                counters["not_found"] += 1
                failed_entities.append(entity)
                continue

            if write_to_db(session, entity, abstract, base_url):
                counters["written"] += 1
            else:
                counters["error"] += 1
                failed_entities.append(entity)

    yield {"counters": counters, "failed": failed_entities}

def main():
    parser = argparse.ArgumentParser(description="NT/HDFS -> Wikipedia -> RocksDB enricher")
    parser.add_argument("--file",    required=True,          help="HDFS or local path to NT file")
    parser.add_argument("--host",    default="localhost",     help="RocksDB server host")
    parser.add_argument("--port",    type=int, default=5000,  help="RocksDB server port")
    args = parser.parse_args()

    start = time.time()

    try:
        r = requests.get("%s/health" % base_url, timeout=5)
        r.raise_for_status()
        log.info("RocksDB server reachable: %s", r.json())
    except Exception as e:
        log.error("RocksDB server not reachable: %s", e)
        sys.exit(1)

    spark = (SparkSession.builder
        .appName("NT Wikipedia Enricher")
        .config("spark.driver.memory", "32g")
        .config("spark.executor.memory", "16g")
        .getOrCreate()
             )

    log.info("Extracting unique entities from: %s", args.file)

    rdd = (
        spark.sparkContext
        .textFile(args.file)
        .mapPartitions(parse_subject)
        .filter(lambda x: x is not None and "__" not in x)
        .distinct()
        .coalesce(2)
    )

    results = rdd.mapPartitions(process_partition).collect()

    counters = Counter()
    failed_all = []
    for r in results:
        counters.update(r["counters"])
        failed_all.extend(r["failed"])

    elapsed = time.time() - start
    if failed_all:
        spark.sparkContext.parallelize(failed_all).saveAsTextFile("failed_uris")
        log.info("Wrote %d failed URIs to failed_uris.txt", len(failed_all))

    spark.stop()

    log.info(
        "Done! written=%d skipped=%d not_found=%d errors=%d - %.1f seconds",
        counters["written"], counters["skip"],
        counters["not_found"], counters["error"], elapsed,
    )


if __name__ == "__main__":
    main()
