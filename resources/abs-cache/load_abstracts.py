import argparse
import logging
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Event

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

LINE_RE = re.compile(
    r'^<(?P<uri>[^>]+)>\s+'
    r'<[^>]*ontology/abstract>\s+'
    r'"(?P<abstract>.+)"@en\s*\.',
    re.DOTALL,
)


def make_session():
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json"})
    adapter = requests.adapters.HTTPAdapter(
        max_retries=requests.adapters.Retry(total=3, backoff_factor=0.3)
    )
    s.mount("http://", adapter)
    return s


def send_batch(batch, base_url):
    session = make_session()
    ok_count = 0
    err_count = 0
    for key, value in batch:
        try:
            r = session.put(
                "%s/db/%s" % (base_url, requests.utils.quote(key, safe="")),
                json={"value": value},
                timeout=10,
            )
            if r.status_code in (200, 201):
                ok_count += 1
            else:
                log.warning("PUT failed key=%s status=%s", key, r.status_code)
                err_count += 1
        except Exception as e:
            log.warning("PUT exception key=%s error=%s", key, e)
            err_count += 1
    return ok_count, err_count


def parse_abstracts(filepath, encoding):
    with open(filepath, "r", encoding=encoding, errors="replace") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            m = LINE_RE.match(line)
            if not m:
                continue
            uri = m.group("uri")
            entity = uri.rsplit("/", 1)[-1]
            abstract = m.group("abstract").replace('\\"', '"')
            yield entity, abstract


def progress_reporter(ok_counter, err_counter, stop, start):
    while not stop.is_set():
        elapsed = time.time() - start
        rate = ok_counter[0] / elapsed if elapsed > 0 else 0
        print(
            "\r  -> %8d entries  |  %6.0f/s  |  errors: %d   " % (
                ok_counter[0], rate, err_counter[0]
            ),
            end="",
            flush=True,
        )
        time.sleep(1)


def main():
    parser = argparse.ArgumentParser(description="TTL -> RocksDB Importer")
    parser.add_argument("--file",     required=True,         help="Path to TTL file")
    parser.add_argument("--host",     default="localhost",    help="Server host")
    parser.add_argument("--port",     type=int, default=5000, help="Server port")
    parser.add_argument("--batch",    type=int, default=500,  help="Batch size")
    parser.add_argument("--workers",  type=int, default=8,    help="Parallel threads")
    parser.add_argument("--encoding", default="utf-8",        help="File encoding")
    args = parser.parse_args()

    base_url = "http://%s:%d" % (args.host, args.port)

    try:
        r = requests.get("%s/health" % base_url, timeout=5)
        r.raise_for_status()
        log.info("Server reachable: %s", r.json())
    except Exception as e:
        log.error("Server not reachable: %s", e)
        sys.exit(1)

    log.info("Starting import: %s  (batch=%d, workers=%d)", args.file, args.batch, args.workers)
    start = time.time()

    total_ok  = [0]
    total_err = [0]

    stop_event = Event()
    reporter = threading.Thread(
        target=progress_reporter,
        args=(total_ok, total_err, stop_event, start),
        daemon=True,
    )
    reporter.start()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = []
        batch = []

        for entity, abstract in parse_abstracts(args.file, args.encoding):
            batch.append((entity, abstract))
            if len(batch) >= args.batch:
                futures.append(pool.submit(send_batch, batch, base_url))
                batch = []

            done = [f for f in futures if f.done()]
            for f in done:
                ok_n, err_n = f.result()
                total_ok[0]  += ok_n
                total_err[0] += err_n
            futures = [f for f in futures if not f.done()]

        if batch:
            futures.append(pool.submit(send_batch, batch, base_url))

        for f in as_completed(futures):
            ok_n, err_n = f.result()
            total_ok[0]  += ok_n
            total_err[0] += err_n

    stop_event.set()
    elapsed = time.time() - start
    print()
    log.info(
        "Done! %d entries imported, %d errors - %.1f seconds (%.0f/s)",
        total_ok[0], total_err[0], elapsed,
        total_ok[0] / elapsed if elapsed > 0 else 0,
    )


if __name__ == "__main__":
    main()
