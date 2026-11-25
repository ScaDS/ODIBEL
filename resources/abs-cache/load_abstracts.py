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



def make_session():
    s = requests.Session()
    s.headers.update({"Content-Type": "application/json"})
    adapter = requests.adapters.HTTPAdapter(
        max_retries=requests.adapters.Retry(total=3, backoff_factor=0.3)
    )
    s.mount("http://", adapter)
    return s


def send_batch_http(batch, base_url):
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


def run_http(args, base_url, start):
    total_ok = [0]
    total_err = [0]

    stop_event = Event()
    threading.Thread(target=progress_reporter, args=(total_ok, total_err, stop_event, start), daemon=True).start()

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = []
        batch = []

        for entity, abstract in parse_abstracts(args.file, args.encoding):
            batch.append((entity, abstract))
            if len(batch) >= args.batch:
                futures.append(pool.submit(send_batch_http, batch, base_url))
                batch = []

            done = [f for f in futures if f.done()]
            for f in done:
                ok_n, err_n = f.result()
                total_ok[0] += ok_n
                total_err[0] += err_n
            futures = [f for f in futures if not f.done()]

        if batch:
            futures.append(pool.submit(send_batch_http, batch, base_url))

        for f in as_completed(futures):
            ok_n, err_n = f.result()
            total_ok[0] += ok_n
            total_err[0] += err_n

    stop_event.set()
    return total_ok[0], total_err[0]



def run_direct(args, start):
    import rocksdbpy

    opts = rocksdbpy.Option()
    opts.create_if_missing(True)

    try:
        db = rocksdbpy.open(args.db, opts)
        log.info("RocksDB opened: %s", args.db)
    except Exception as e:
        log.error("Failed to open RocksDB: %s", e)
        sys.exit(1)

    total_ok = [0]
    total_err = [0]

    stop_event = Event()
    threading.Thread(target=progress_reporter, args=(total_ok, total_err, stop_event, start), daemon=True).start()

    for entity, abstract in parse_abstracts(args.file, args.encoding):
        try:
            db.set(entity.encode(), abstract.encode())
            total_ok[0] += 1
        except Exception as e:
            log.warning("RocksDB set failed key=%s error=%s", entity, e)
            total_err[0] += 1

    stop_event.set()
    db.close()
    return total_ok[0], total_err[0]



def main():
    parser = argparse.ArgumentParser(description="TTL -> RocksDB importer")
    parser.add_argument("--file",     required=True,               help="Path to TTL file")
    parser.add_argument("--mode",     choices=["http", "direct"],
                        default="http",                            help="Write mode: http (via REST server) or direct (rocksdbpy) (default: http)")
    parser.add_argument("--host",     default="localhost",         help="Server host, http mode only (default: localhost)")
    parser.add_argument("--port",     type=int, default=5000,     help="Server port, http mode only (default: 5000)")
    parser.add_argument("--db",       default=None,               help="Path to RocksDB directory, required for direct mode")
    parser.add_argument("--batch",    type=int, default=500,      help="Batch size, http mode only (default: 500)")
    parser.add_argument("--workers",  type=int, default=8,        help="Parallel threads, http mode only (default: 8)")
    parser.add_argument("--encoding", default="utf-8",            help="File encoding (default: utf-8)")
    args = parser.parse_args()

    if args.mode == "direct" and not args.db:
        log.error("--db is required when using --mode direct")
        sys.exit(1)

    start = time.time()

    if args.mode == "http":
        base_url = "http://%s:%d" % (args.host, args.port)
        try:
            r = requests.get("%s/health" % base_url, timeout=5)
            r.raise_for_status()
            log.info("Server reachable: %s", r.json())
        except Exception as e:
            log.error("Server not reachable: %s", e)
            sys.exit(1)
        log.info("Starting import: %s  (batch=%d, workers=%d)", args.file, args.batch, args.workers)
        total_ok, total_err = run_http(args, base_url, start)
    else:
        log.info("Starting import: %s  (direct)", args.file)
        total_ok, total_err = run_direct(args, start)

    elapsed = time.time() - start
    print()
    log.info(
        "Done! %d entries imported, %d errors - %.1f seconds (%.0f/s)",
        total_ok, total_err, elapsed,
        total_ok / elapsed if elapsed > 0 else 0,
    )


if __name__ == "__main__":
    main()