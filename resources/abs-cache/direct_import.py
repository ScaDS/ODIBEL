import argparse
import logging
import re
import sys
import time

import rocksdbpy
from rocksdbpy import Option

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


def main():
    parser = argparse.ArgumentParser(description="TTL -> RocksDB direct writer")
    parser.add_argument("--file",     required=True,          help="Path to TTL file")
    parser.add_argument("--db",       required=True,          help="Path to RocksDB directory (host volume)")
    parser.add_argument("--encoding", default="utf-8",        help="File encoding")
    parser.add_argument("--batch",    type=int, default=1000, help="Log progress every N entries")
    args = parser.parse_args()

    opts = Option()
    opts.create_if_missing(True)

    try:
        db = rocksdbpy.open(args.db, opts)
        log.info("RocksDB opened: %s", args.db)
    except Exception as e:
        log.error("Failed to open RocksDB: %s", e)
        sys.exit(1)

    start = time.time()
    count = 0

    for entity, abstract in parse_abstracts(args.file, args.encoding):
        db.set(entity.encode(), abstract.encode())
        count += 1
        if count % args.batch == 0:
            elapsed = time.time() - start
            rate = count / elapsed if elapsed > 0 else 0
            log.info("progress: %d entries written  |  %.0f/s", count, rate)

    db.close()
    elapsed = time.time() - start
    log.info(
        "Done! %d entries written - %.1f seconds (%.0f/s)",
        count, elapsed, count / elapsed if elapsed > 0 else 0,
    )


if __name__ == "__main__":
    main()
