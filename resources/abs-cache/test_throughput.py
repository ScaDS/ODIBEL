import argparse
import logging
import random
import string
import time

import rocksdbpy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


def random_string(length):
    return "".join(random.choices(string.ascii_letters, k=length))


def bench(label, fn, entries):
    t = time.time()
    fn()
    elapsed = time.time() - t
    log.info(
        "%s: %d entries in %.3fs — %.0f ops/s  |  %.3f ms/op",
        label, entries, elapsed,
        entries / elapsed,
        elapsed / entries * 1000,
    )


def main():
    parser = argparse.ArgumentParser(description="RocksDB read/write throughput benchmark")
    parser.add_argument("--db",       required=True,           help="Path to RocksDB directory")
    parser.add_argument("--entries",  type=int, default=10000, help="Number of entries to benchmark (default: 10000)")
    parser.add_argument("--key-size", type=int, default=20,    help="Key length in bytes (default: 20)")
    parser.add_argument("--val-size", type=int, default=200,   help="Value length in bytes (default: 200)")
    args = parser.parse_args()

    opts = rocksdbpy.Option()
    opts.create_if_missing(True)

    try:
        db = rocksdbpy.open(args.db, opts)
        log.info("RocksDB opened: %s", args.db)
    except Exception as e:
        log.error("Failed to open RocksDB: %s", e)
        return

    log.info("Generating %d random entries (key=%db, val=%db)...", args.entries, args.key_size, args.val_size)
    keys = [random_string(args.key_size) for _ in range(args.entries)]
    values = [random_string(args.val_size) for _ in range(args.entries)]

    bench("WRITE", lambda: [db.set(k.encode(), v.encode()) for k, v in zip(keys, values)], args.entries)

    shuffled = keys[:]
    random.shuffle(shuffled)
    bench("READ ", lambda: [db.get(k.encode()) for k in shuffled], args.entries)

    log.info("Deleting benchmark entries...")
    for key in keys:
        db.delete(key.encode())
    log.info("Cleanup done")

    db.close()


if __name__ == "__main__":
    main()