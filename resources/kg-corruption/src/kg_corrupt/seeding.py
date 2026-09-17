"""Deterministic seed derivation. Never uses Python's salted hash()."""

from __future__ import annotations

import hashlib
import random


def derive_int_seed(
    global_seed: int,
    family: str,
    fraction: float,
    seed: int,
    *,
    nested: bool = False,
) -> int:
    """Stable 64-bit seed from run identity.

    Nested runs omit the fraction so increasing corruption levels share one
    RNG stream and remain monotonic for shuffle-then-prefix strategies.
    """
    frac_key = "nested" if nested else repr(fraction)
    payload = f"{global_seed}|{family}|{frac_key}|{seed}".encode("utf-8")
    digest = hashlib.sha256(payload).digest()
    return int.from_bytes(digest[:8], "big")


def make_rng(
    global_seed: int,
    family: str,
    fraction: float,
    seed: int,
    *,
    nested: bool = False,
) -> random.Random:
    return random.Random(
        derive_int_seed(global_seed, family, fraction, seed, nested=nested)
    )
