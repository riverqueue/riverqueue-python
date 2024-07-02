"""
FNV is the Fowler–Noll–Vo hash function, a simple hash that's very easy to
implement, and hash the perfect characteristics for use with the 64 bits of
available space in a PG advisory lock.

I'm implemented it myself so that the River package can stay dependency free
(and because it's quite easy to do).
"""

from typing import Dict, Literal


def fnv1_hash(data: bytes, size: Literal[32] | Literal[64]) -> int:
    """
    Hashes data as a 32-bit or 64-bit FNV hash and returns the result. Data
    should be bytes rather than a string, so encode a string with something like
    `input_str.encode("utf-8")` or `b"string as bytes"`.
    """

    assert isinstance(data, bytes)

    hash = __OFFSET_BASIS[size]
    mask = 2**size - 1  # creates a mask of 1s of `size` bits long like 0xffffffff
    prime = __PRIME[size]

    for byte in data:
        hash *= prime
        hash &= mask  # take lower N bits of multiplication product
        hash ^= byte

    return hash


__OFFSET_BASIS: Dict[Literal[32] | Literal[64], int] = {
    32: 0x811C9DC5,
    64: 0xCBF29CE484222325,
}

__PRIME: Dict[Literal[32] | Literal[64], int] = {
    32: 0x01000193,
    64: 0x00000100000001B3,
}
