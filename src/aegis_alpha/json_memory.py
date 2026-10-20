"""Bound JSON materialization from its shape before decoding it.

This is an allocation estimate, not a parser or an authentication step. Callers still
validate and hash their documents. Scanning uses bounded chunks and retains no tokens.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import BinaryIO

# A dict/list header, a string header, and a member/element slot plus scalar.
# Deliberately above CPython's sizes, including dict/list growth and numeric objects.
_CONTAINER = 256
_STRING = 128
_SLOT = 64
_CHUNK = 64 * 1024
_SYNTAX = re.compile(rb'["\\{}\[\],:]')


def json_memory(  # noqa: C901 -- one streaming quote/escape state across chunks
    chunks: Iterable[bytes], *, copies: int = 1
) -> int:
    """Estimate bytes plus decoder text and ``copies`` of the decoded graph.

    Count punctuation only outside strings; an escaped quote or a chunk boundary must
    not turn string contents into structure. Charge four bytes per encoded byte for
    both Unicode decoder text and graph strings (even ASCII, numbers and whitespace).
    Container/member charges cover short, densely packed values that a byte multiplier
    underestimates. The extra graph copies cover the caller's simultaneous projections.
    """
    if type(copies) is not int or copies < 1:
        raise ValueError("JSON graph copies must be a positive integer")
    size = 0
    structure = _CONTAINER
    quoted = False
    escaped_at = -1
    for chunk in chunks:
        for match in _SYNTAX.finditer(chunk):
            position = size + match.start()
            token = match[0]
            if quoted:
                if position == escaped_at:
                    continue
                if token == b"\\":
                    escaped_at = position + 1
                elif token == b'"':
                    quoted = False
            elif token == b'"':
                quoted = True
                structure += _STRING
            elif token in (b"{", b"["):
                structure += _CONTAINER
            elif token in (b",", b":"):
                structure += _SLOT
        size += len(chunk)
    # Raw bytes + decoder text, then graph strings and structure for every live copy.
    return _CHUNK + 5 * size + copies * (4 * size + structure)


def json_bytes_memory(raw: bytes, *, copies: int = 1) -> int:
    return json_memory((raw,), copies=copies)


def json_file_memory(handle: BinaryIO, *, copies: int = 1) -> int:
    """Scan from the current position; leave the descriptor at EOF."""
    return json_memory(iter(lambda: handle.read(_CHUNK), b""), copies=copies)
