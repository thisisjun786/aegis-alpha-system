"""KIND listed-company downloads (유가증권 ``kind-kospi``, 코스닥 ``kind-kosdaq``).

KIND serves its listed-company list as an EUC-KR HTML table through the same form post
its Excel download button sends. A list is named only by its market: asking again on
another day is another attempt of the same request. The receipt this module builds is
the shape ``storage.kr_identity.kind_unit`` reads (request ``source_id``, HTTP status,
response size and SHA-256, retrieval instant), so a collected list commits as a
``kind-listings`` content source that ``kind.listings@1`` and ``kind.industry@1``/``@2``
read.
"""

from __future__ import annotations

import hashlib
import urllib.parse
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

from aegis_alpha.data.opendart import HttpAnswer, canonical, instant

if TYPE_CHECKING:
    from datetime import datetime

    from aegis_alpha.data.opendart import Clock, Transport

RECEIPT_FORMAT: Final = "aas-kind-receipt-v1"
URL: Final = "https://kind.krx.co.kr/corpgeneral/corpList.do"
REFERER: Final = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage"
MARKETS: Final = {"kind-kospi": "stockMkt", "kind-kosdaq": "kosdaqMkt"}
LISTS: Final = tuple(sorted(MARKETS))


def form(list_id: str) -> bytes:
    """The form body KIND's download button posts for one market."""
    if list_id not in MARKETS:
        raise ValueError(f"unknown KIND list {list_id!r}")
    return urllib.parse.urlencode(
        {
            "currentPageSize": "3000",
            "marketType": MARKETS[list_id],
            "method": "download",
            "pageIndex": "1",
            "searchType": "13",
        }
    ).encode()


@dataclass(frozen=True, slots=True)
class KindResponse:
    list_id: str
    answer: HttpAnswer
    requested_at: datetime
    retrieved_at: datetime

    def receipt(self) -> bytes:
        """The canonical receipt naming the response bytes by size and SHA-256."""
        body = self.answer.body
        return canonical(
            {
                "schema_version": RECEIPT_FORMAT,
                "request": {"source_id": self.list_id},
                "method": "POST",
                "source_uri": URL,
                "form": form(self.list_id).decode(),
                "status": self.answer.status,
                "headers": [list(pair) for pair in self.answer.headers],
                "requested_at_utc": instant(self.requested_at),
                "retrieved_at_utc": instant(self.retrieved_at),
                "raw": {"content_sha256": hashlib.sha256(body).hexdigest(),
                        "size_bytes": len(body)},
            }
        ).encode()  # fmt: skip


def fetch(list_id: str, transport: Transport, clock: Clock) -> KindResponse:
    """Post one market's download form and keep the answer with its instants."""
    headers = {
        "Accept-Encoding": "identity",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": REFERER,
        "User-Agent": "AAS KR listings collector",
    }
    started = clock()
    answer = transport("POST", URL, form(list_id), headers)
    return KindResponse(list_id, answer, started, clock())
