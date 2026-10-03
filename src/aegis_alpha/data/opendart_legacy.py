"""Replay a legacy OpenDART collection root (``attempts.jsonl``) into cohort knowledge.

The legacy collector kept one ledger line per call (``{"request", "timestamp"}``), a
receipt per request at ``receipts/<fingerprint>.json`` (with an optional
``<fingerprint>.validation-v1.json`` that re-validated a ``FAILED`` receipt) and the
response at ``raw/<fingerprint>.raw``. Its fingerprint hashed the request document with
the observation date of its fixed cohort, so every request it ever planned had already
been answered once and it planned nothing new.

This replay reads that root without writing: each request the ledger names becomes an
``Observation`` of the request without its observation date, with the receipt's outcome
and retrieval instant (a request with no receipt is ``FAILED`` at its ledger instant),
and the newest completed corp code answer gives the listed companies. The rolling
planner then says what the legacy cohort never asked.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Final, cast

from aegis_alpha.data.descriptor_tree import DescriptorTree, DescriptorTreeError
from aegis_alpha.data.opendart import (
    COMPLETED,
    CORP_CODES,
    FAILED,
    OUTCOMES,
    DartRequest,
    listed_corp_codes,
)
from aegis_alpha.data.opendart_cohort import Knowledge, Observation
from aegis_alpha.data.serialization import canonical_json_bytes

if TYPE_CHECKING:
    from pathlib import Path

MAX_LEDGER_BYTES: Final = 256 * 1024 * 1024
MAX_RECEIPT_BYTES: Final = 256 * 1024
MAX_RAW_BYTES: Final = 64 * 1024 * 1024


@dataclass(slots=True)
class LegacyReplay:
    knowledge: Knowledge = field(default_factory=Knowledge)
    ledger_lines: int = 0
    requests: int = 0
    receipts_missing: int = 0
    revalidated: int = 0
    last_attempt: str | None = None
    outcomes: dict[str, int] = field(default_factory=dict)
    observation_dates: dict[str, int] = field(default_factory=dict)

    def report(self) -> dict[str, object]:
        return {
            "ledger_lines": self.ledger_lines,
            "requests": self.requests,
            "receipts_missing": self.receipts_missing,
            "revalidated": self.revalidated,
            "last_attempt": self.last_attempt,
            "outcomes": dict(sorted(self.outcomes.items())),
            "observation_dates": dict(sorted(self.observation_dates.items())),
            "listed_corps": len(self.knowledge.corps),
        }


def _instant(value: object) -> datetime:
    if not isinstance(value, str):
        raise ValueError("legacy instant must be text")  # noqa: TRY004 -- malformed-content ValueError contract
    moment = datetime.fromisoformat(value)
    if moment.tzinfo is None:
        raise ValueError("legacy instant must carry its offset")
    return moment


def _document(tree: DescriptorTree, relative: str) -> dict[str, object] | None:
    if not tree.exists(relative):
        return None
    value = json.loads(tree.read_bytes(relative, max_bytes=MAX_RECEIPT_BYTES))
    if not isinstance(value, dict):
        raise ValueError(f"legacy receipt {relative} is not a JSON object")  # noqa: TRY004 -- malformed-content ValueError contract
    return cast("dict[str, object]", value)


def _outcome(tree: DescriptorTree, fingerprint: str, receipt: dict[str, object]) -> str:
    validation = _document(tree, f"receipts/{fingerprint}.validation-v1.json")
    outcome = receipt.get("outcome")
    if validation is not None and outcome == FAILED:
        outcome = validation.get("validated_outcome")
    if outcome not in OUTCOMES:
        raise ValueError(f"legacy receipt {fingerprint} has outcome {outcome!r}")
    return cast("str", outcome)


def replay(root: Path) -> LegacyReplay:
    """Read ``root`` (the legacy ``opendart`` directory) into knowledge; writes nothing."""
    result = LegacyReplay()
    newest_corp_codes: tuple[datetime, str] | None = None
    try:
        tree_context = DescriptorTree.open_path(root.absolute())
    except (OSError, DescriptorTreeError) as error:
        raise ValueError("cannot open the legacy OpenDART root") from error
    with tree_context as tree:
        seen: dict[str, dict[str, object]] = {}
        ledger = tree.read_bytes("attempts.jsonl", max_bytes=MAX_LEDGER_BYTES)
        for line in ledger.splitlines():
            event = json.loads(line)
            if not isinstance(event, dict) or set(event) != {"request", "timestamp"}:
                raise ValueError("legacy ledger line is not a request and a timestamp")
            result.ledger_lines += 1
            result.last_attempt = cast("str", event["timestamp"])
            document = cast("dict[str, object]", event["request"])
            fingerprint = hashlib.sha256(canonical_json_bytes(document)).hexdigest()
            seen[fingerprint] = {**document, "_attempted_at": event["timestamp"]}
        for fingerprint, document in seen.items():
            attempted = document.pop("_attempted_at")
            observed = str(document.get("observation_date"))
            result.observation_dates[observed] = result.observation_dates.get(observed, 0) + 1
            request = DartRequest.from_document(document)
            receipt = _document(tree, f"receipts/{fingerprint}.json")
            if receipt is None or "retrieved_at_utc" not in receipt:
                result.receipts_missing += receipt is None
                outcome, at = FAILED, _instant(attempted)
            else:
                outcome, at = (
                    _outcome(tree, fingerprint, receipt),
                    _instant(receipt["retrieved_at_utc"]),
                )
                result.revalidated += outcome != receipt.get("outcome")
            result.requests += 1
            result.outcomes[outcome] = result.outcomes.get(outcome, 0) + 1
            result.knowledge.observe(Observation(request, outcome, at))
            if (
                request.endpoint == CORP_CODES
                and outcome == COMPLETED
                and (newest_corp_codes is None or at > newest_corp_codes[0])
            ):
                newest_corp_codes = (at, fingerprint)
        if newest_corp_codes is not None:
            at, fingerprint = newest_corp_codes
            body = tree.read_bytes(f"raw/{fingerprint}.raw", max_bytes=MAX_RAW_BYTES)
            result.knowledge.listed(listed_corp_codes(body), at)
    return result
