"""Validate completed FMP evidence without constructing a collector or transport."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from http import HTTPStatus
from pathlib import Path

from aegis_alpha.collection.records import CollectionRunPlan
from aegis_alpha.data.fmp_catalog_io import (
    CatalogFiles,
    digest,
    identifier,
    instant,
    json_object,
    list_value,
    object_value,
    require,
)
from aegis_alpha.data.fmp_dataset_selection import ALL_DATASETS, DatasetSelection
from aegis_alpha.data.fmp_symbol_observation import DURABLE_RESPONSE_KEYS


@dataclass(frozen=True, slots=True)
class CompletedEvidence:
    run_id: str
    receipt_path: Path
    receipt_bytes: bytes
    receipt_sha256: str
    receipt: dict[str, object]
    marker: dict[str, object]
    artifacts: tuple[tuple[str, Path, int, str], ...]
    provenance_addresses: tuple[str, ...]


def _address(value: object) -> str:
    require(isinstance(value, str) and value.startswith("sha256:"), "invalid FMP content address")
    return digest(str(value).removeprefix("sha256:"))


def _addresses(value: object) -> list[str]:
    values = [_address(item) for item in list_value(value)]
    require(len(values) == len(set(values)), "duplicate FMP content addresses")
    return values


def _json(
    files: CatalogFiles, root: Path, path: Path, expected: str | None = None
) -> dict[str, object]:
    return json_object(files.read(root, path, expected))


def _provenance(
    files: CatalogFiles,
    run_id: str,
    sequence: int,
    attempt: dict[str, object],
    attempt_hash: str,
) -> str:
    root = files.raw_root
    ref = _json(
        files, root, root / "fmp/runs" / run_id / "provenance" / f"{sequence:08d}.provenance.json"
    )
    sha = digest(ref["provenance_sha256"])
    record = _json(files, root, root / "fmp/provenance/sha256" / sha[:2] / f"{sha}.json", sha)
    require(set(record) == DURABLE_RESPONSE_KEYS, "FMP response provenance schema mismatch")
    require(
        ref.get("schema_version") == 1
        and ref.get("run_identity") == run_id
        and ref.get("attempt_seq") == sequence
        and record.get("run_identity") == run_id
        and record.get("attempt_seq") == sequence
        and ref.get("request_fingerprint")
        == record.get("request_fingerprint")
        == attempt.get("request_fingerprint")
        and record.get("attempt_record_sha256") == attempt_hash,
        "FMP response provenance lineage mismatch",
    )
    for key in ("content_sha256", "raw_byte_length", "status_code"):
        require(record.get(key) == attempt.get(key), "FMP response differs from attempt")
    require(
        instant(record["retrieved_at_utc"]) >= instant(record["requested_at_utc"]),
        "FMP response timestamps are reversed",
    )
    sha_body = digest(record["content_sha256"])
    size, _ = files.verify(
        root, root / "fmp/blobs/sha256" / sha_body[:2] / f"{sha_body}.raw", sha_body
    )
    require(size == record["raw_byte_length"], "FMP raw response length mismatch")
    return sha


def _attempts(
    files: CatalogFiles, run_id: str, marker: dict[str, object]
) -> tuple[set[str], set[str]]:
    root = files.raw_root
    relative = Path("fmp/runs") / run_id / "attempts"
    tree = files.tree(root)
    names = tree.listdir(relative) if tree.exists(relative) else ()
    require(all(name.endswith(".json") for name in names), "unexpected FMP attempt file")
    previous = "0" * 64
    raw: set[str] = set()
    provenance: set[str] = set()
    byte_count = rate_limited = retry_waits = 0
    last: dict[str, object] | None = None
    for sequence, name in enumerate(sorted(names), 1):
        require(name == f"{sequence:08d}.json", "FMP attempt sequence is incomplete")
        payload = files.read(root, root / relative / name)
        record = json_object(payload)
        require(
            record.get("attempt_seq") == sequence
            and record.get("run_identity") == run_id
            and record.get("previous_attempt_sha256") == previous,
            "FMP attempt hash chain mismatch",
        )
        instant(record["attempted_at_utc"])
        _address(record["request_fingerprint"])
        previous = hashlib.sha256(payload).hexdigest()
        outcome = record.get("outcome")
        require(
            outcome in {"response", "timeout", "network_error", "credential_rejected_response"},
            "FMP attempt outcome is invalid",
        )
        size = record.get("raw_byte_length", 0)
        require(type(size) is int and size >= 0, "FMP attempt byte count is invalid")
        byte_count += int(str(size))
        rate_limited += record.get("status_code") == HTTPStatus.TOO_MANY_REQUESTS
        retry_waits += record.get("retry_after_wait") is True
        if outcome == "response":
            raw.add(digest(record["content_sha256"]))
            provenance.add(_provenance(files, run_id, sequence, record, previous))
        last = record
    require(digest(marker["attempt_ledger_sha256"]) == previous, "FMP marker attempt root mismatch")
    require(
        marker["usage"]
        == {
            "calls_attempted": len(names),
            "bytes_received": byte_count,
            "rate_limited_attempts": rate_limited,
            "retry_after_waits": retry_waits,
        },
        "FMP marker usage differs from attempt ledger",
    )
    latest_path = root / "fmp/runs" / run_id / "latest-attempt.json"
    if last is not None:
        require(_json(files, root, latest_path) == last, "FMP latest-attempt pointer mismatch")
    return raw, provenance


def _receipt_refs(
    files: CatalogFiles,
    run_id: str,
    receipt: dict[str, object],
    raw: set[str],
    provenance: set[str],
) -> None:
    require(
        set(_addresses(receipt["raw_content_addresses"])) == raw,
        "FMP receipt raw references differ from attempts",
    )
    refs = _addresses(receipt["provenance_addresses"])
    for index, sha in enumerate(_addresses(receipt["request_metadata_chunk_addresses"])):
        chunk = _json(
            files,
            files.raw_root,
            files.raw_root / "fmp/receipt-metadata/sha256" / sha[:2] / f"{sha}.json",
            sha,
        )
        require(
            chunk.get("run_identity") == run_id
            and chunk.get("schema_version") == 1
            and chunk.get("chunk_index") == index,
            "FMP receipt metadata lineage mismatch",
        )
        refs.extend(_addresses(chunk["provenance_addresses"]))
        for request_value in list_value(chunk["requests"]):
            request = object_value(request_value)
            require(
                _address(request["raw_content_address"]) in raw,
                "FMP request metadata references foreign raw bytes",
            )
    require(
        len(refs) == len(set(refs)) and set(refs) == provenance,
        "FMP receipt provenance inventory differs from attempts",
    )


def _artifacts(
    files: CatalogFiles,
    run_id: str,
    marker: dict[str, object],
    plan: CollectionRunPlan,
) -> tuple[tuple[str, Path, int, str], ...]:
    inventory: list[tuple[str, Path, int, str]] = []
    seen: set[str] = set()
    selection = DatasetSelection(str(plan.parameters.get("dataset_selection", "probe")))
    require(plan.dataset == selection.plan_dataset, "FMP plan dataset differs from its selection")
    run_root = files.dataset_root / "normalized/fmp/.runs" / f"run_id={run_id}"
    for item in list_value(marker["artifacts"]):
        record = object_value(item)
        require(set(record) == {"path", "sha256"}, "FMP artifact inventory schema mismatch")
        path = Path(str(record["path"]))
        relative = Path(files.relative(run_root, path))
        require(len(relative.parts) == 2, "FMP artifact must belong to one run dataset")  # noqa: PLR2004 -- dataset/leaf
        dataset, leaf = relative.parts
        require(
            dataset in ALL_DATASETS and dataset in selection.datasets,
            "FMP artifact dataset is outside the run selection",
        )
        require(
            leaf == "history-index.json"
            or (leaf.startswith("part-") and leaf.endswith(".parquet")),
            "FMP artifact is not a normalized part or immutable index",
        )
        key = str(path).casefold()
        require(key not in seen, "FMP marker contains duplicate artifact paths")
        seen.add(key)
        sha = digest(record["sha256"])
        size, _ = files.verify(files.dataset_root, path, sha)
        if leaf == "history-index.json":
            index = _json(files, files.dataset_root, path, sha)
            require(
                index.get("dataset") == dataset and index.get("schema_version") == 1,
                "FMP history index differs from its declared dataset",
            )
        inventory.append((dataset, path, size, sha))
    return tuple(inventory)


def completed_evidence(
    files: CatalogFiles, plan: CollectionRunPlan, run_id: str
) -> CompletedEvidence:
    identifier(run_id)
    root = files.raw_root / "fmp/runs" / run_id
    completion = _json(files, files.raw_root, root / "completion.json")
    require(
        set(completion) == {"marker_sha256", "plan_id", "receipt_path", "receipt_sha256", "run_id"},
        "FMP completion schema mismatch",
    )
    marker_sha = digest(completion["marker_sha256"])
    marker = _json(files, files.raw_root, root / "publication.json", marker_sha)
    require(
        set(marker)
        == {"attempt_ledger_sha256", "plan_id", "run_id", "artifacts", "advances", "usage"},
        "FMP publication schema mismatch",
    )
    require(
        completion["run_id"] == marker["run_id"] == run_id
        and completion["plan_id"] == marker["plan_id"] == plan.plan_id,
        "FMP completion identity differs from persisted run",
    )
    receipt_path = Path(str(completion["receipt_path"]))
    receipt_sha = digest(completion["receipt_sha256"])
    receipt_bytes = files.read(files.home, receipt_path, receipt_sha)
    receipt = json_object(receipt_bytes)
    require(
        receipt.get("run_id") == receipt.get("run_identity") == run_id
        and receipt.get("plan_id") == plan.plan_id
        and receipt.get("plan_sha256") == plan.plan_sha256
        and receipt.get("publication_marker_sha256") == marker_sha
        and receipt.get("usage") == marker["usage"]
        and receipt.get("artifact_hashes") == plan.parameters.get("artifact_hashes")
        and receipt.get("mode") == plan.mode.value
        and receipt.get("as_of") == plan.parameters.get("as_of"),
        "FMP receipt differs from persisted plan or publication",
    )
    recovery = plan.parameters.get("recurring_recovery")
    if recovery is not None:
        output = Path(str(object_value(recovery)["output_path"]))
        shard = plan.parameters.get("shard")
        if shard is not None:
            index, total = shard if isinstance(shard, tuple) else list_value(shard)
            output = output.with_name(f"{output.stem}-shard{index}of{total}{output.suffix}")
        require(output == receipt_path, "FMP receipt path differs from persisted operation")
    raw, provenance = _attempts(files, run_id, marker)
    _receipt_refs(files, run_id, receipt, raw, provenance)
    return CompletedEvidence(
        run_id,
        receipt_path,
        receipt_bytes,
        receipt_sha,
        receipt,
        marker,
        _artifacts(files, run_id, marker, plan),
        tuple("sha256:" + sha for sha in sorted(provenance)),
    )
