# Registration-time strategy lineage

The storage owners are `storage/strategies.py` (private admission, write and
receipt verification) and `storage/strategy_import.py` (durable acceptance and
recovery). This protocol uses the existing base DDL without migration. It does
not change engine contracts, requirement rows, operation phases or CLI options.

## Accepted evidence and exact bytes

`LineageSpec` contains exactly four caller fields: `parent_id`, `parent_version`,
`change_kind`, and `reason`. Identity fields must be nonempty trimmed strings;
the reason must be nonblank but retains every supplied byte, including leading
space and trailing newline. There is no caller-supplied status.

For present lineage, the request is the following object, with the illustrated
identity, raw pin, caller fields and status replaced by their actual values:

```json
{
  "schema_version": "aas-strategy-import-request-v2",
  "strategy_id": "child",
  "version": "1",
  "raw_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "lineage": {
    "parent_id": "parent",
    "parent_version": "7",
    "change_kind": "derived",
    "reason": " exact\n"
  },
  "parent_status": "unresolved"
}
```

The request hash is lowercase SHA-256 of canonical UTF-8 JSON: sorted keys,
compact separators, `ensure_ascii=false`, `allow_nan=false`, no trailing newline.
Allowed statuses are exactly `resolved` and `unresolved`; null/unknown reject.
The existing `reason_hash` is SHA-256 of the canonical JSON **string**, including
its JSON quotes and escapes, not the unquoted reason bytes.

Both immutable `storage_operations.request_hash` and private
`strategy_imports.request_hash` store this v2 digest as independent commitments.
The existing eight-column lineage row stores the accepted status. The parent
identity is its exact ID/version, not an added raw/contract pin or recursive
eligibility proof. The preimage is reconstructible, not another persisted file.

Other fields remain unchanged:

| Field | Value |
| --- | --- |
| operation ID | `strategy-` + SHA-256 of UTF-8 `strategy_id + '\x00' + version + '\x00' + raw_sha256` |
| kind | `strategy_import` |
| target ID | `strategy_id + ':' + version` |
| expected_parent | SQL NULL |
| payload_hash | Exact raw bundle SHA-256 |
| no-lineage request_hash, both stores | Exact raw bundle SHA-256 (unchanged v1) |

No-lineage imports have no lineage row or synthetic status. The operation ID
excludes all lineage fields and status, so changed caller evidence conflicts
with the same durable key. Creation/completion times are metadata, never proof.

## First durable acceptance and retries

Workspace registration validates external bytes/pins, execution definition,
existing content/lineage/receipts and cycles before creating a new intent. While
workspace admission is held, the **first durable acceptance** is the parent
snapshot immediately before PREPARED commits. A genuinely new request selects
resolved only if the exact direct parent is registered, otherwise unresolved.
The later private commit persists this accepted choice, not a new observation.

An existing operation is looked up before choosing a fresh status. Using the
original raw bundle and exact caller fields, retry constructs both v2 digests
and requires exactly one to equal the existing request hash. Zero or two matches
reject. This decodes a two-element commitment domain; it does not infer history
from current parent existence. No-lineage has only the raw-SHA candidate.
`prepare_operation` still checks every immutable identity field and rejects a
quarantined operation. No old identity is rewritten.

Before idempotent reuse, existing private rows must authenticate their **actual
stored status** against every receipt. The supplied caller fields and accepted
state request must agree. A missing matching state intent is not manufactured
from an orphan receipt, and an unsupported private record cannot gain a new seal
by adding another receipt.

The internal prepared-import handoff supplies the committed request hash, not a
status override or validation bypass. One private write owner serves that path
and standalone `import_strategy`. Standalone fresh imports select and seal status
inside their transaction; existing versions reuse only authenticated evidence.
Private admission repeats atomically, preserving raw/contract checks and cycle
rules. Resolved still requires the direct parent to exist. Accepted unresolved
allows a now-present parent without promotion. An intervening cycle or content
conflict rejects, leaving the original intent pending rather than rewriting,
demoting or automatically quarantining it.

| Durable interruption state | Retry / recovery |
| --- | --- |
| Nothing committed | No accepted snapshot; a fresh attempt observes the then-current parent. |
| PREPARED v2 only | Original caller bytes decode the accepted status; identical retry keeps it even after parent arrival. Recovery alone stays pending: no private evidence exists to replay or authenticate. |
| PREPARED plus private commit | Verify actual stored content/status, every private receipt and matching intent. Source-independent recovery changes only completion metadata/phase. |
| COMPLETED plus private commit | Identical retry leaves evidence unchanged. Recovery does not rewrite terminal state. |
| Changed parent/version/kind/exact reason or lineage presence | Reject before private writes/new intent; keep old evidence. |
| Corrupt, unsupported or quarantined evidence | Reject; no adoption, status reselection or silent sealing. |

## Reads, eligibility and backup

The private reader validates zero-or-one lineage row, exact caller field validity,
reason hash, supported status and at least one receipt. **Every** receipt for the
version must match the hash reconstructed from the actual row (or raw SHA when
there is no row). It must never choose whichever status candidate matches while
ignoring a forged row. Missing/extra lineage or missing/disagreeing receipts fail.

Load authenticates evidence before unresolved eligibility rejection. False
demotion is invalid evidence, not an ordinary valid unresolved strategy. A
legitimately sealed unresolved version is valid stored content but cannot be
loaded/shown for execution. Resolved only checks its exact direct parent's
existence; a direct parent with an unresolved ancestor is permitted. An unresolved
edge does not require the parent still to be absent. Existing cycle rules include
unresolved edges; this is not recursive ancestry eligibility.

Load uses only its supplied strategies connection, with no hidden state discovery.
A state-only corruption therefore fails workspace verification/recovery/backup,
not private load. Workspace verification additionally checks receipt/intent
identity, request equality, NULL expected parent, raw payload pin and both
directions of the operation/receipt graph. An absent private receipt for PREPARED
stays pending, not falsely authenticated or completed.

Content verification remains separate from current execution derivation. Stored
pre-admission bundles with invalid scoring definitions can still pass integrity,
recovery and backup/restore if their raw bytes, parsed contract, complete v1
requirement rows and required evidence are valid; execution inspection may reject.

Ordinary checks are SELECT-only. Recovery uses stored bytes, not source files,
providers or execution derivation, and grants no eligibility. Backup refuses
pending operations and invalid evidence. Restore verifies logical contents even
when outer backup hashes are honestly refreshed; a failed restore remains
`restore-incomplete`, never ready.

## Compatibility and proof limit

The shipped baseline registration API had no lineage argument and wrote no
lineage rows. Its no-lineage v1 receipts, identity, reads, retry, committed recovery
and backup/new-root restore are preserved unchanged, without migration.

Interim branch lineage used caller-only `aas-strategy-import-request-v1`. It did
**not** seal derived status. Such records are physically retained but cannot be
accepted for load/show, verified retry/resealing, committed recovery, integrity
success, backup or ready restore. PREPARED-only interim evidence remains pending
on recovery; retry rejects rather than manufacture a historical status seal.
A lineage row paired with raw-SHA receipts (or absent row with lineage receipts)
is invalid, not a compatibility fallback.

Current parent existence, timestamps, rowids, restored outer hashes and a checksum
of today's status cannot distinguish legitimate interim status from a forgery.
There is no automatic migration, current-state blessing, status CLI flag or
validation-off path. A new child version can carry newly accepted lineage, but
cannot authenticate old history or make unsupported old workspace evidence pass.

These are immutable-store integrity commitments, not externally signed history.
They detect status-only tampering with independent hashes intact and one-sided
cross-store disagreement. Someone able to rewrite the row and all commitments
coherently can manufacture history; no new signing secret is introduced.

## Independent digest vectors

For the illustrated synthetic raw pin, exact reason `" exact\n"`, child `child:1`
and parent `parent:7`, stdlib JSON/SHA-256 gives:

| Input | Literal digest |
| --- | --- |
| unresolved v2 | `5cb4df616642f167342c946051fbac471d21b232cc99a4d383a150f4e0d6b79d` |
| resolved v2 | `264bdef280c2a0cf60794aee3aa0ef50c5da4e755bd56fe44fc1b738c7bf2c9f` |
| unresolved, reason changed only to `" exact\n "` | `15f96950fe815a02faa5ec8c4630fc412da1679bcc9feb34d751650d68a74504` |
| interim v1: omit status, change schema string | `a469eccd2f87eb1e9cd02d6697f2bd2ce38350b7a344b862abe7604c4d6fa4fd` |
| original reason_hash | `030019ad868b140268253350e32ff60b333b3428a25b5a4d7ed3c8e5ea2dce27` |
| operation ID suffix, every variant | `a35b6c4bcbb03a071d4b715fafe2eebcceb3b037043fb8f1cdbd5dbafa5e3089` |

The no-lineage request and raw payload pin remain the illustrated 64 `a`s;
expected_parent remains SQL NULL. These are hash-algebra vectors, not a claim
about a real bundle's raw hash. Regression tests use independent literal
expectations and copied stores with exact schema restoration.
