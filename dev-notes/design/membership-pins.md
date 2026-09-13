# Immutable membership pins

[`storage/membership_pins.py`](../../src/aegis_alpha/storage/membership_pins.py) owns
exact registration and SELECT-only reconstruction of two v1 whole-document
formats. They use the existing [state tables](../../src/aegis_alpha/storage/state_schema.py),
not stored JSON, an extra marker, a request bundle, or an identity-resolution
engine. This is the bounded snapshot/universe prerequisite; subsequent request
binding work must reuse these bytes and APIs rather than define another codec.

## Exact document formats

Every key below is required, including nullable keys. Additional keys at any
level, duplicate JSON keys, duplicate references, and unused declarations reject.
The pseudo-schema names below stand for the complete objects, not optional examples.

```text
Identity = {
  schema: "aas-identity-snapshot-v1",
  hash_format: "aas-canonical-json-sha256-v1",
  snapshot_id: text,
  instruments: [Instrument], assertions: [Assertion],
  members: [IdentityMember], sources: [Source]
}
Universe = {
  schema: "aas-universe-version-v1",
  hash_format: "aas-canonical-json-sha256-v1",
  universe_id: text, version: version,
  instruments: [Instrument], members: [UniverseMember], sources: [Source]
}
Instrument = {
  instrument_id: text, issuer_id: text|null, asset_type: text, venue: text
}
Assertion = {
  assertion_id: text, instrument_id: text,
  provider: text, namespace: text, token: text,
  valid_from_us: us, valid_to_us: us|null, known_from_us: us,
  supersedes_assertion_id: text|null,
  source_snapshot_id: text, source_hash: sha
}
IdentityMember = {
  ordinal: integer, assertion_id: text,
  valid_from_us: us, valid_to_us: us|null,
  known_from_us: us, known_to_us: us|null
}
UniverseMember = {
  instrument_id: text,
  valid_from_us: us, valid_to_us: us|null,
  known_from_us: us, known_to_us: us|null,
  source_snapshot_id: text
}
Source = {
  snapshot_id: text, provider: text,
  requested_at_us: us, retrieved_at_us: us, publication_at_us: us|null,
  status: "raw_verified"|"quarantined", files: [SourceFile]
}
SourceFile = {relative_path: text, byte_hash: sha, size_bytes: integer}
```

- `text` is nonempty, trimmed, printable Unicode scalar text. No controls,
  unpaired surrogates, case folding, or Unicode normalization. IDs are opaque.
- `sha` is exactly 64 lowercase ASCII hexadecimal characters.
- Integers are JSON integers, never bool or float, in signed SQLite int64 range.
  Knowledge, request, retrieval, publication, creation, ordinal, and size values
  are nonnegative. Economic starts may be negative. A finite interval end must
  exceed its start; null end is positive infinity, not unknown knowledge.
- `version` is text other than exact lowercase `latest`. `Latest` and `LATEST`
  are valid distinct explicit universe versions. Identity snapshot IDs include
  literal `latest` and `LATEST`. There is no latest lookup.
- Source retrieval must be at or after request. Null publication remains unknown.
  Relative paths are normalized POSIX paths: no absolute path, backslash, NUL,
  empty component, `.` or `..`. They are not resolved against an incoming file.
- `issuer_id=null` means no issuer binding. Nonnull issuers must already exist;
  issuer display names are excluded. Asset type and venue are stored literal
  classifications, not tradability claims.

Identity has exactly one assertion per member assertion ID, one instrument per
distinct assertion instrument ID, and one source per distinct assertion source ID.
Universe has one instrument per distinct member instrument ID and one source per
distinct member source ID. Empty member sets are valid frozen sets; their dependent
arrays must also be empty. Universe's unique member key is
`(instrument_id, valid_from_us, known_from_us)`. No universe ordinal is invented.

Assertions include every retained assertion column. A predecessor is an exact
reference to an existing assertion or another supplied assertion. Self-reference,
cycles among supplied assertions, and unknown references reject. New assertions
are inserted in dependency order. An existing external predecessor remains an ID
reference; its entire historical registry is not traversed or resolved.

Identity members contain caller-supplied **projected** intervals, distinct from raw
assertion intervals. They are not clipped to raw intervals or derived from later
assertions, source timestamps, registration time, or UTC midnight. The same
provider/namespace/token may not overlap in both economic and knowledge dimensions
within one snapshot. Half-open adjacency is legal. Distinct provider keys may
identify the same instrument. Distinct universe episodes can overlap and retain
any-active/OR semantics; there is no precedence inference.

The hash binds every projected member field, assertion ID and full assertion,
and the full joined instrument. Missing JOIN targets cannot disappear as empty
history. Every referenced source must already exist and match **all** header
fields and its **complete** file inventory, including no extra files. Empty file
inventories are representable. Registration does not create sources or issuers,
copy files, upgrade status, or evaluate source authority. Existing raw verification
separately checks safe raw-root file access, byte hashes, and sizes.

`Assertion.source_hash` is an opaque retained assertion provenance digest. The
native contract does not say which bytes it authenticates. It is neither derived
from provider/token nor equated with a source-file hash. Source and assertion
providers need not agree. No new aggregate source hash or authority-policy field
is invented.

## Canonical bytes and hashes

Incoming documents are strict UTF-8 bytes without BOM or literal NUL, at most
1 MiB. `expected_file_sha256` must match SHA-256 of the **exact incoming bytes**
before decoding. The existing strict JSON decoder rejects duplicate object keys
and nonfinite constants; the complete schema is validated before serialization.

Normalize only these array orders:

1. Instruments by `instrument_id`, assertions by `assertion_id`, sources by
   `snapshot_id`, and source files by `relative_path`, in Unicode code-point order.
2. Identity members by `assertion_id`. Their supplied ordinals must already equal
   zero-based positions in this order. Gapped, negative, duplicate, or changed
   ordinals reject; they are not repaired or interpreted as priority.
3. Universe members by `(instrument_id, valid_from_us, known_from_us)`, comparing
   integers numerically. Duplicate keys reject before sorting, even if identical.

[`canonical_json_bytes` and `content_sha256`](../../src/aegis_alpha/data/serialization.py)
own serialization: UTF-8, sorted object keys, `ensure_ascii=False`, `allow_nan=False`,
compact `,`/`:` separators, standard JSON escaping, decimal integers, lowercase
null, no trailing newline/BOM/whitespace or Unicode normalization. No float,
Decimal, date, or coercion reaches this serializer from these schemas. Canonical
bytes must also fit 1 MiB.

The semantic content hash is SHA-256 of that **whole canonical document**. Schema
and hash-format literals are inside the bytes. There is no self-hash field, prefix,
rowset framing, physical database hash, installation/path/operation/creation ID,
or hash of a hash. Source relative paths remain content provenance; absolute
operating paths are excluded. Transport whitespace and array permutations can
change file SHA-256 without changing semantic SHA-256.

The existing header table and v1-specific pin type imply the format; there is no
new format column or synthetic snapshot version. Read success requires:

```text
requested content_hash == stored header content_hash
                       == SHA256(reconstructed canonical document)
```

Registration additionally requires reconstructed bytes equal submitted canonical
bytes. Existing arbitrary labels fail closed without mutation, adoption, repair,
or migration. A historical row that genuinely matches the complete v1 content is
valid regardless of who inserted it. Changed identity content needs a new snapshot
ID; changed universe content needs a new `(universe_id, version)`. A new valid pin
does not repair an invalid old pin for whole-workspace integrity.

Independent complete byte/digest vectors live in
[`test_membership_pins.py`](../../tests/storage/test_membership_pins.py).

| Vector | Bytes | SHA-256 |
| --- | ---: | --- |
| I0, empty identity | 167 | `acad1dd33d462620eec24fb70ad1598d5d0920ed1f97ce71cd19837d4c0b9a29` |
| U0, empty universe | 162 | `174da3304f5c64c49526800534a95cae51439e82738e1e79e427ca5ddbd6102b` |
| I1, single identity | 953 | `4ee2b31ef469210c4dd39609722ddf8c3bcb2bcbe1c91bbfc1faecc2d89e0cc6` |
| U1, single universe | 676 | `b431314edcb080dadf853b8313d8c102da8ad793b52ecaa1c8b9c7a4a5b10bde` |

I1's synthetic `source_hash=a*64` is opaque. Its file is exactly empty bytes with
SHA-256 `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`.

## Public API and ownership

```python
IdentityPin(snapshot_id: str, content_hash: str)
UniversePin(universe_id: str, version: str, content_hash: str)

register_identity_snapshot(connection, raw: bytes, *,
                           expected_file_sha256: str,
                           created_at_us: int) -> IdentityPin
register_universe_version(connection, raw: bytes, *,
                          expected_file_sha256: str) -> UniversePin
read_membership_pins(connection, identity_pin: IdentityPin | None,
                     universe_pin: UniversePin | None, *,
                     max_materialization_bytes: int) -> VerifiedMemberships
```

The frozen pin types retain their positional shapes and validation and are
re-exported by `market_inputs`. `VerifiedMemberships.identity` and `.universe`
are each None or a frozen `VerifiedMembership` containing `pin`, immutable
`canonical_bytes`, and immutable `members`. Each member projection has exactly
`instrument_id`, `valid_from_us`, `valid_to_us`, `known_from_us`, `known_to_us`.
These are derived from the same verified document, never a subsequent unverified
query. Supplying one pin is the individual read API; two null pins return empty
evidence, not an implicit lookup.

Writers validate incoming shape/hash/limits, then enter existing `state.atomic`
**before** checking existing headers or referenced state. Matching retries verify
actual content and preserve the original `created_at_us`, ignoring a different
valid supplied creation time. Existing conflicts never insert missing content to
make the old header match. New registration reuses exactly matching declarations,
inserts only explicitly supplied missing instruments/assertions and all header/
member rows, then reconstructs and compares before returning. All writes roll back
together on any failure. Nested callers retain their own transaction with SAVEPOINT
isolation; writers neither commit nor discard prior caller work.

Readers execute SELECT only, including scalar aggregates and missing-reference
checks. They do not open connections, acquire compute leases, run PRAGMA/DDL,
start writer transactions, write receipts, or repair content. The caller's admitted
workspace owns schema/FK/store checks and installation locks for the entire read.
Out-of-band concurrent writers bypassing those locks are not a security boundary;
sequential legal INSERTs between reads are detected. Trust is recomputed on every
fresh read; detached evidence remains unchanged.

## Admission and integrity

Both writes and reads use the conservative per-document estimate:

```text
charge = 65536 + 16384 * R + 128 * B
```

`R` counts each member, assertion, instrument, source, and source-file document
row once. `B` sums UTF-8 bytes of all text fields in those rows plus root IDs
(snapshot ID, or universe ID and version). Relational parent columns omitted from
the document are not additional document fields. Fixed schema/hash literals are
covered by overhead. Repeated content across two documents is charged twice.
Zero documents cost zero. Each document must fit 64 MiB; reads also require a
positive caller allowance covering the **sum of both documents**.

Before fetching any variable-width document rows, readers check both pins using
scalar counts, byte-length aggregates (`length(CAST(field AS BLOB))`, including
provenance text), and missing-reference queries over the entire base member set
and distinct reachable evidence. Neither active dates, requested instrument
subsets, latest rows, INNER JOIN survivors, nor LIMIT prefixes reduce admission.
Materialization failure raises `ComputeResourceError`; invalid references/content/
hashes raise controlled `ValueError`. This is an admission estimate for buffers,
objects, sorting, validation, and detached projections, **not measured RSS or a
guaranteed total-process memory bound**.

`market_inputs._members` supplies its existing shared allowance
`(budget.memory_limit_bytes - budget.duckdb_memory_limit_bytes)//8`. Price/session/
grid allocations, compute admission ownership, and actual DuckDB limits are not
changed. Results are complete or explicitly rejected, never truncated.

`verify_workspace` verifies every header, including empty and unreferenced pins,
one document at a time with a 64 MiB maintenance allowance. Existing backup and
new-root restore inherit that same logical verification in addition to physical
file checks. Copied member/JOIN/interval/source-inventory corruption rejects even
if outer backup hashes are refreshed; failed restore stays `restore-incomplete`.
No original membership document is required after registration. Old stores remain
physically intact when unverifiable headers block backup.

Content integrity proves neither source eligibility, temporal truth, authority,
PIT qualification, strategy lineage, nor execution readiness. Quarantined sources,
unknown publication, empty inventories, closed intervals, empty snapshots, and
unready strategy evidence can be valid preserved content. Authority and future
request/bundle/execution admission remain separate owners.
