# src/aegis_alpha/engine

Public reusable evaluation engine. Strategy recipes, asset lists, horizons,
signals, and ensemble membership arrive in an externally supplied bundle.
This package does not discover files, open a database, import private DAA
defaults, or execute orders.

## Public API

`load_bundle(raw, expected_sha256, expected_id, expected_version)` is the
ingress. It hashes the exact supplied bytes, parses the envelope
(`schema_version`, `bundle_id`, `bundle_version`, `contract`), and constructs
an immutable `EngineContract`. Missing nested fields, unknown keys, non-string
asset IDs, duplicate assets, non-finite numbers, and identity mismatches fail
closed. `serialize_bundle` round-trips those four envelope keys.

`replay(bundle, request)` consumes a validated bundle plus injected prices,
macro points, derived inputs, and caller-supplied ensemble rows. The receipt
records `bundle_id`, `bundle_version`, `source_sha256`, and `contract_sha256`.
It never invents a strategy when configuration is missing.

## Capabilities

The engine preserves scoring, horizon-keyed returns and moving-average ratios,
momentum formulas, comparisons, canary/switch, the seven selection/allocation
modes, derived trailing-sum yields, and positive-weight ensemble
renormalization. Feature lookup uses the actual horizon month, not tuple
position. Derived dispatch uses explicit `operation` and named fields
(`price`, `addend_a`, `addend_b`), never a private series name.

## Ownership

Private strategy data stays outside this package. CLI, CI classification, and
root documentation links to this file are owned by the surrounding application
change, not by this module.
