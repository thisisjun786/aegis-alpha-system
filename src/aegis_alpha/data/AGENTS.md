# Data contracts and acquisition

This directory uses flat provider prefixes. Follow root policy and decision 0012.

| Work | Owner |
| --- | --- |
| Data-root configuration and safe file access | `data_root.py`, `descriptor_tree.py` |
| Catalog validation and version-pinned prices | `catalog_access.py`, `pinned_prices.py`, `price_schema.py` |
| FMP one-shot/backfill and daily acquisition | `fmp_collector_cli.py`, `fmp_daily_cli.py` and `fmp_*` contracts |
| Macro observations/vintages and raw archives | `fred_alfred_*`, `fred_raw_archive*` |
| SEC collection, bulk archives and period identity | `sec_*`; trusted policy pin in `sec_policy.py` |
| FinImpulse estimates | `finimpulse_*` |
| Supported historical record/schema formats | `canonical_records.py`, `canonical_json.py`, `canonical_generation_schema.py` |

- Filesystem access uses admitted data roots and descriptor-relative operations:
  O_NOFOLLOW/O_DIRECTORY, locks, fsync and file identity checks. Do not resolve a
  path and then trust a later open of that pathname.
- Published files are immutable and no-clobber. Never rewrite verified data,
  delete unowned state, or silently adopt incomplete output.
- Authority and credentials are external inputs. Verify hashes, signatures,
  permissions, scope, expiry and revocation before side effects. Planning and
  help commands do not call providers, open operational DBs or write outputs.
- Preserve source hashes, UTC availability, adjustment basis and explicit missingness.
  Never clamp anomalous OHLC rows, invent prices, or use ticker alone for identity.
- SEC does not repair identity or prices from a discrepancy sidecar. Failed FRED
  series do not advance watermarks; unsupported derived spreads are not inferred.
- Usage ledgers and receipts remain durable across interruption. Uncertain runs
  cannot trigger unaccounted paid retries.
- `duckdb_engine.py` owns its version pin; `pinned_prices.py` owns catalog-bound
  queries and resource budgets. No obsolete glob/query runtime is installed.
- Generation SQL models remain for installation/adoption. Their presence does
  not provide the removed canonical build/publish/postflight/rebind operations.
- Do not revive fixed Norgate snapshot import/bootstrap tools or host-specific pins.

Use focused `uv run --no-sync pytest tests/data -m "not database"` and the
isolated database lane when needed. All provider fixtures are synthetic/offline.
