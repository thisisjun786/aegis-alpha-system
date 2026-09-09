# Local resource console

The optional console makes a private AAS installation and separately registered
resource locations visible in a browser. It reuses native workspace admission and
catalog readers. Public code and packaged assets contain no operator data.

## Boundaries

Run `python -m aegis_alpha.console --home <private-home>`. The server binds only
`127.0.0.1`; the default port is 8765. `--port 0` selects an available port.
`--registry-home` selects a separate private directory for pointers and notes;
the default is `~/.local/share/aegis-alpha-console`. Both data and registry roots
must be outside Git checkouts. Stop the foreground server with Ctrl+C.

The primary screens show readable stored strategy names and observed market-data
coverage. Strategy collections keep original records and derived research separate;
search and pagination operate within the selected collection. A strategy report
period is not market-price coverage. Investment assets are extracted from bounded
stored settings, never inferred from the strategy title.

Coverage groups supported price observations by instrument market and asset type,
with per-instrument first/last dates and the spread of final dates across each group.
Dates describe stored observations, not file modification times, gap-free history,
current listing status, point-in-time certification or execution approval. Unknown
identity and unsupported schemas remain explicit.

Secondary screens inspect store locations and file sizes, native dataset versions,
native run metadata, and imported source-table samples. Native
records and imported research records are different catalogs. An empty native
ledger does not mean no archived strategies or results exist. Source-only catalog
visibility grants neither point-in-time certification nor execution eligibility.

Registration records a path, name, kind and note. It does not adopt, copy, move,
import or execute the referenced resource. Editing changes only its name and note;
paths are immutable. No deletion, provider call, collection restart, backup,
strategy editing or order execution is exposed.

## Storage and access

`console/catalog.py` uses `storage/workspace.py` for short-lived read-only admission.
A busy installation yields a visible busy state with file metadata still available.
No background polling, full-content hashing or recursive directory scan occurs.
Strategy and coverage snapshots are independent and cached in process for at most
60 seconds, with their observation timestamp displayed. Reads have a 30-second
connection deadline. A busy cache miss reports busy; a valid cached snapshot
retains its original observation timestamp. Both lists return pages of 100 rows;
search runs across the admitted snapshot, not only the displayed page.
Coverage groups by provider-backed identity, joins matching raw and canonical
Norgate IDs, and collapses exact duplicate tables. Different providers can still
represent the same security, so counts are labelled as instrument/source counts.
Invalid dates and nonnumeric/nonfinite prices are counted separately. Native
revision visibility remains separate from this imported-source history summary.
Folder sizes are unknown, not zero. Resources and notes use their own private
JSON file and lock, so a market collector does not block note updates.

`storage/inspection.py` owns fixed metadata queries. Native lists are limited to
500 items. Source catalogs have a 5,000-candidate SQL limit, a 32 MiB streamed manifest
budget and an aggregate 6,000-table descriptor cap. Each manifest is capped before
materialization. Limited metadata is marked explicitly. Source previews show at
most 50 rows and 64 columns; SQL shortens text to 2,000 characters and omits
binary and complex values before they enter the application. Preview
is not a full export or integrity verification. Source previews use native SQL and need no Arrow dependency. DuckDB scalar
values are displayed as text; previews are not lossless exports.

Registry data uses format version 1, at most 100 resources and a 1 MiB file limit.
Updates require the observed revision and return a conflict if it changed. Writes
use descriptor-based atomic replacement under an independent lock. Path admission
rejects symlinks, foreign ownership, hardlinked files and Git-contained resources.
A missing resource remains listed with its note and an unavailable state.

## Browser boundary

This is a single-OS-user local tool, not an authenticated multi-user service.
The server validates exact Host, Origin and Fetch Metadata. Mutations additionally
require same-origin JSON requests and `X-AAS-Console: 1`. A local process running as
the same OS user can make these requests; these controls protect against foreign
websites, not a compromised local account. Direct remote binding is unsupported. An explicitly configured `--tailscale-origin`
allows exactly one Tailscale HTTPS origin through a separately configured Serve proxy.
Tailscale network access rules then govern remote callers; there is no public Funnel
configuration or per-user application authorization. Keep the backend on loopback.

Only packaged assets are served; no arbitrary file paths or SQL are accepted.
Responses use no-store, a self-only content security policy, nosniff and frame
restrictions. The browser loads no third-party fonts, scripts or analytics and
stores no private results in localStorage. Request URLs and content are not logged.
Errors omit raw database diagnostics. The API is internal browser transport with
no external stability promise and does not implement the single-owner service
from decision 0013.

## Verification

Tests under `tests/console/` use synthetic files and disposable installations.
They cover request rejection, registry revisions, path admission, missing/busy
storage, and SQLite preview without Arrow. Existing storage and application
regressions remain applicable. Browser checks exercise registration, note update,
reload, search and table preview at desktop and narrow widths. Real operator data
is never a test fixture or packaged screenshot.

The existing CI classifier conservatively selects full coverage for this new
package; this change does not alter that classifier.

## Optional Tailscale access

Supply `--tailscale-origin https://device.tailnet.ts.net:8765` and separately run
`tailscale serve --bg --https=8765 http://127.0.0.1:8765`. Only use the actual
node DNS name. Other origins remain rejected, including cross-origin mutations.
Remove only this route with `tailscale serve --https=8765 off` and restart without
the flag to return to local-only operation. Existing Serve routes need no reset.
