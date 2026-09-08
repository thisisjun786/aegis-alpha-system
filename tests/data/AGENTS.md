# Data tests

Follow ../AGENTS.md and ../../src/aegis_alpha/data/AGENTS.md.

- Test current collectors, catalogs, immutable readers and retained record formats.
  The former canonical writer/Norgate import/rebind harnesses are retired.
- Provider fixtures live under `tests/fixtures/provider_neutral/`. Keep them
  synthetic; do not import captured paid data or real strategy definitions.
- Reuse provider support modules, injected clocks/transports and the shared
  disposable PostgreSQL fixtures. Fixed UTC instants make replay deterministic.
- Recovery cases force failure, inspect durable state, then retry the same run
  and assert correct provider-call counts and unchanged completed evidence.
- Negative cases cover signature/expiry/revocation, duplicate JSON keys, unsafe
  paths/permissions, changed bytes, future observations and unsupported schemas.
- Credential sentinels must not appear in diagnostics or stored provenance.
- Output roots must remain outside Git repositories. Use tmp_path/tmp_path_factory;
  never restore an old operation-specific path for a test.
- Keep tests for shared authority and catalog contracts when retiring a CLI.
  Network access remains blocked by the root fixture; DB tests use `database`.
