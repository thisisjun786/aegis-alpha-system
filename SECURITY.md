# Security reporting

Do not put credentials, strategy definitions, private data or exploit details in
public issues, pull requests or CI logs.

When GitHub's private reporting form is available under **Security → Advisories**,
use it. Otherwise open an issue asking for a private contact route, with no
vulnerability details. Wait for a confidential handoff before sharing the report.
This document does not mean private reporting has been activated on GitHub.

Include the affected commit or installed version, a minimal reproduction using
synthetic data, expected and observed behavior, and redacted diagnostics. There
is no guaranteed response time, bounty or security-support release matrix.

## Execution boundary

Strategy definitions and data belong in separate private storage. AAS validates
explicit input versions and hashes; a hash proves byte identity, not permission
to use or execute an untrusted source. The public rule engine accepts data and
does not evaluate arbitrary Python from a strategy record.

Provider calls, DB installation/adoption and order execution have separate
authorization scopes. CI uses synthetic data and disposable databases. Never
point tests at recovered or production data. Follow [POLICY.md](POLICY.md) and
the [operating procedures](dev-notes/operations.md).
