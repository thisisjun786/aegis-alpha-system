# AAS Versioning

Versioning belongs to this repository. [POLICY.md](POLICY.md#development-and-release) owns branch, gate, and promotion authority; no external release policy is inherited.

## Version selection

- Continue from the latest published stable version. If none exists, start at `v0.0.1`.
- The default next release increments the patch component. Select a minor or major increment only with the owner's instruction, and describe compatibility changes explicitly.
- Use lowercase pre-release identifiers with increasing counters on the next intended stable version: `v0.0.2-alpha.1`, `v0.0.2-beta.1`, or `v0.0.2-rc.1`.
- A hotfix candidate uses the next intended stable version, such as `v0.0.2-hotfix.1`, and becomes `v0.0.2` when published. Do not attach a new pre-release suffix to an already released version.
- Published version tags are immutable and point to the approved `main` merge result. Never reset version history or move an existing tag.

## Release sequence

1. Prepare the `dev -> main` release PR, its intended version, user-facing notes, compatibility impact, and rollback instructions. Hotfixes use the policy's hotfix route.
2. Pass `release-gate` on the current release candidate and obtain explicit owner approval for that promotion.
3. Merge with a merge commit and identify the resulting `main` SHA.
4. When release publication is authorized, tag that SHA and publish notes using the [release template](.github/RELEASE_NOTES_TEMPLATE.md). Tagging and publication are not automatic side effects of promotion.
5. Deploy only when separately authorized, using the approved immutable commit or image digest. Record the version, provenance, and deployment result.

Notes explain user impact first. Include upgrade steps, known limitations, and rollback only at the detail needed for this release. PR lists and compare links are supporting references, not the release description itself.
