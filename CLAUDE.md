@AGENTS.md

Follow the current standalone AAS architecture and repository policy.

- `dev-notes/architecture.md` owns module and data boundaries.
- `dev-notes/operations.md` owns runnable commands and their authorization scope.
- Public code contains engine mechanisms and synthetic examples. Actual strategy
  definitions, private implementation packages and data remain outside Git.
- Preserve immutable publications, missingness semantics, authority pins, hashes,
  time/cutoff contracts and current migrations. Use disposable data for tests.
- Read nearest instructions before editing. Do not infer current policy from old
  Git history or retired runtimes.
