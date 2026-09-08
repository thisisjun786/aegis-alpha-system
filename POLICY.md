# AAS Development Policy

AAS is maintained by one owner with agent assistance. Optimize for short feedback loops, small reviewable changes, and deliberate releases. Add a check because it catches a relevant failure, not because another repository has it.

This file is the repository's development policy. [AGENTS.md](AGENTS.md) is the agent entry point; directory-level instructions define local implementation contracts. External control-plane templates and copied policy versions are not policy authorities for AAS. Product and data contracts remain governed by the [architecture](dev-notes/architecture.md) and [accepted decisions](dev-notes/decisions/README.md).

**Implementation status:** `.github/workflows/ci.yml` implements repository-owned change selection, independent jobs, and result-only gates. Verification lanes are shared with local execution. See [transition evidence](#transition) for activation requirements and rollback; a checked-in workflow alone is not remote execution proof.

## Development and release

| Target | Purpose | Required check | Merge authority | Method |
|---|---|---|---|---|
| `dev` | Integrate completed changes | `dev-gate` | Agent may merge when the conditions below hold | Merge commit |
| `main` | Promote a release | `release-gate` | Explicit owner instruction for that promotion | Merge commit |

Use a short-lived `<tool>/<change>` branch and a PR into `dev`. Keep one coherent outcome per PR: large enough to verify together, small enough to explain and revert. Do not split by agent role or file count. Use dependent PRs only when each intermediate state is valid and independently useful.

A `dev` PR is ready to merge when it is not a draft, its current head has a successful required gate, GitHub reports no conflicts, conversations are resolved, and no confirmed material defect remains. Routine integration needs no additional owner confirmation. Respect an explicit review-only request or merge hold. Use an expected-head check when merging so a newly pushed commit cannot inherit a previous decision.

Required human approvals are zero. Automated review is optional and has no mandatory waiting period. Triage useful findings; fix confirmed material defects regardless of who found them, and explain rejected findings. Do not manufacture approval rounds, mandatory issue links, per-PR planning documents, or draft-to-ready evidence rituals.

Normal releases use `dev -> main`. Require the release gate, owner approval, and user-facing release notes before merging. Urgent fixes also integrate through `dev` and the same release gate; there is no alternate-head promotion bypass. See [VERSIONING.md](VERSIONING.md) for version selection and publication.

Neither branch accepts direct pushes, force-pushes, or protection bypasses. A passing check or merge never authorizes deployment, release publication, provider calls, trading, or production data changes. Finish an owned PR by merging it under these rules or recording a concrete blocker and next action.

## Verification architecture

**Run independent checks as sibling jobs. Keep the required gate as a small result aggregator.** A gate must not install the application, rerun tests, or invoke the full verifier after its prerequisite jobs have already done the work.

The target graph is `changes -> selected jobs in parallel -> dev-gate / release-gate`. Jobs depend on another job only when they consume its output. A package build does not wait for unrelated tests; a consumer that tests a built artifact does wait for its producer.

| Job | Responsibility | Environment |
|---|---|---|
| `changes` | Select checks from the complete PR diff and record the selection | Lightweight runner; no project installation |
| `style` | Formatting and linting, including relevant shell/workflow validation | Only the required check tools |
| `types` | Type checking | Locked Python development environment |
| `tests` | All database-free application and retained-code tests | Isolated temporary data; no PostgreSQL |
| `database` | Retained PostgreSQL regression tests | Its own disposable database and runner |
| `package` | Build the Python package and smoke-test the installed artifact | Clean build/install directories |
| `container` | Build and smoke-test affected runtime images | Disposable image/container state |
| `docs` | Changed-document links, whitespace, conflict markers, and policy consistency | No project dependency installation |
| `security` | Git history/candidate secret scan, locked dependency audit and public-content checks | No private strategy data or production credentials |

These jobs run repository-owned lane implementations and `.github/actions/setup-python` prepares an isolated environment once per Python job. Keep `./scripts/verify` as the full local Python regression entry point; do not create separate implementations of the same tests for local, dev, and release runs. Image and document checks have independent entry points. Current commands are documented in [scripts/AGENTS.md](scripts/AGENTS.md).

### Development selection

Use a small explicit path map, not a custom dependency-analysis platform. Select the union of all affected scopes.

| Change | Required development coverage |
|---|---|
| Allowlisted prose only | `docs` and `security`; no application, DB, or image tests |
| Isolated application/module code and tests | `style`, `types`, `tests`, `package` |
| Retained data/registry/migration code or its tests | The code checks above plus `database` |
| Image, Compose, entry point, or installation inputs | Relevant code checks plus affected image build and smoke tests |
| Shared dependencies, test harness, classifier, verifier, or workflow logic | Full affected regression, including PostgreSQL and affected images |
| Unknown paths, uncertain shared impact | Conservative full coverage, including the AAS image |

`application/`, `modules/`, `engine/`, `tests/application/`, and `tests/engine/` are the standalone-app scope. Retained PostgreSQL dependencies include `data/`, `collection/`, `identity/`, `metadata/`, `migrations/`, SQL, and their tests. Classify paths by their actual repository location and consumers. Do not silently exempt a new shared DB layer or other new subsystem.

Pure prose can include policy and agent instructions. Validate their links and internal consistency without starting PostgreSQL merely because the text describes a gate. The classifier must use reviewed path allowlists, not a blanket `*.md` exemption: executable examples, generated inputs, and documents consumed by tests require their consumer checks. Review an intended policy change against the owner's request rather than testing that the old prose stayed unchanged.

The executable allowlist is in `scripts/ci_changes.py`: named root policy/instruction files and Markdown in known live documentation directories, excluding evidence and archived history subtrees. Security checks remain mandatory even for prose-only changes. `README.md` also requires package validation because it is packaged metadata. Executable examples require code/package/AAS-image checks; unknown paths and shared workflow/toolchain inputs require full coverage including the AAS image. Document validation checks links across live Markdown (including incoming anchor references) and whitespace/conflict markers in changed live Markdown. Preserved archives, raw captures and patch bytes are not prose formatting targets; their changes conservatively select full regression. Policy meaning remains a review responsibility.

Compute changes from the merge base to the PR head. Include deleted paths and both sides of renames. Missing history or an unreadable diff must never yield a docs-only pass. Unknown but readable paths can select full coverage; classifier execution failure must fail the gate. Record the base, head, selected jobs, and expected skips.

Do not use workflow-level path filtering to suppress a required check. The workflow must run and publish an explicit result, even for a docs-only PR.

### Release coverage

The release gate requires `security`, `style`, `types`, all database-free tests, retained PostgreSQL regression, package build/install smoke, AAS image build/CLI smoke, and documentation validation. It never uses the dev docs-only shortcut. The AAS CLI is the only supported runtime image.

Verify the combined result of the current promotion head and target `main`, and record both SHAs and the tested tree. If either input changes, refresh the candidate and gate before promotion. Do not infer DB restoration, provider readiness, strategy profitability, or order safety from these checks.

The classifier verifies the synthetic merge commit's two parents against the event's base and head; every job checks out that exact candidate. Before merging a promotion, re-read current `main` and `dev` and compare both with the successful run's recorded parents. `--match-head-commit` protects only the head, and current branch rules do not enforce strict base freshness. A changed base requires a new candidate and successful gate. Only `dev` may be the source of a PR into `main`.

Keep retained data and persistence regression while its compatibility contracts remain supported. The owner-approved separation moves the historical strategy oracle and its private regression material outside the distributed tree; generic engine tests cover public mechanisms. Preserve the private baseline before removing those files. Never remove failing tests merely to make a release green.

### Aggregation and failure handling

Run the aggregator with `always()` and include the classifier and every potentially selected job in `needs`. Require classifier success and `success` for every selected job. Permit `skipped` only when the successful classifier explicitly excluded that job. A missing result, unexpected skip, failure, or cancellation is not a pass. Never use blanket `continue-on-error` or `|| true` for required checks.

The aggregator does no verification work beyond checking results. Keep the required check names stable even when the internal job layout changes. Test the classifier and aggregator with docs-only, mixed, renamed, deleted, unknown, failed, skipped, and cancelled cases when changing CI logic.

## Execution cost and isolation

- Prepare dependencies once **per selected job**. Reuse download/build caches keyed by the relevant OS, architecture, runtime, and lock inputs. Share setup code, not a mutable `.venv`, database, or working directory between runners. Cache artifacts, not previous test verdicts.
- Bundle short checks with similar setup costs, starting with format and lint. Separate long tests, DB work, and image builds. Parallelize independent jobs instead of chaining them through a single shell dispatcher.
- Keep shared-fixture tests serial within a job. Separate PostgreSQL and database-free jobs may run concurrently because their state is isolated. Add test shards or multiple pytest workers only after proving fixture isolation and measuring a real bottleneck.
- Start with the supported Linux/Python environment. Add OS, Python-version, or database-version matrix entries only to cover an actual support commitment. Reuse setup code when a matrix becomes necessary.
- Cancel obsolete runs for the same PR and gate when a new head arrives. Keep concurrency groups distinct across PRs and between development and release. Do not duplicate the same required suite on push, draft-state changes, schedules, or post-merge transitions without a specific failure mode to justify it. Do not enable a merge queue without demonstrated integration contention.
- Give jobs bounded timeouts and preserve useful failure logs. Reap child processes and disposable resources on failure, timeout, and cancellation. Infrastructure retries must be bounded and distinguishable from test retries; never retry an assertion until it happens to pass.
- Measure queue time, setup/cache time, per-job duration, and total PR feedback time. Compare the same revision and representative cache states before and after a topology change; preserve serial verdict parity and a rollback path. Do this when changing the topology, not as a repeated ceremony on every PR. Do not promise a speedup without measurements.

## Local work and review

Run the narrowest meaningful checks while editing. There is **no mandatory full local verifier before opening a normal PR**. CI owns its selected gate coverage; reproducing a failure or validating a risky change may warrant additional local checks. Prose-only edits need document validation, not new application tests.

Scale planning and review to the change. A small fix needs a clear patch and evidence. Changes to persistence, execution boundaries, or CI failure handling need focused negative cases and a review of those risks. Use subagents for bounded independent work when useful, not as extra approval layers. Reuse valid evidence for the same revision and criteria.

Use an isolated linked worktree for implementation and preserve the canonical checkout. One owner manages each worktree; one writer owns overlapping files. Inspect existing changes first. Do not reset, clean, stash, rebase, force-push, or delete another task's work to make a command succeed. Preserve `.gjc`, credentials, data, and unknown files. Cleanup is limited to resources this task created and owns.

A PR description needs the problem/result, verification and gaps, and material risk or follow-up. Omit irrelevant sections. Report local changes, CI verification, integration, release, and deployment as distinct states.

## CI security

Use `pull_request`, minimal token permissions, reviewed full-SHA Action pins, and repository-owned verification logic. Do not execute PR code with `pull_request_target` privileges, inherited secrets, or production credentials. Development PRs may come from forks. Run them only on disposable hosted runners with read-only tokens and no secrets; apply GitHub contributor approval settings before execution. Promotions into `main` must come from `dev` in the same numeric repository ID. Re-evaluate checks when a PR changes its target branch. Repository settings are verified separately from checked-in workflow conditions.

Prefer disposable hosted runners. A self-hosted runner must isolate concurrent jobs and have no production data, credentials, or shared mutable runtime. Required tests use synthetic/offline inputs; provider and trading validation require their own explicit scope.

Only jobs needing PostgreSQL create it. Use the code-owned image/version pins and disposable test databases; never use recovered or production data. Keep ports unpublished where possible, or publish an ephemeral loopback port such as `127.0.0.1::<container-port>`. Cleanup must verify resource ownership.

## Transition

The checked-in workflow uses independent checks for the standalone AAS image and
root Python lock. The database job runs disposable PostgreSQL regression; the
public security job scans secrets, locked dependencies and distribution contents.
Legacy runtime images and their dependency environments have been removed.

Before activating this design in a new repository, verify both classic branch
protection and Rulesets. Require `dev-gate` on `dev` and `release-gate` on `main`,
resolved conversations and merge commits; prohibit direct pushes, force pushes
and deletion. Fork checks need disposable runners, read-only tokens and contributor
approval settings. Recheck the exact base and head before release promotion.
These are required settings, not a claim that they are already enabled remotely.

Public migration uses the reviewed current source tree with fresh Git history.
The existing history, strategy definitions and operating evidence remain private.
Rollback uses the private baseline and a normal reviewed change; it does not
restore production services or data without a separate operating scope.

## Design references

The selected patterns are illustrated by [CPython's explicit required-result aggregation](https://github.com/python/cpython/blob/5141621d44e8ee88d97a5b8b225c8a32e7a343f0/.github/workflows/build.yml#L632), [Ruff's change-aware jobs](https://github.com/astral-sh/ruff/blob/0451200c3428e4b81661b91af8ed75bdb16fa3fe/.github/workflows/ci.yaml), [pandas' separate serial test subset](https://github.com/pandas-dev/pandas/blob/04dff44f6224b15ed647ad5cd865905bca8b116e/.github/workflows/unit-tests.yml#L186), and [VS Code's reusable test workflows](https://github.com/microsoft/vscode/blob/fc0a9e94576224c89cc08d390b38ab760a261f1f/.github/workflows/pr.yml#L109). They are references, not inherited policy or proof of AAS performance.
