This document records the agreed workflow and the current progress of the unit-test enhancement backlog.

## Working Agreements

- Use English for issues, branches, commits, pull requests, comments, and documentation.
- Create a GitHub issue before changing code.
- Keep each issue narrow enough to describe one behavior or one closely related behavior group.
- Use one branch and one pull request per issue.
- Never create a branch whose name starts with `codex`.
- Use `master` as the trunk branch for this repository.
- Do not combine unrelated cleanup or production-code changes with a test issue.
- Preserve pre-existing local work before switching branches. Use a clearly named stash when necessary, and do not drop it during the unit-test workflow.
- Follow `AGENTS.md`. In particular, do not edit generated files, and read exact generated APIs rather than guessing method names.

## Issue-to-Merge Workflow

### 1. Prepare the trunk branch

1. Inspect the working tree and identify any unrelated local changes.
2. Preserve unrelated work in a named stash if branch switching requires it.
3. Switch to `master`.
4. Run `git pull --ff-only`.
5. Confirm that local `master` matches `origin/master`.

### 2. Define a focused issue

The issue should include:

- the component and behavior under test;
- the important success, boundary, and failure cases;
- explicit acceptance criteria;
- the expected focused test command;
- a statement that production behavior should not change.

Prefer several small issues over a single broad coverage issue. This keeps failures, reviews, and future reversions easy to understand.

### 3. Create an issue branch

Create the branch only after the issue exists. Use a descriptive name containing the issue number, for example:

```text
test/24-entity-descriptor-helpers
```

Do not use a `codex` prefix.

### 4. Implement the tests

- Read the implementation under test before writing assertions.
- Exercise observable behavior instead of internal implementation details.
- Cover boundary and negative cases where they add meaningful protection.
- Keep fixtures local and minimal.
- Avoid changing production code unless the issue explicitly requires it.
- Never manually edit files under `generate-lib/`, `generate-workspace/`, or `bizcore/` unless specifically instructed.

### 5. Verify locally

Run checks proportional to the change. The standard sequence used for the completed core issues is:

```bash
cargo fmt --all -- --check
cargo test -p teaql-core <focused-test-filter>
cargo clippy -p teaql-core --all-targets --all-features
cargo test --workspace --exclude teaql-provider-linux --no-fail-fast
git diff --check
```

The unfiltered workspace cannot build on macOS because `procfs` supports Linux and Android. This is an environment limitation, not a test failure. GitHub CI runs on Ubuntu and remains the authority for the full repository checks.

Existing warnings should be reported but not mixed into a focused test PR unless they prevent completion.

### 6. Commit, push, and open the pull request

- Use an English conventional commit, such as `test(core): cover entity descriptor helpers`.
- Push the issue branch.
- Open a ready-for-review pull request.
- Include `Closes #<issue-number>` in the pull-request body.
- Summarize the behaviors covered and list the verification commands.
- Mention the macOS/Linux provider limitation when the local workspace command excludes `teaql-provider-linux`.

### 7. Merge only after CI succeeds

The required GitHub checks observed during this work are:

- `Test and Lint`
- `Security Audit`

The repository currently rejects GitHub auto-merge with `Auto merge is not allowed for this repository`. The established fallback is:

1. Wait until both required checks succeed.
2. Squash-merge the pull request immediately after CI is green.
3. Delete the remote issue branch.

Do not merge while either check is pending or failing.

### 8. Return to a clean trunk

After the pull request is merged:

1. Switch local work back to `master`.
2. Run `git pull --ff-only`.
3. Confirm `master` matches `origin/master`.
4. Delete the local issue branch. A squash-merged branch may require `git branch -D` because its original commit is not an ancestor of the squash commit.
5. Confirm the working tree is clean and any unrelated stash is still intact.
6. Start the next issue only after this cleanup is complete.

## Current Progress

Snapshot on 2026-07-17:

- Trunk: `master`
- Completed issues: 16 of 16
- Remaining issues: 0 of 16
- Active unit-test branch: none

### Completed

| Issue | Coverage | Pull request | Result |
| --- | --- | --- | --- |
| [#19](https://github.com/teaql/teaql-rs/issues/19) | `Value` numeric conversion boundaries, including signed, unsigned, decimal, floating-point, and unrelated variants | [#35](https://github.com/teaql/teaql-rs/pull/35) | Squash-merged |
| [#20](https://github.com/teaql/teaql-rs/issues/20) | `Value` date and timestamp parsing, timezone normalization, and invalid input | [#36](https://github.com/teaql/teaql-rs/pull/36) | Squash-merged |
| [#21](https://github.com/teaql/teaql-rs/issues/21) | Trimmed string serde helpers for required and optional values | [#37](https://github.com/teaql/teaql-rs/pull/37) | Squash-merged |
| [#22](https://github.com/teaql/teaql-rs/issues/22) | `SafeExpression` optional composition, lazy fallbacks, errors, and null branches | [#38](https://github.com/teaql/teaql-rs/pull/38) | Squash-merged |
| [#23](https://github.com/teaql/teaql-rs/issues/23) | `SmartList::merge_by` replacement, append order, and repeated incoming keys | [#39](https://github.com/teaql/teaql-rs/pull/39) | Squash-merged |
| [#24](https://github.com/teaql/teaql-rs/issues/24) | Entity descriptor defaults and lookup helpers | Merged | Squash-merged |
| [#25](https://github.com/teaql/teaql-rs/issues/25) | `EntityGraph` builder annotations and child operations | Merged | Squash-merged |
| [#26](https://github.com/teaql/teaql-rs/issues/26) | `ObjectLocation` formatting and nesting levels | Merged | Squash-merged |
| [#27](https://github.com/teaql/teaql-rs/issues/27) | `CheckObjectStatus` inference and explicit markers | Merged | Squash-merged |
| [#28](https://github.com/teaql/teaql-rs/issues/28) | `Language` code aliases and canonical codes | Merged | Squash-merged |
| [#29](https://github.com/teaql/teaql-rs/issues/29) | Hierarchical trace-chain recovery | Merged | Squash-merged |
| [#30](https://github.com/teaql/teaql-rs/issues/30) | `GraphMutationPlan` batching keys and counts | Merged | Squash-merged |
| [#31](https://github.com/teaql/teaql-rs/issues/31) | `MutationRequest` trace and comment accessors | Merged | Squash-merged |
| [#32](https://github.com/teaql/teaql-rs/issues/32) | `WebResponse` constructors and JSON shape | Merged | Squash-merged |
| [#33](https://github.com/teaql/teaql-rs/issues/33) | `TeaContext` request-header extraction | Merged | Squash-merged |
| [#34](https://github.com/teaql/teaql-rs/issues/34) | Recursive JSON-to-`Value` conversion for Meilisearch | Merged | Squash-merged |

### Remaining Backlog

None! All planned issues have been completed and merged.

## Preserved Local Work

Unrelated work that existed before the unit-test cycle remains preserved in:

```text
stash@{0}: On 20260709-source-location-trace: wip/20260709-source-location-trace-before-test-19
```

This stash belongs to the pre-existing `20260709-source-location-trace` work. Do not pop, drop, or modify it as part of the unit-test backlog unless explicitly requested.
