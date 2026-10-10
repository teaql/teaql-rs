# TeaQL Rust

An AI-native library and runtime for building reviewable business applications in Rust.

TeaQL gives coding agents business-shaped APIs, model-aware guidance and
software checks. People review the business operation instead of reconstructing
pages of query-building, relationship-loading and persistence code.

Automatic execution logs make those operations easier to debug and operate,
without adding logging statements to every business method.

[Try with your coding agent](#try-it-with-your-coding-agent) ·
[Run the local example](#run-the-local-example) ·
[See automatic logs](#automatic-logs-without-handwritten-logging-code)

[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/13608/badge)](https://www.bestpractices.dev/projects/13608)

## Why this is an AI native library

AI-native does not mean the library was written by AI, or that every query
calls an LLM. It means the APIs, knowledge delivery and verification workflow
are designed for how coding agents work: limited context, incomplete knowledge
of a project and a tendency to guess unfamiliar operations.

The semantic model describes your business objects, fields, relationships and
states. A generated domain library turns those words into language-native
**Q** queries, **E** expressions and mutation methods. This repository provides
the reusable, domain-independent **runtime** that executes them through
metadata, Context, Checker, Policy and database providers.

The application workspace keeps business code and concise project guidance
together. Progressive **Assist** supplies the exact API guidance needed for
the next operation, instead of asking the agent to explore the entire generated
library. Compilation, runtime checks and tests provide feedback beyond a prompt.

## Less code for a reviewer to reconstruct

The benefit is visible in the application code: the query, data access path
and audited change stay together, while shared mechanics live in the library
and runtime.

| Application code to review | Shared work handled by TeaQL |
| --- | --- |
| A composed Q request | SQL construction and selected relation loading |
| An E access path | Loaded-field checks and explicit NULL handling |
| A mutation with an audit reason | Change tracking, Checker and configured Policy execution |
| Business intent and audit reason | Automatic execution logs and trace context without per-operation logging code |

Reviewers still decide whether the requested data, business rules and
authorization policy are correct. They have less application plumbing to
trace before making that decision; reuse does not make business logic correct
automatically.

**Token savings have not yet been measured in a controlled comparison.**
We make no numerical token-saving claim. Loading guidance only when needed
is a context-management strategy; the smaller application review surface is
visible in the examples, not a measured percentage reduction in review time.

## See a business operation execute

The [Robot Task Board showcase](https://teaql.io/blog/robot-task-board-showcase/)
makes the idea concrete: move a task and follow the state change, generated
SQL, field-level audit and refreshed board.

![TeaQL Robot Task Board](https://teaql.io/img/003-task-board.png)

Explore the [open-source demo](https://github.com/teaql/robot-task-board).
The showcase is a Rust application; the examples below use this repository's
own Rust APIs.

## Q API composes business queries

These application-code excerpts assume the example's generated imports, trusted
Context and an existing object identity. `Q` and `E` belong to the generated
domain library, not a generic runtime object with arbitrary field strings.
See the [application source](examples/school-management/src/main.rs) for complete setup.

```rust
let school_type_details = Q::school_types_minimal().select_name().select_code();

let mut school = Q::schools()
    .with_name_is("Riverside Primary School")
    .select_platform_with(Q::platforms_minimal().select_name().select_base_url())
    .select_school_type_with(school_type_details)
    .comment("what: load the school and its reference data")
    .purpose("why: review a school before editing")
    .execute_for_one(&context)
    .await?
    .expect("School must exist");
```

The child request is a query in its own right. Give it a business name,
extract it into a helper and compose it into other requests like a Lego brick.
The reviewer can see which relationships are loaded without following a
separate set of SQL loops. `comment` describes **what**; `purpose` describes
**why**. Both must be non-empty, even when logging is disabled.

## E API protects business reads

```rust
let name = E::school(&school).get_name().eval();
let type_code = E::school(&school).get_school_type().get_code().eval();
```

E reads the loaded graph without implicitly querying the database. A loaded
NULL and a field that was **NotLoaded** are different facts. A business rule
must not conclude that a value is absent merely because the query never
requested it.

The access path is explicit, so the reviewer checks whether Q loaded the
required data and whether the code handles a real NULL appropriately.
Unloaded access produces the language-specific diagnostic; a NULL fallback
is not permission to ignore an unloaded property.

## Mutation API keeps changes behind one Save boundary

The application declares the change and its reason instead of writing its own
persistence loop. A graph can stage additions, updates and deletions before Save.

Load the complete scalar fields of every persisted object you actually modify.
A display-only partial projection is not a complete mutation input. Unmodified
reference objects do not have to become fully loaded just to save their parent.

```rust
school.update_name("Riverside Academy");
let saved = school.audit_as("Rename the reviewed school").save(&context).await?;
```

Save traverses the reached mutation graph, runs Checker/Fix and the configured
customer Mutation Policy, then uses the provider transaction boundary.
An explicit policy denial blocks persistence. Missing customer policy or exact
approval emits a warning; it is not automatic rejection. Marking for deletion
stages a change—the audited Save performs it. Atomicity is provider/route scoped,
not a distributed transaction across unrelated databases.

## Automatic logs without handwritten logging code

**Debuggability and operability are core design goals.** The Q and Save
examples above contain no logging calls: you declare the business intent,
and the runtime produces the execution diagnostics and trace context.
Configured audit sinks receive change events without each application
method having to construct its own field-level diff.

The intent declarations already belong to the business operation:

- `comment` describes what the query does.
- `purpose` explains why it runs.
- `audit_as` gives the reason for a saved change.

No-code logging means no handwritten logger calls in each business method,
not an application without code or intent declarations. Runtime startup
still configures any required sink; custom formats and destinations use the
logging extension points rather than edits to every business operation.

- **Debugging:** connect a query's intent to generated SQL, execution outcome
  and the trace path when a screen or business operation behaves unexpectedly.
- **Operations:** follow query and mutation activity and use audit reasons
  and safely exposed changes to investigate incidents.
- **Review:** inspect the business operation without separately checking
  that every TeaQL execution path has a matching application logging statement.

Query and Mutation diagnostic logs are enabled by default and can be switched
off independently. Logging off does not disable intent validation or Policy.

Illustrative, shortened view of the rename operation, not a literal logger
format or a captured test result; the SQL excerpt omits additional selected fields:

```text
query entity=School comment="load the reviewed object" purpose="edit form" outcome=success
SQL: SELECT id, version, name FROM school_data WHERE id = 42;
trace: School -> request -> sqlite -> select
mutation entity=School auditReason="Rename the reviewed school" outcome=committed
change: name "Riverside Primary School" -> "Riverside Academy"
lineage: School#42("Rename the reviewed school")
```

The declarations explain **why**; SQL and audit evidence show **what happened**.
Together they help locate the relevant operation without reconstructing the
entire call path from scattered application code.

SQL execution stays parameterized. Diagnostic SQL has parameters substituted
after field-aware masking; it is not a question-mark template you must manually
reconstruct. Ordinary bindings stay visible, sensitive bindings use the shared
mask algorithm, and credentials/unknown bindings stay hidden. A masked statement
is labeled `MASKED; NOT REPLAYABLE`; SQL that cannot be safely rendered is omitted.
Application mutation audits are delivered after commit and discarded on rollback.
This is not a durable audit outbox. Do not put secrets in comment or purpose.

## Discover only the API needed next

A coding agent does not need every generated method in its context. Start with
the generated application's `AGENTS.md` and workspace guidance, request an
entity/action Assist, then ask for field-specific help only for the field
being used.

For a model in `models/`, the Rust query path is:

```bash
cargo teaql rust-assist-query/school --input models/
cargo teaql rust-assist-query/school.established_date --input models/
```

These commands call the model-aware service; the model must match the generated
library. Assist uses canonical KSML entity and field names, even when the
language-native API uses a different naming style. Cargo TeaQL is a service
client for multiple languages, not just Rust.

Use Assist to discover operations instead of reading generated library source
or guessing names. If the required operation is missing, report
`MISSING_ASSIST` rather than inventing an API. The
[Agent Kit](https://github.com/teaql/teaql-agent-kit) defines this progressive
workflow. Library and runtime maintenance are separate from application coding.

## Software checks complement agent instructions

AI proposes the model and application logic; software checks the parts that
can be verified mechanically. The library is one part of the TeaQL harness,
not a replacement for business review.

| Stage | TeaQL support | Feedback to inspect |
| --- | --- | --- |
| Model and repair | Semantic model evaluation | Errors, warnings and repair guidance |
| Generate | Domain library combined with this runtime | Language tooling checks the generated API contract |
| Customize the workspace | Progressive Assist, Q, E and mutation APIs | Build diagnostics, Checker and configured Policy results |
| Verify and operate | Tests, runtime startup, intent and audit evidence | Actual outcomes, remaining failures and execution paths |

Build checks verify API use, not whether the business decision is right.
Tests must still verify the application's behavior. A prompt, successful
generation or a clean build alone does not prove the finished application correct.

## Try it with your coding agent

You can start with an agent you already use. Paste this prompt:

```text
Follow the current instructions at https://github.com/teaql/teaql-agent-kit.
Build a small school-management application in Rust using SQLite.
Evaluate and repair the model, generate the domain library and prepare a runnable
application workspace. Verify Q, E, Checker and audited Save.
Finish with a report of commands, results and
remaining frictions. Include Q/E examples and relevant intent/audit logs.
Do not claim token savings without a measured comparison.
```

The [Agent Kit](https://github.com/teaql/teaql-agent-kit) guides modeling,
evaluation/repair, generation and application customization. It may install
toolchains and download dependencies; review installation permissions and the
final execution report. It is a workflow, not a guarantee of unattended correctness.

## Run the local example

A stable Rust toolchain supporting edition 2024. SQLite examples need no database server. From a checkout of this repository:

```bash
cargo run --manifest-path examples/school-management/Cargo.toml
```

The [School example](examples/school-management/README.md)
exercises SQLite schema/bootstrap and generated application APIs against this
repository's local runtime. No external database server or generator is needed
to run the retained generated example. Use only the example's test database;
reset/repeated-run behavior is documented in its README.

## Use the published runtime

Current documented runtime: **5.0.6**. Keep domain-library and runtime
versions compatible; regenerate older libraries when their provider or graph
adapter SPI has changed.

```toml
[dependencies]
teaql-core = "5.0.6"
teaql-runtime = "5.0.6"
teaql-provider-sqlite = "5.0.6"
```

Generation is a separate service; see the [Agent Kit](https://github.com/teaql/teaql-agent-kit)
for producing the application-specific library and workspace. Repository examples
use local source; isolated released-package regression is a separate gate.

## Supported scope

Rust is a backend reference runtime with PostgreSQL, MySQL, SQLite and in-memory execution. Optional web, cache, cloud and tool integrations are separate crates. A generated domain library supplies application-specific Q/E APIs; the runtime is reusable and domain-independent.

One semantic model can produce language-native applications across
[Java](https://github.com/teaql/teaql-java),
[Rust](https://github.com/teaql/teaql-rs),
[TypeScript](https://github.com/teaql/teaql-ts),
[Go](https://github.com/teaql/teaql-golang),
[Swift](https://github.com/teaql/teaql-swift),
[.NET](https://github.com/teaql/teaql-dotnet) and
[Python](https://github.com/teaql/teaql-python).
That does not imply identical APIs, providers or complete cross-language feature parity.
The semantic model defines business objects, fields, relationships and states;
it is not the language model used by a coding agent.
The [conformance matrices](https://github.com/teaql/teaql-conformance)
distinguish implemented source, executed examples, artifact verification and release.

## Verification and customization

```bash
cargo test --workspace --exclude teaql-provider-linux
bash scripts/verify-examples.sh
```

The full example gate is the acceptance entry point for local runtime changes.
External-provider tests need their configured services; skips are not live-provider
evidence. A committed-operation error after audit delivery failure must not be
retried as if the business transaction rolled back.

Install application-owned policies, ID/business-ID services, logging sinks,
transports and other supported SPIs through trusted runtime/Context setup.
Request JSON is not allowed to replace those trusted capabilities.

- [Runtime guide: modules, SPI and advanced verification](RUNTIME_GUIDE.md)
- [Workspace and provider details](RUNTIME_GUIDE.md#workspace-layout)
- [SQLite transactions](teaql-provider-sqlite/README.md)
- [Shared load state](examples/shared-load-state/README.md)
- [Trace Chain example](examples/trace-chain/README.md)
- [Benchmarks and reproduction](https://github.com/teaql/teaql-runtime-benchmark)

## Safe troubleshooting

Default diagnostics do not authorize plaintext sensitive data. Controlled
debugging requires the exact acknowledgement below and any sensitive sink
required by the runtime:

```bash
export TEAQL_ALLOW_SENSITIVE_PLAINTEXT_LOGS=I_UNDERSTAND_SENSITIVE_DATA_MAY_BE_WRITTEN_TO_DISK
```

Debug output is explicitly labeled. Credentials and unknown bindings remain
protected; the flag does not erase old files or govern application/driver
prints outside TeaQL. Restrict access and retention, then unset it and restart.
See the [runtime guide](RUNTIME_GUIDE.md) for log switches and extension details.
