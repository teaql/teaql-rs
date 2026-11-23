# Supplier onboarding in 15 locales with TeaQL Rust

The [KSML model](model.xml) describes a warehouse with suppliers. The [semantic model](semantics.json) sends two registrations through the generated workspace. One lacks a supplier name. The other submits 3 as the minimum order quantity, below the allowed value of 10. The application attempts audited saves and uses generated Q to confirm that SQLite stores neither invalid supplier.

## Run and select a locale

Rust and Cargo, a C compiler for the SQLite dependency, and Python 3.9 or newer. The verified compiler was Rust 1.97.1. This example uses local Rust runtime 8.2.0 source. The runner compiles the generated console against the enclosing runtime and maps its TeaQL crates to the same local source via temporary Cargo config arguments.

```sh
bash examples/supplier-onboarding-i18n/run.sh
bash examples/supplier-onboarding-i18n/run.sh --locale fr
bash examples/supplier-onboarding-i18n/run.sh --all
bash examples/supplier-onboarding-i18n/run.sh --edge-cases
bash examples/supplier-onboarding-i18n/run.sh --verify
```

The commands run from the runtime repository root. The output is UTF-8 JSON Lines. Each validation row gives the effective locale, stable message key, canonical field, native rule, arguments and localized message. `--all` uses the 15 locales from the semantic model in one process. The final command runs the full set twice against one SQLite file and runs the locale boundary checks.

The app selects the locale with `context.set_locale_code(code)?` and reads `context.language().code()`. A new context defaults to `en`. The parser trims, normalizes underscores and case, and resolves known aliases such as `ZH_hant` to `zh-TW`. An empty or unsupported code raises `RuntimeError::UnsupportedLocale` and preserves the previous locale. Single-language mode prints the rejection and exits 2. Locale selection comes from the trusted CLI and context. Environment variables and business input do not set it.

`TEAQL_RUST_EXAMPLE_SOURCE_ROOT` defaults to the enclosing repository. `CARGO_NET_OFFLINE=true` uses the existing public Cargo cache. Native compilation uses a disposable target directory.

## How the message is produced

1. The console builds its `UserContext`, installs generated model metadata, and explicitly initializes SQLite. It sets the locale before the generated business call.
2. The real model check rejects the invalid registration. The generated `service_runtime_from_env` composes each context and SQLite provider. The save yields `RuntimeError::Check`; the example reads the rule's wire ID and the native JSON parameter from `to_wire`.
3. The renderer chooses the locale and a template for the field's location shape. It binds the rendered field, model path and applicable checker parameters, including the lower bound.
4. The sample attaches the generated application catalog through `context.set_i18n_catalog(Arc::new(I18nCatalog::from_json(&text)?))`. Its Vietnamese message entries are absent. Built-in Vietnamese supplies the same two messages after attachment. For this natural checker message, selection tries the application's shape-specific override, the application legacy template, and the built-in natural template at the active locale, then repeats that order for English. The generic invalid rule can supply the final natural template. Direct lookup through the legacy message catalog selects application and built-in entries at the current locale first, then application and built-in English, then the stable key.

The independent French context remains French through the main context's Japanese, Arabic and Japanese sequence. Domain labels without an available translation can remain in English. The application binds `SUPPLIER_ONBOARDING_SERVICE_CORE_DATABASE_URL` to its temporary file. This variable selects the database. [Locale source](../../teaql-runtime/src/language.rs) · [Message lookup source](../../teaql-runtime/src/i18n.rs) · [Natural templates source](../../teaql-runtime/src/checker_messages/catalog.rs)

## Run the same scene in another runtime

Keep the same KSML and semantic inputs when switching to its sibling checkout. Each runtime uses its own setup and call style, while the error keys, field paths and minimum bound stay stable.

[Java](../../../java/examples/supplier-onboarding-i18n/README.md) · [Rust](./README.md) · [TypeScript](../../../typescript/examples/supplier-onboarding-i18n/README.md) · [Go](../../../go/examples/supplier-onboarding-i18n/README.md) · [Python](../../../python/examples/supplier-onboarding-i18n/README.md) · [C#/.NET](../../../dotnet/examples/supplier-onboarding-i18n/README.md) · [Swift](../../../swift/Examples/SupplierOnboardingI18n/README.md)

## Add a locale

Add an `Italian` variant and code match in `language.rs`, add it to the list of supported values, then extend alias normalization as needed. Update the built-in resources `builtin-i18n-v1.json`, `checker_messages/builtin-v1.json` and `checker_messages/presentation-defaults-v1.json` under `teaql-runtime/src`. Supply natural forms for the supported shapes and add presentation words, such as the name for the current object and the measurement units. For a legacy required-message entry the identifier and placeholder stay the same.

```json
{
  "it": {
    "messages": {"checker.required": "Il campo {location} è obbligatorio"},
    "vocabulary": {}
  }
}
```

Keep placeholders such as `{field}`, `{location}` and `{system}` aligned with their respective catalog schemas. Add the locale to the generator and shared semantic model, regenerate the workspaces, then run the same save, alias, fallback and isolation checks. To customize text for an already supported locale, attach an explicit application catalog.
