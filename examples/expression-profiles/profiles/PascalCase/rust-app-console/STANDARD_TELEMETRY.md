## Standard OpenTelemetry module

Use the optional standard module supplied by the selected TeaQL runtime. Match
its version to the runtime dependency selected by the model and workspace;
keep local source or internal Registry overrides during development. Do not
upgrade to an unrelated public release just to enable telemetry. A runtime
without this module reports `module_unavailable`; package installation alone
does not enable export.

On a supported backend, configure these trusted process variables before startup, never
through a query, JSON or TFP payload:

```bash
export TEAQL_TELEMETRY_ENABLED=true
export TEAQL_TELEMETRY_SERVICE_NAME=payment-service
export TEAQL_TELEMETRY_OTLP_ENDPOINT=http://localhost:4318
```

Without enablement, startup reports `disabled` and all three activation variables;
no SDK export resources are created. Enabled startup reports safe effective
settings, full recording and masking. Invalid settings produce a fixed safe
diagnostic, not a claim of successful export. Never print credentials or raw
configuration values.

The module automatically records supported Q, graph mutation and physical SQL
operations as traces, logs and metrics. SQL is expanded with masked parameters;
plaintext debugging never authorizes plaintext telemetry export. Full recording
does not promise lossless delivery when bounded queues or exporters fail.
Do not add per-method spans or replace the customer's global SDK providers.

Keep schema reconciliation explicit. Run the generated ensure-schema and seed
flow through the same configured runtime to observe startup settings and audited,
idempotent seeding. Telemetry setup must not run ensure-schema implicitly.
Flush and shut down the owned pipeline at application shutdown, not per query.
Exporter failures do not change business results; durable business audit remains
an independent path.


### Rust assembly

Enable the `standard-telemetry` feature on the application's existing
`teaql-runtime` dependency, preserving its selected version, registry or path.
UserContext initialization selects the optional module automatically. Do not
add SDK initialization to generated Q or entity methods. Set the three variables
before launching the process.
