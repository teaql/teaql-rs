# Context-owned Secret Provider example

Run `bash examples/secret-provider/verify.sh`. The first runtime-owned executable
binds a symbolic secret reference to an owner-only local file at startup,
resolves it through `UserContext` with a comment, rotates the file, and proves
the next resolution sees the new value.

The second executable installs a fixed Vault KV v2 path and a separate bootstrap
token provider, then uses the real HTTP adapter against a loopback fixture. It
checks the path, token header, response version, and a stalled-response timeout.
The verifier runs both examples twice and rejects plaintext canaries in output.
This does not require or claim a production Vault deployment.
