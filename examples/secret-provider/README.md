# Context-owned Secret Provider example

Run `bash examples/secret-provider/verify.sh`. The runtime-owned executable
binds a symbolic secret reference to an owner-only local file at startup,
resolves it through `UserContext` with a comment, rotates the file, and proves
the next resolution sees the new value. The verifier runs twice and rejects
any plaintext canary in output. No Vault or remote secret service is used.
