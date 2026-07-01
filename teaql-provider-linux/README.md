# teaql-provider-linux

TeaQL data service provider for Linux system information. This crate allows you to query the local Linux system's `/proc` filesystem directly through TeaQL.

## Features

This provider uses `procfs` to extract real-time system metrics and exposes them as TeaQL queryable entities. It currently supports reading:
- **SystemInfo**: Global system metrics including load averages, uptime, and memory usage.
- **Process**: Information about running processes (PID, state, CPU usage, memory, command line, etc.).
- **Thread**: Thread-level information for specific processes.

*Note: This provider is **read-only**. It acts only as a `QueryExecutor` and will not modify the underlying system.*

## Usage

Add `teaql-provider-linux` to your `Cargo.toml`:

```toml
[dependencies]
teaql-provider-linux = "4.1.0"
```

To use it, construct a `LinuxDataServiceExecutor` and pass it to your runtime or context:

```rust
use teaql_provider_linux::LinuxDataServiceExecutor;

let executor = LinuxDataServiceExecutor::new();
// Provide `executor` to your TeaQL user context to execute queries against the system
```

## Entities Mapping

The following entity queries are resolved dynamically by querying `/proc` on-the-fly:

- `SystemInfo` -> Returns a single record reflecting the current system state.
- `Process` -> Supports fetching all processes or fetching a specific process by its `id` (PID).
- `Thread` -> Supports fetching threads.

## Permissions

Some metrics require elevated permissions to read (e.g., certain memory metrics or inspecting processes owned by other users). If the provider lacks permission to read specific `/proc` entries, it will gracefully skip them or leave those fields empty.
