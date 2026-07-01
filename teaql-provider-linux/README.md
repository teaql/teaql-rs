# teaql-provider-linux

TeaQL data service provider for Linux system information. This crate allows you to query the local Linux system's `/proc` filesystem directly through TeaQL.

## Features

This provider uses `procfs` to extract real-time system metrics and exposes them as TeaQL queryable entities. It currently supports reading:
- **SystemInfo**: Global system metrics including load averages, uptime, and memory usage.
- **Process**: Information about running processes (PID, state, CPU usage, memory, command line, etc.).
- **Thread**: Thread-level information for specific processes.

*Note: This provider is **read-only**. It acts only as a `QueryExecutor` and will not modify the underlying system.*

## Usage

The recommended way to use this provider is alongside the **TeaQL Code Generator**. While you can manually define models and use the provider, using the generator makes querying the Linux system fully type-safe and incredibly ergonomic.

1. **Generate the Domain Library**
   Use `teaql-code-gen` to generate a native Rust library from your domain model that contains `SystemInfo`, `Process`, and `Thread` entities.

2. **Add Dependencies**
   In your application, depend on the generated library and this provider:
   ```toml
   [dependencies]
   your-generated-lib = { path = "../your-generated-lib" }
   teaql-provider-linux = "4.1.0"
   teaql-runtime = "4.1.0"
   ```

3. **Initialize the Context**
   Construct the `LinuxDataServiceExecutor` and provide it to the generated user context to execute queries against the system:

   ```rust
   use teaql_provider_linux::LinuxDataServiceExecutor;
   use your_generated_lib::runtime::UserContext;
   use your_generated_lib::process::Process;

   #[tokio::main]
   async fn main() -> Result<(), Box<dyn std::error::Error>> {
       // 1. Initialize the Linux provider
       let executor = LinuxDataServiceExecutor::new();
       
       // 2. Initialize the generated user context
       let ctx = UserContext::new(executor).await?;
       
       // 3. Query the system with full type safety!
       let processes = ctx.process().query().find_many().await?;
       for process in processes {
           println!("PID: {}, Name: {}", process.id(), process.name());
       }
       
       Ok(())
   }
   ```

### Full Example
For a complete, runnable example of using `teaql-provider-linux` with generated code, see the **[Linux System Info Application Example](https://github.com/teaql/teaql-rust-app-examples/tree/main/003-linux-sysinfo-using-teaql)** in the `teaql-rust-app-examples` repository.

## Entities Mapping

The following entity queries are resolved dynamically by querying `/proc` on-the-fly:

- `SystemInfo` -> Returns a single record reflecting the current system state.
- `Process` -> Supports fetching all processes or fetching a specific process by its `id` (PID).
- `Thread` -> Supports fetching threads.

## Permissions

Some metrics require elevated permissions to read (e.g., certain memory metrics or inspecting processes owned by other users). If the provider lacks permission to read specific `/proc` entries, it will gracefully skip them or leave those fields empty.
