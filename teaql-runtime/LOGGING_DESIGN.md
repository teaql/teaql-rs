# TeaQL Logging Zero-Code Configuration Design

## Overview
This document outlines the standard environment-variable-driven logging configuration for the TeaQL Runtime. The goal is to provide a unified, zero-code way to control log outputs, formatting, and granularity across all supported languages (Java, Rust).

## 1. Output & Routing (System Level)
Controls where and how logs are written.
* `TEAQL_LOG_ENDPOINT`: Specifies the exact destination for logs (e.g., `stdout`, `off`, or a file path like `/var/log/app.log`).
* `TEAQL_DOMAIN`: Acts as a fallback for the log file name. If `TEAQL_LOG_ENDPOINT` is not set, logs will default to `${TEAQL_DOMAIN}.log`.
* `TEAQL_LOG_FORMAT`: Determines the log output format (`human`, `json`, `debug`).
* `TEAQL_LOG_MAX_SIZE` / `TEAQL_LOG_MAX_FILES`: Configures rolling file strategies (e.g., `50MB`, `7`).

## 2. Level Control (Module Level)
Controls the verbosity of three core modules. The standard prefix is `TEAQL_{MODULE}_LOG`.
Allowed values:
- `_silent`: No output.
- `_summary`: Skeleton fields (e.g., execution time, status, basic identifier).
- `_full`: Includes business intent and essential payload.
- `_full_with_payload`: Includes full request/response bodies or deep debugging data.

### Core Modules:
* **`TEAQL_AUDIT_LOG`**
  * **Scope**: Entity lifecycle and mutations (Create, Update, Delete).
  * **Default**: `_full` (production compliance standard).
* **`TEAQL_SQL_LOG`**
  * **Scope**: Underlying database SQL execution.
  * **Default**: `_summary` or `_silent` (to prevent flooding production logs).
* **`TEAQL_TOOL_LOG`**
  * **Scope**: Tooling and external integrations (HTTP calls, File I/O, etc.).
  * **Default**: `_full`.

## 3. Focus & Filtering (Fine-Grained Level)
Used to isolate logs for specific domains without lowering the global level.
* `TEAQL_AUDIT_LOG_ENTITIES`: Comma-separated list of entities to capture (e.g., `TEAQL_AUDIT_LOG_ENTITIES=User,Order`).
* `TEAQL_SQL_LOG_TABLES`: Comma-separated list of database tables to track (e.g., `TEAQL_SQL_LOG_TABLES=users,orders`).
* `TEAQL_TOOL_LOG_FOCUS`: Comma-separated list of tool subsystems to monitor (e.g., `TEAQL_TOOL_LOG_FOCUS=http,file`).
