# teaql-cloud-core

Core trait definitions and shared models for TeaQL cloud integration.

## Overview

This crate provides the **abstraction layer** for embedding Rust services into Java microservice architectures. It defines backend-agnostic traits that can be implemented by any service discovery/configuration backend.

**This crate has zero dependency on `teaql-runtime` or `teaql-core`** — it can be used in any Rust service.

## Traits

| Trait | Purpose |
|-------|---------|
| `ServiceRegistry` | Register / deregister / heartbeat |
| `ServiceDiscovery` | Instance lookup and change subscription |
| `ConfigSource` | Configuration retrieval, publishing, and watch |
| `HealthIndicator` | Health check (Spring Boot Actuator compatible) |
| `MetricsCollector` | Metrics with Prometheus exposition format |

## Types

- `ServiceInstance` — a running service instance with IP, port, weight, metadata
- `ServiceGroup` — namespace + group (maps to Nacos/Consul/etcd/K8s concepts)
- `ConfigId` — locates a specific config entry (data_id + group)
- `HealthStatus` / `HealthDetail` / `HealthResponse` — health check results
- `Metric` / `MetricValue` — Prometheus-compatible metrics
- `ServiceLifecycle` — manages register → heartbeat → shutdown → deregister
- `CloudError` — unified error type

## Backend Implementations

| Crate | Backend |
|-------|---------|
| `teaql-cloud-nacos` | Nacos v2 gRPC |
| (future) | Consul, etcd, Kubernetes |
