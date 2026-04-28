# dagster-nomad-plus

A Dagster integration for HashiCorp Nomad.

Provides a `NomadRunLauncher` that launches Dagster runs as Nomad jobs, with
support for both programmatic job construction (default) and
pre-registered parameterized HCL jobs. Supports TLS/mTLS to the Nomad API,
Consul service discovery, and Vault secret integration.

This package ships the generic Nomad integration used by Dagster+ hybrid
agents (via `dagster-cloud[nomad]`) as well as any Dagster OSS deployment.
