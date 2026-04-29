"""Programmatic builders for Nomad Job JSON.

The Nomad HTTP API accepts fully-formed job specs as JSON (no HCL needed);
see https://developer.hashicorp.com/nomad/api-docs/json-jobs. This module
builds those dicts from Dagster-friendly kwargs so the run launcher and
UserCodeLauncher can construct server / run jobs consistently.

We model the common subset: a single ``batch`` or ``service`` job with one
task group and one Docker task, plus optional Consul ``Services``, a
``Vault`` block, constraints, affinities, and sidecars. Advanced
customizations should land here rather than in the launchers.
"""

import base64
from collections.abc import Mapping, Sequence
from typing import Any

_DEFAULT_CPU_MHZ = 500
_DEFAULT_MEMORY_MB = 512


def _resources_block(resources: Mapping[str, Any] | None) -> dict[str, Any]:
    if not resources:
        return {"CPU": _DEFAULT_CPU_MHZ, "MemoryMB": _DEFAULT_MEMORY_MB}
    out: dict[str, Any] = {}
    if "cpu" in resources:
        out["CPU"] = int(resources["cpu"])
    if "memory" in resources:
        out["MemoryMB"] = int(resources["memory"])
    if "memory_max" in resources:
        out["MemoryMaxMB"] = int(resources["memory_max"])
    if "cores" in resources:
        out["Cores"] = int(resources["cores"])
    if "disk" in resources:
        out["DiskMB"] = int(resources["disk"])
    out.setdefault("CPU", _DEFAULT_CPU_MHZ)
    out.setdefault("MemoryMB", _DEFAULT_MEMORY_MB)
    return out


def _env_list_to_map(env_vars: Sequence[str] | Mapping[str, str] | None) -> dict[str, str]:
    """Accept Dagster-style ``["KEY=value", "OTHER"]`` or plain dicts."""
    if not env_vars:
        return {}
    if isinstance(env_vars, Mapping):
        return {str(k): str(v) for k, v in env_vars.items()}
    out: dict[str, str] = {}
    for entry in env_vars:
        if "=" in entry:
            key, value = entry.split("=", 1)
            out[key] = value
        else:
            # Bare name: caller is expected to have already resolved from env,
            # but we propagate the name as an empty passthrough so Nomad's
            # client env still makes it available when set on the host.
            out[entry] = ""
    return out


def _constraints(constraints: Sequence[Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    if not constraints:
        return []
    out: list[dict[str, Any]] = []
    for c in constraints:
        out.append(
            {
                "LTarget": c.get("attribute") or c.get("l_target", ""),
                "Operand": c.get("operator", "="),
                "RTarget": str(c.get("value", "")),
            }
        )
    return out


def _affinities(affinities: Sequence[Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    if not affinities:
        return []
    return [
        {
            "LTarget": a.get("attribute", ""),
            "Operand": a.get("operator", "="),
            "RTarget": str(a.get("value", "")),
            "Weight": int(a.get("weight", 50)),
        }
        for a in affinities
    ]


def _service_block(
    consul: Mapping[str, Any] | None,
    *,
    provider: str,
    port_label: str | None,
    default_name: str,
) -> list[dict[str, Any]]:
    """Build a ``Services`` entry for either Nomad-native or Consul discovery."""
    if consul is None and provider == "nomad":
        return [
            {
                "Name": default_name,
                "Provider": "nomad",
                **({"PortLabel": port_label} if port_label else {}),
            }
        ]
    if consul:
        service: dict[str, Any] = {
            "Name": consul.get("service_name", default_name),
            "Provider": "consul",
            "Tags": list(consul.get("tags") or []),
        }
        if port_label:
            service["PortLabel"] = port_label
        if "partition" in consul:
            service["Partition"] = consul["partition"]
        checks = consul.get("checks")
        if checks:
            service["Checks"] = [dict(c) for c in checks]
        return [service]
    return []


def _vault_block(vault: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not vault:
        return None
    out: dict[str, Any] = {}
    if "policies" in vault:
        out["Policies"] = list(vault["policies"])
    if "namespace" in vault:
        out["Namespace"] = vault["namespace"]
    if "role" in vault:
        out["Role"] = vault["role"]
    if "change_mode" in vault:
        out["ChangeMode"] = vault["change_mode"]
    return out or None


def _task(
    *,
    name: str,
    image: str,
    command: Sequence[str] | None,
    env_vars: Sequence[str] | Mapping[str, str] | None,
    resources: Mapping[str, Any] | None,
    vault: Mapping[str, Any] | None,
    mount_points: Sequence[Mapping[str, Any]] | None = None,
    extra_config: Mapping[str, Any] | None = None,
    port_labels: Sequence[str] | None = None,
) -> dict[str, Any]:
    # Set force_pull=false explicitly: matches the documented Nomad default,
    # but Nomad 2.0 has been observed to re-pull when unset, which breaks
    # cross-platform setups (e.g. amd64 image pre-pulled on an Apple-Silicon
    # daemon — fresh pull resolves the manifest list to arm64 and fails).
    # Callers can override via extra_config.
    config: dict[str, Any] = {"image": image, "force_pull": False}
    if command:
        config["args"] = list(command)
    if mount_points:
        config["mount"] = [dict(m) for m in mount_points]
    # Nomad's docker driver only publishes a network-stanza port to host
    # if the task config explicitly references its label here.
    if port_labels:
        config["ports"] = list(port_labels)
    if extra_config:
        config.update(extra_config)

    task: dict[str, Any] = {
        "Name": name,
        "Driver": "docker",
        "Config": config,
        "Env": _env_list_to_map(env_vars),
        "Resources": _resources_block(resources),
    }
    vault_spec = _vault_block(vault)
    if vault_spec:
        task["Vault"] = vault_spec
    return task


class NomadJobSpecBuilder:
    """Build a Nomad Job JSON dict for a Dagster code server or run.

    Kept as a class so callers can incrementally add sidecars / networks /
    templates before calling ``build()``. For simple cases prefer the
    ``build_run_job_spec`` / ``build_server_job_spec`` module-level helpers.
    """

    def __init__(
        self,
        *,
        job_id: str,
        job_type: str,
        name: str | None = None,
        namespace: str | None = None,
        region: str | None = None,
        datacenters: Sequence[str] | None = None,
        node_pool: str | None = None,
        meta: Mapping[str, str] | None = None,
        priority: int | None = None,
    ):
        if job_type not in ("batch", "service"):
            raise ValueError(f"Unsupported Nomad job type {job_type!r}")
        self._job: dict[str, Any] = {
            "ID": job_id,
            "Name": name or job_id,
            "Type": job_type,
            "Datacenters": list(datacenters) if datacenters else [],
            "TaskGroups": [],
        }
        if namespace:
            self._job["Namespace"] = namespace
        if region:
            self._job["Region"] = region
        if node_pool:
            self._job["NodePool"] = node_pool
        if meta:
            self._job["Meta"] = dict(meta)
        if priority is not None:
            self._job["Priority"] = int(priority)
        self._tasks: list[dict[str, Any]] = []
        self._services: list[dict[str, Any]] = []
        self._constraints: list[dict[str, Any]] = []
        self._affinities: list[dict[str, Any]] = []
        self._networks: list[dict[str, Any]] = []

    def add_task(self, task: Mapping[str, Any]) -> "NomadJobSpecBuilder":
        self._tasks.append(dict(task))
        return self

    def add_service(self, service: Mapping[str, Any]) -> "NomadJobSpecBuilder":
        self._services.append(dict(service))
        return self

    def add_constraint(self, constraint: Mapping[str, Any]) -> "NomadJobSpecBuilder":
        self._constraints.append(dict(constraint))
        return self

    def add_affinity(self, affinity: Mapping[str, Any]) -> "NomadJobSpecBuilder":
        self._affinities.append(dict(affinity))
        return self

    def add_network(self, network: Mapping[str, Any]) -> "NomadJobSpecBuilder":
        self._networks.append(dict(network))
        return self

    def build(self) -> dict[str, Any]:
        group: dict[str, Any] = {
            "Name": self._job["Name"],
            "Tasks": self._tasks,
        }
        if self._services:
            group["Services"] = self._services
        if self._constraints:
            group["Constraints"] = self._constraints
        if self._affinities:
            group["Affinities"] = self._affinities
        if self._networks:
            group["Networks"] = self._networks
        self._job["TaskGroups"] = [group]
        return self._job


def build_run_job_spec(
    *,
    job_id: str,
    image: str,
    command: Sequence[str],
    env_vars: Sequence[str] | Mapping[str, str] | None = None,
    namespace: str | None = None,
    region: str | None = None,
    datacenters: Sequence[str] | None = None,
    node_pool: str | None = None,
    resources: Mapping[str, Any] | None = None,
    constraints: Sequence[Mapping[str, Any]] | None = None,
    affinities: Sequence[Mapping[str, Any]] | None = None,
    vault: Mapping[str, Any] | None = None,
    consul: Mapping[str, Any] | None = None,
    meta: Mapping[str, str] | None = None,
    sidecar_tasks: Sequence[Mapping[str, Any]] | None = None,
    task_name: str = "run",
    mount_points: Sequence[Mapping[str, Any]] | None = None,
    extra_task_config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a Nomad ``batch`` job spec for a single Dagster run."""
    builder = NomadJobSpecBuilder(
        job_id=job_id,
        job_type="batch",
        namespace=namespace,
        region=region,
        datacenters=datacenters,
        node_pool=node_pool,
        meta=meta,
    )
    builder.add_task(
        _task(
            name=task_name,
            image=image,
            command=command,
            env_vars=env_vars,
            resources=resources,
            vault=vault,
            mount_points=mount_points,
            extra_config=extra_task_config,
        )
    )
    for sidecar in sidecar_tasks or []:
        builder.add_task(dict(sidecar))
    for constraint in _constraints(constraints):
        builder.add_constraint(constraint)
    for affinity in _affinities(affinities):
        builder.add_affinity(affinity)
    if consul:
        for service in _service_block(
            consul, provider="consul", port_label=None, default_name=job_id
        ):
            builder.add_service(service)
    return builder.build()


def build_server_job_spec(
    *,
    job_id: str,
    image: str,
    command: Sequence[str],
    grpc_port: int,
    env_vars: Sequence[str] | Mapping[str, str] | None = None,
    namespace: str | None = None,
    region: str | None = None,
    datacenters: Sequence[str] | None = None,
    node_pool: str | None = None,
    resources: Mapping[str, Any] | None = None,
    constraints: Sequence[Mapping[str, Any]] | None = None,
    affinities: Sequence[Mapping[str, Any]] | None = None,
    vault: Mapping[str, Any] | None = None,
    consul: Mapping[str, Any] | None = None,
    service_provider: str = "nomad",
    service_name: str | None = None,
    meta: Mapping[str, str] | None = None,
    sidecar_tasks: Sequence[Mapping[str, Any]] | None = None,
    task_name: str = "server",
    health_check: Mapping[str, Any] | None = None,
    mount_points: Sequence[Mapping[str, Any]] | None = None,
    extra_task_config: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a Nomad ``service`` job spec for a Dagster code server."""
    builder = NomadJobSpecBuilder(
        job_id=job_id,
        job_type="service",
        namespace=namespace,
        region=region,
        datacenters=datacenters,
        node_pool=node_pool,
        meta=meta,
    )
    builder.add_task(
        _task(
            name=task_name,
            image=image,
            command=command,
            env_vars=env_vars,
            resources=resources,
            vault=vault,
            mount_points=mount_points,
            extra_config=extra_task_config,
            port_labels=["grpc"],
        )
    )
    for sidecar in sidecar_tasks or []:
        builder.add_task(dict(sidecar))
    builder.add_network({"DynamicPorts": [{"Label": "grpc", "To": grpc_port}]})

    for constraint in _constraints(constraints):
        builder.add_constraint(constraint)
    for affinity in _affinities(affinities):
        builder.add_affinity(affinity)

    resolved_name = service_name or job_id
    for service in _service_block(
        consul,
        provider=service_provider,
        port_label="grpc",
        default_name=resolved_name,
    ):
        if health_check:
            service.setdefault("Checks", []).append(dict(health_check))
        builder.add_service(service)

    return builder.build()


def encode_dispatch_payload(lines: Sequence[str]) -> str:
    """Encode a payload for ``POST /v1/job/{id}/dispatch``.

    Nomad expects the ``Payload`` field to be a base64-encoded string; the
    parameterized job typically receives it at ``NOMAD_META_...`` or via a
    ``file`` payload. We join on newlines to match how Dagster's
    ``ExecuteRunArgs.get_command_args`` renders the run command.
    """
    return base64.b64encode("\n".join(lines).encode("utf-8")).decode("ascii")
