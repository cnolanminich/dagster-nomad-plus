"""``NomadContainerContext`` — per-job overrides that merge instance config.

Modeled on ``dagster_aws.ecs.container_context.EcsContainerContext``. The
``NomadRunLauncher`` resolves a single context per run by layering:

    base run-launcher config  <-  location-level ``container_context.nomad``
                             <-  run tag overrides (future)

Precedence: right-hand entries win. Sequences concatenate so additive
fields (env vars, constraints, sidecars) compose across layers.
"""

from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any, NamedTuple

from dagster import (
    Array,
    Field,
    Permissive,
    Shape,
    StringSource,
    _check as check,
)
from dagster._config import process_config
from dagster._core.container_context import process_shared_container_context_config
from dagster._core.errors import DagsterInvalidConfigError
from dagster._core.storage.dagster_run import DagsterRun
from dagster._core.utils import parse_env_var

if TYPE_CHECKING:
    from dagster_nomad_plus.run_launcher import NomadRunLauncher


_RESOURCES = Permissive(
    {
        "cpu": Field(int, is_required=False, description="CPU in MHz."),
        "memory": Field(int, is_required=False, description="Memory in MiB."),
        "memory_max": Field(int, is_required=False, description="Max memory in MiB."),
        "cores": Field(int, is_required=False, description="Dedicated CPU cores."),
        "disk": Field(int, is_required=False, description="Ephemeral disk in MiB."),
    }
)

_CONSTRAINT = Shape(
    {
        "attribute": Field(StringSource, is_required=True),
        "operator": Field(StringSource, is_required=False, default_value="="),
        "value": Field(StringSource, is_required=False),
    }
)

_AFFINITY = Shape(
    {
        "attribute": Field(StringSource, is_required=True),
        "operator": Field(StringSource, is_required=False, default_value="="),
        "value": Field(StringSource, is_required=False),
        "weight": Field(int, is_required=False, default_value=50),
    }
)

_VAULT = Shape(
    {
        "policies": Field(Array(StringSource), is_required=False),
        "namespace": Field(StringSource, is_required=False),
        "role": Field(StringSource, is_required=False),
        "change_mode": Field(StringSource, is_required=False),
    }
)

_CONSUL = Shape(
    {
        "service_name": Field(StringSource, is_required=False),
        "tags": Field(Array(StringSource), is_required=False),
        "partition": Field(StringSource, is_required=False),
        "checks": Field(Array(Permissive()), is_required=False),
    }
)


NOMAD_CONTAINER_CONTEXT_SCHEMA = {
    "env_vars": Field(
        Array(StringSource),
        is_required=False,
        description=(
            "List of environment variable names to include in the Nomad task. "
            "Each can be of the form KEY=VALUE or just KEY (value pulled from the current process)."
        ),
    ),
    "meta": Field(
        Permissive(),
        is_required=False,
        description="Additional Nomad job meta to attach.",
    ),
    "resources": Field(_RESOURCES, is_required=False),
    "constraints": Field(Array(_CONSTRAINT), is_required=False),
    "affinities": Field(Array(_AFFINITY), is_required=False),
    "vault": Field(_VAULT, is_required=False),
    "consul": Field(_CONSUL, is_required=False),
    "mount_points": Field(Array(Permissive()), is_required=False),
    "sidecar_tasks": Field(Array(Permissive()), is_required=False),
}


class NomadContainerContext(
    NamedTuple(
        "_NomadContainerContext",
        [
            ("env_vars", Sequence[str]),
            ("meta", Mapping[str, str]),
            ("run_resources", Mapping[str, Any]),
            ("server_resources", Mapping[str, Any]),
            ("run_meta", Mapping[str, str]),
            ("server_meta", Mapping[str, str]),
            ("run_sidecar_tasks", Sequence[Mapping[str, Any]]),
            ("server_sidecar_tasks", Sequence[Mapping[str, Any]]),
            ("constraints", Sequence[Mapping[str, Any]]),
            ("affinities", Sequence[Mapping[str, Any]]),
            ("vault", Mapping[str, Any]),
            ("consul", Mapping[str, Any]),
            ("datacenters", Sequence[str]),
            ("node_pool", str | None),
            ("mount_points", Sequence[Mapping[str, Any]]),
            ("server_health_check", Mapping[str, Any] | None),
            ("server_service_name", str | None),
            ("service_provider", str),
        ],
    )
):
    """Resolved Nomad launch configuration for a Dagster code server or run."""

    def __new__(
        cls,
        env_vars: Sequence[str] | None = None,
        meta: Mapping[str, str] | None = None,
        run_resources: Mapping[str, Any] | None = None,
        server_resources: Mapping[str, Any] | None = None,
        run_meta: Mapping[str, str] | None = None,
        server_meta: Mapping[str, str] | None = None,
        run_sidecar_tasks: Sequence[Mapping[str, Any]] | None = None,
        server_sidecar_tasks: Sequence[Mapping[str, Any]] | None = None,
        constraints: Sequence[Mapping[str, Any]] | None = None,
        affinities: Sequence[Mapping[str, Any]] | None = None,
        vault: Mapping[str, Any] | None = None,
        consul: Mapping[str, Any] | None = None,
        datacenters: Sequence[str] | None = None,
        node_pool: str | None = None,
        mount_points: Sequence[Mapping[str, Any]] | None = None,
        server_health_check: Mapping[str, Any] | None = None,
        server_service_name: str | None = None,
        service_provider: str = "nomad",
    ):
        return super().__new__(
            cls,
            env_vars=check.opt_sequence_param(env_vars, "env_vars"),
            meta=check.opt_mapping_param(meta, "meta"),
            run_resources=check.opt_mapping_param(run_resources, "run_resources"),
            server_resources=check.opt_mapping_param(server_resources, "server_resources"),
            run_meta=check.opt_mapping_param(run_meta, "run_meta"),
            server_meta=check.opt_mapping_param(server_meta, "server_meta"),
            run_sidecar_tasks=check.opt_sequence_param(run_sidecar_tasks, "run_sidecar_tasks"),
            server_sidecar_tasks=check.opt_sequence_param(
                server_sidecar_tasks, "server_sidecar_tasks"
            ),
            constraints=check.opt_sequence_param(constraints, "constraints"),
            affinities=check.opt_sequence_param(affinities, "affinities"),
            vault=check.opt_mapping_param(vault, "vault"),
            consul=check.opt_mapping_param(consul, "consul"),
            datacenters=check.opt_sequence_param(datacenters, "datacenters"),
            node_pool=check.opt_str_param(node_pool, "node_pool"),
            mount_points=check.opt_sequence_param(mount_points, "mount_points"),
            server_health_check=check.opt_mapping_param(server_health_check, "server_health_check"),
            server_service_name=check.opt_str_param(server_service_name, "server_service_name"),
            service_provider=check.str_param(service_provider, "service_provider"),
        )

    def merge(self, other: "NomadContainerContext") -> "NomadContainerContext":
        return NomadContainerContext(
            env_vars=[*self.env_vars, *other.env_vars],
            meta={**self.meta, **other.meta},
            run_resources={**self.run_resources, **other.run_resources},
            server_resources={**self.server_resources, **other.server_resources},
            run_meta={**self.run_meta, **other.run_meta},
            server_meta={**self.server_meta, **other.server_meta},
            run_sidecar_tasks=[*self.run_sidecar_tasks, *other.run_sidecar_tasks],
            server_sidecar_tasks=[*self.server_sidecar_tasks, *other.server_sidecar_tasks],
            constraints=[*self.constraints, *other.constraints],
            affinities=[*self.affinities, *other.affinities],
            vault={**self.vault, **other.vault},
            consul={**self.consul, **other.consul},
            datacenters=list(other.datacenters) or list(self.datacenters),
            node_pool=other.node_pool or self.node_pool,
            mount_points=[*self.mount_points, *other.mount_points],
            server_health_check=other.server_health_check or self.server_health_check,
            server_service_name=other.server_service_name or self.server_service_name,
            service_provider=other.service_provider or self.service_provider,
        )

    def get_environment_dict(self) -> Mapping[str, str]:
        parsed = [parse_env_var(entry) for entry in self.env_vars]
        return {k: v for k, v in parsed}

    @staticmethod
    def create_for_run(
        dagster_run: DagsterRun, run_launcher: "NomadRunLauncher | None"
    ) -> "NomadContainerContext":
        context = NomadContainerContext()
        if run_launcher is not None:
            context = context.merge(
                NomadContainerContext(
                    env_vars=run_launcher.env_vars,
                    run_resources=run_launcher.run_resources,
                    server_resources=run_launcher.server_resources,
                    run_meta=run_launcher.run_meta,
                    server_meta=run_launcher.server_meta,
                    run_sidecar_tasks=run_launcher.run_sidecar_tasks,
                    server_sidecar_tasks=run_launcher.server_sidecar_tasks,
                    constraints=run_launcher.constraints,
                    affinities=run_launcher.affinities,
                    vault=run_launcher.vault,
                    consul=run_launcher.consul,
                    datacenters=run_launcher.datacenters,
                    node_pool=run_launcher.node_pool,
                    mount_points=run_launcher.mount_points,
                    server_health_check=run_launcher.server_health_check,
                    server_service_name=run_launcher.server_service_name,
                    service_provider=run_launcher.service_provider,
                )
            )

        run_container_context = (
            dagster_run.job_code_origin.repository_origin.container_context
            if dagster_run.job_code_origin
            else None
        )
        if not run_container_context:
            return context

        return context.merge(NomadContainerContext.create_from_config(run_container_context))

    @staticmethod
    def create_from_config(run_container_context: Mapping[str, Any]) -> "NomadContainerContext":
        processed_shared = process_shared_container_context_config(run_container_context or {})
        shared = NomadContainerContext(env_vars=processed_shared.get("env_vars", []))

        nomad_subconfig = run_container_context.get("nomad", {}) if run_container_context else {}
        if not nomad_subconfig:
            return shared

        processed = process_config(NOMAD_CONTAINER_CONTEXT_SCHEMA, nomad_subconfig)
        if not processed.success:
            raise DagsterInvalidConfigError(
                "Errors while parsing Nomad container context",
                processed.errors,
                nomad_subconfig,
            )
        value = processed.value or {}
        return shared.merge(
            NomadContainerContext(
                env_vars=value.get("env_vars"),
                meta=value.get("meta"),
                run_resources=value.get("resources"),
                constraints=value.get("constraints"),
                affinities=value.get("affinities"),
                vault=value.get("vault"),
                consul=value.get("consul"),
                mount_points=value.get("mount_points"),
                run_sidecar_tasks=value.get("sidecar_tasks"),
            )
        )
