"""``NomadRunLauncher`` — launches Dagster runs as Nomad jobs.

Supports two modes selected by ``job_spec_mode``:

- ``programmatic`` (default): builds a full Nomad Job JSON per run and
  POSTs to ``/v1/jobs``. Passes env, resources, constraints, Consul
  service registration, and Vault through from config. Parity with the
  ``EcsRunLauncher`` / ``K8sRunLauncher`` design.

- ``parameterized``: expects the operator to have pre-registered a Nomad
  *parameterized* HCL job; launching a run calls ``/v1/job/{id}/dispatch``
  with the Dagster run command as the payload and ``IMAGE``/``RUN_ID``
  meta. This is the approach used by the community ``PayLead/dagster-nomad-plus``
  and preserves HCL-as-source-of-truth workflows.

Both modes support ``terminate``, ``resume_run``, and
``check_run_worker_health`` over the same Nomad HTTP client.
"""

import logging
import re
import uuid
from collections.abc import Mapping, Sequence
from typing import Any

from dagster import (
    Array,
    BoolSource,
    Field,
    Noneable,
    Permissive,
    Shape,
    StringSource,
    _check as check,
)
from dagster._core.instance import T_DagsterInstance
from dagster._core.launcher.base import (
    CheckRunHealthResult,
    LaunchRunContext,
    RunLauncher,
    WorkerStatus,
)
from dagster._core.storage.dagster_run import DagsterRun
from dagster._grpc.types import ExecuteRunArgs, ResumeRunArgs
from dagster._serdes import ConfigurableClass
from dagster._serdes.config_class import ConfigurableClassData
from typing_extensions import Self

from dagster_nomad_plus.client import NomadAPIError, NomadClient, NomadJobNotFoundError
from dagster_nomad_plus.container_context import (
    NOMAD_CONTAINER_CONTEXT_SCHEMA,
    NomadContainerContext,
)
from dagster_nomad_plus.job_spec import build_run_job_spec, encode_dispatch_payload

_JOB_SPEC_MODES = ("programmatic", "parameterized")
_NOMAD_JOB_ID_TAG = "nomad/job_id"
_NOMAD_EVAL_ID_TAG = "nomad/eval_id"
_NOMAD_DISPATCHED_JOB_ID_TAG = "nomad/dispatched_job_id"

_MAX_JOB_ID_LEN = 120  # Nomad allows up to 128; leave slack for suffixes


def sanitize_job_id(raw: str) -> str:
    """Normalize a string to a valid Nomad job ID (alnum + ``-``/``_``/``.``)."""
    cleaned = re.sub(r"[^a-zA-Z0-9._-]+", "-", raw).strip("-") or "dagster-run"
    return cleaned[:_MAX_JOB_ID_LEN]


class NomadRunLauncher(RunLauncher[T_DagsterInstance], ConfigurableClass):
    """Launch Dagster runs as jobs on a HashiCorp Nomad cluster."""

    def __init__(
        self,
        address: str,
        inst_data: ConfigurableClassData | None = None,
        *,
        token: str | None = None,
        region: str | None = None,
        namespace: str | None = None,
        tls: Mapping[str, Any] | None = None,
        job_spec_mode: str = "programmatic",
        parameterized_job_id: str | None = None,
        job_id_prefix: str = "dagster-run",
        image: str | None = None,
        env_vars: Sequence[str] | None = None,
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
        docker_network: str | None = None,
    ):
        if job_spec_mode not in _JOB_SPEC_MODES:
            raise ValueError(
                f"job_spec_mode must be one of {_JOB_SPEC_MODES}, got {job_spec_mode!r}"
            )
        if job_spec_mode == "parameterized" and not parameterized_job_id:
            raise ValueError("parameterized_job_id is required when job_spec_mode='parameterized'")

        self._inst_data = inst_data
        self._address = address
        self._token = token
        self._region = region
        self._namespace = namespace
        self._tls = dict(tls or {})
        self._job_spec_mode = job_spec_mode
        self._parameterized_job_id = parameterized_job_id
        self._job_id_prefix = job_id_prefix
        self._image = image

        self.env_vars = list(env_vars or [])
        self.run_resources = dict(run_resources or {})
        self.server_resources = dict(server_resources or {})
        self.run_meta = dict(run_meta or {})
        self.server_meta = dict(server_meta or {})
        self.run_sidecar_tasks = list(run_sidecar_tasks or [])
        self.server_sidecar_tasks = list(server_sidecar_tasks or [])
        self.constraints = list(constraints or [])
        self.affinities = list(affinities or [])
        self.vault = dict(vault or {})
        self.consul = dict(consul or {})
        self.datacenters = list(datacenters or [])
        self.node_pool = node_pool
        self.mount_points = list(mount_points or [])
        self.server_health_check = dict(server_health_check) if server_health_check else None
        self.server_service_name = server_service_name
        self.service_provider = service_provider
        self.docker_network = docker_network

        super().__init__()

    # -- ConfigurableClass -----------------------------------------------

    @property
    def inst_data(self) -> ConfigurableClassData | None:
        return self._inst_data

    @classmethod
    def config_type(cls) -> dict[str, Field]:
        return {
            "address": Field(StringSource, is_required=True, description="Nomad HTTP API address."),
            "token": Field(StringSource, is_required=False, description="Nomad ACL token."),
            "region": Field(StringSource, is_required=False),
            "namespace": Field(StringSource, is_required=False),
            "tls": Field(
                Shape(
                    {
                        "ca_cert": Field(StringSource, is_required=False),
                        "client_cert": Field(StringSource, is_required=False),
                        "client_key": Field(StringSource, is_required=False),
                        "server_name": Field(StringSource, is_required=False),
                        "insecure_skip_verify": Field(
                            BoolSource, is_required=False, default_value=False
                        ),
                    }
                ),
                is_required=False,
            ),
            "job_spec_mode": Field(
                StringSource,
                is_required=False,
                default_value="programmatic",
                description="One of 'programmatic' or 'parameterized'.",
            ),
            "parameterized_job_id": Field(
                StringSource,
                is_required=False,
                description="Pre-registered parameterized Nomad job (parameterized mode only).",
            ),
            "job_id_prefix": Field(
                StringSource,
                is_required=False,
                default_value="dagster-run",
                description="Prefix for per-run Nomad job IDs (programmatic mode).",
            ),
            "image": Field(
                StringSource,
                is_required=False,
                description="Default Docker image for runs that do not set container_image on their job origin.",
            ),
            **NOMAD_CONTAINER_CONTEXT_SCHEMA,
            "run_resources": NOMAD_CONTAINER_CONTEXT_SCHEMA["resources"],
            "server_resources": Field(
                Permissive(), is_required=False, description="Default resources for code servers."
            ),
            "run_meta": Field(Permissive(), is_required=False),
            "server_meta": Field(Permissive(), is_required=False),
            "run_sidecar_tasks": Field(Array(Permissive()), is_required=False),
            "server_sidecar_tasks": Field(Array(Permissive()), is_required=False),
            "datacenters": Field(Array(StringSource), is_required=False),
            "node_pool": Field(StringSource, is_required=False),
            "server_health_check": Field(Noneable(Permissive()), is_required=False),
            "server_service_name": Field(StringSource, is_required=False),
            "service_provider": Field(StringSource, is_required=False, default_value="nomad"),
            "docker_network": Field(
                StringSource,
                is_required=False,
                description=(
                    "Docker network name to attach launched containers to. Use this in dev "
                    "setups where the agent and Nomad share a Docker bridge network."
                ),
            ),
        }

    @classmethod
    def from_config_value(
        cls, inst_data: ConfigurableClassData, config_value: Mapping[str, Any]
    ) -> Self:
        launcher_kwargs = dict(config_value)
        # ``resources`` in NOMAD_CONTAINER_CONTEXT_SCHEMA maps to run_resources
        # at the top level; drop duplicates.
        launcher_kwargs.pop("resources", None)
        launcher_kwargs.pop("meta", None)
        launcher_kwargs.pop("sidecar_tasks", None)
        launcher_kwargs.pop("mount_points_container_context", None)
        return cls(inst_data=inst_data, **launcher_kwargs)

    # -- RunLauncher -----------------------------------------------------

    @property
    def supports_check_run_worker_health(self) -> bool:
        return True

    @property
    def supports_resume_run(self) -> bool:
        return True

    def _client(self) -> NomadClient:
        tls = self._tls
        return NomadClient(
            address=self._address,
            token=self._token,
            region=self._region,
            namespace=self._namespace,
            ca_cert=tls.get("ca_cert"),
            client_cert=tls.get("client_cert"),
            client_key=tls.get("client_key"),
            server_name=tls.get("server_name"),
            insecure_skip_verify=bool(tls.get("insecure_skip_verify", False)),
        )

    def _resolve_image(self, run: DagsterRun) -> str:
        origin_image = None
        if run.job_code_origin:
            origin_image = run.job_code_origin.repository_origin.container_image
        image = origin_image or self._image
        if not image:
            raise RuntimeError(
                f"No Docker image resolved for run {run.run_id}: neither container_image on the "
                "job origin nor launcher.image is configured."
            )
        return image

    def _launch_args_for(self, run: DagsterRun, resume: bool) -> list[str]:
        instance_ref = self._instance.get_ref()
        job_origin = check.not_none(run.job_code_origin)
        if resume:
            return ResumeRunArgs(
                job_origin=job_origin, run_id=run.run_id, instance_ref=instance_ref
            ).get_command_args()
        return ExecuteRunArgs(
            job_origin=job_origin, run_id=run.run_id, instance_ref=instance_ref
        ).get_command_args()

    def _launch_common(self, context: LaunchRunContext, resume: bool) -> None:
        run = context.dagster_run
        image = self._resolve_image(run)
        command = self._launch_args_for(run, resume=resume)
        container_context = NomadContainerContext.create_for_run(run, self)

        with self._client() as client:
            if self._job_spec_mode == "programmatic":
                job_id = sanitize_job_id(
                    f"{self._job_id_prefix}-{run.run_id[:8]}-{uuid.uuid4().hex[:6]}"
                )
                spec = build_run_job_spec(
                    job_id=job_id,
                    image=image,
                    command=command,
                    env_vars=container_context.env_vars,
                    namespace=self._namespace,
                    region=self._region,
                    datacenters=container_context.datacenters,
                    node_pool=container_context.node_pool,
                    resources=container_context.run_resources or self.run_resources,
                    constraints=container_context.constraints,
                    affinities=container_context.affinities,
                    vault=container_context.vault or None,
                    consul=container_context.consul or None,
                    meta={
                        "dagster/run_id": run.run_id,
                        "dagster/resume": "true" if resume else "false",
                        **container_context.meta,
                        **container_context.run_meta,
                    },
                    sidecar_tasks=container_context.run_sidecar_tasks,
                    mount_points=container_context.mount_points,
                    extra_task_config=(
                        {"network_mode": self.docker_network} if self.docker_network else None
                    ),
                )
                response = client.register_job(spec)
                self._instance.add_run_tags(
                    run.run_id,
                    {
                        _NOMAD_JOB_ID_TAG: job_id,
                        _NOMAD_EVAL_ID_TAG: str(response.get("EvalID", "")),
                    },
                )
            else:
                parameterized = check.not_none(self._parameterized_job_id)
                payload = encode_dispatch_payload(command)
                response = client.dispatch_job(
                    parameterized,
                    payload=payload,
                    meta={
                        "IMAGE": image,
                        "RUN_ID": run.run_id,
                        "RESUME": "true" if resume else "false",
                    },
                )
                dispatched = str(response.get("DispatchedJobID", ""))
                self._instance.add_run_tags(
                    run.run_id,
                    {
                        _NOMAD_DISPATCHED_JOB_ID_TAG: dispatched,
                        _NOMAD_JOB_ID_TAG: dispatched,
                        _NOMAD_EVAL_ID_TAG: str(response.get("EvalID", "")),
                    },
                )

        logging.info(
            "Launched Dagster run %s on Nomad (mode=%s, image=%s)",
            run.run_id,
            self._job_spec_mode,
            image,
        )

    def launch_run(self, context: LaunchRunContext) -> None:
        self._launch_common(context, resume=False)

    def resume_run(self, context: LaunchRunContext) -> None:
        self._launch_common(context, resume=True)

    def terminate(self, run_id: str) -> bool:
        run = self._instance.get_run_by_id(run_id)
        if run is None or run.is_finished:
            return False

        job_id = run.tags.get(_NOMAD_JOB_ID_TAG) or run.tags.get(_NOMAD_DISPATCHED_JOB_ID_TAG)
        if not job_id:
            return False

        self._instance.report_run_canceling(run)
        try:
            with self._client() as client:
                client.stop_job(job_id, purge=False)
        except NomadJobNotFoundError:
            return False
        except NomadAPIError as err:
            logging.warning("Failed to terminate Nomad job %s for run %s: %s", job_id, run_id, err)
            return False
        return True

    def check_run_worker_health(self, run: DagsterRun) -> CheckRunHealthResult:
        job_id = run.tags.get(_NOMAD_JOB_ID_TAG) or run.tags.get(_NOMAD_DISPATCHED_JOB_ID_TAG)
        if not job_id:
            return CheckRunHealthResult(WorkerStatus.UNKNOWN, "No Nomad job tag on run")
        try:
            with self._client() as client:
                allocs = list(client.get_allocations(job_id))
        except NomadJobNotFoundError:
            return CheckRunHealthResult(WorkerStatus.NOT_FOUND, f"Nomad job {job_id} not found")
        except NomadAPIError as err:
            return CheckRunHealthResult(WorkerStatus.UNKNOWN, f"Nomad API error: {err}")
        return _allocations_to_health(allocs)


def _allocations_to_health(
    allocations: Sequence[Mapping[str, Any]],
) -> CheckRunHealthResult:
    if not allocations:
        return CheckRunHealthResult(WorkerStatus.UNKNOWN, "No Nomad allocations")

    # Latest allocation wins.
    alloc = max(allocations, key=lambda a: int(a.get("CreateIndex", 0)))
    client_status = alloc.get("ClientStatus")
    if client_status == "running":
        return CheckRunHealthResult(WorkerStatus.RUNNING)
    if client_status == "complete":
        return CheckRunHealthResult(WorkerStatus.SUCCESS)
    if client_status in ("failed", "lost"):
        reason = alloc.get("ClientDescription") or client_status
        return CheckRunHealthResult(WorkerStatus.FAILED, f"Allocation {client_status}: {reason}")
    if client_status in ("pending",):
        # WorkerStatus has no explicit STARTING; UNKNOWN keeps the run
        # monitor patient until the alloc transitions.
        return CheckRunHealthResult(WorkerStatus.UNKNOWN, "Allocation pending")
    return CheckRunHealthResult(
        WorkerStatus.UNKNOWN, f"Unknown Nomad client_status {client_status!r}"
    )
