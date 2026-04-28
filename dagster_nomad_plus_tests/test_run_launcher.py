# ruff: noqa: SLF001

import httpx
import pytest
import respx
from dagster_nomad_plus.run_launcher import (
    NomadRunLauncher,
    _allocations_to_health,
    sanitize_job_id,
)


def test_sanitize_job_id_truncates_and_cleans():
    raw = "deployment/prod@location.foo bar"
    out = sanitize_job_id(raw)
    assert "/" not in out and "@" not in out and " " not in out
    assert len(out) <= 120


def test_construct_requires_parameterized_id_in_parameterized_mode():
    with pytest.raises(ValueError):
        NomadRunLauncher(address="http://n:4646", job_spec_mode="parameterized")


def test_construct_rejects_unknown_mode():
    with pytest.raises(ValueError):
        NomadRunLauncher(address="http://n:4646", job_spec_mode="bogus")


def test_allocations_to_health_picks_latest_and_maps_states():
    from dagster._core.launcher.base import WorkerStatus

    allocs = [
        {"CreateIndex": 1, "ClientStatus": "failed"},
        {"CreateIndex": 2, "ClientStatus": "running"},
    ]
    assert _allocations_to_health(allocs).status == WorkerStatus.RUNNING

    assert (
        _allocations_to_health([{"CreateIndex": 1, "ClientStatus": "complete"}]).status
        == WorkerStatus.SUCCESS
    )
    assert (
        _allocations_to_health([{"CreateIndex": 1, "ClientStatus": "lost"}]).status
        == WorkerStatus.FAILED
    )
    assert _allocations_to_health([]).status == WorkerStatus.UNKNOWN


@respx.mock
def test_check_run_worker_health_maps_404_to_not_found(monkeypatch):
    from dagster._core.launcher.base import WorkerStatus

    launcher = NomadRunLauncher(address="http://nomad:4646")
    respx.get("http://nomad:4646/v1/job/abc/allocations").mock(
        return_value=httpx.Response(404, text="gone")
    )

    class _Run:
        run_id = "r"
        tags = {"nomad/job_id": "abc"}

    result = launcher.check_run_worker_health(_Run())  # type: ignore[arg-type]
    assert result.status == WorkerStatus.NOT_FOUND


@respx.mock
def test_launch_run_programmatic_posts_job_json(monkeypatch):
    """Unit-level check that programmatic mode hits /v1/jobs with a Job body.

    We patch out the instance.add_run_tags / get_ref interactions and
    build a minimal run stub — goal is to verify the HTTP interaction
    and tag writes, not full Dagster run semantics (that is covered in
    the Cloud-side integration tests).
    """
    route = respx.post("http://nomad:4646/v1/jobs").mock(
        return_value=httpx.Response(200, json={"EvalID": "eval-123"})
    )

    launcher = NomadRunLauncher(address="http://nomad:4646", image="img:1")

    added_tags: dict[str, dict[str, str]] = {}

    class _Instance:
        def get_ref(self):
            return object()

        def add_run_tags(self, run_id, tags):
            added_tags[run_id] = tags

        def report_run_canceling(self, run):
            pass

        def get_run_by_id(self, run_id):
            return None

    fake_instance = _Instance()
    launcher._instance_weakref = lambda: fake_instance  # type: ignore[assignment]

    class _RepoOrigin:
        container_image = None
        container_context = None

    class _JobOrigin:
        repository_origin = _RepoOrigin()

        def get_command_args(self):  # pragma: no cover - unused
            return []

    class _Run:
        run_id = "run-abcdef1234"
        job_code_origin = _JobOrigin()
        tags: dict[str, str] = {}

    class _Context:
        dagster_run = _Run()
        job_code_origin = _JobOrigin()

    # Patch out ExecuteRunArgs since it requires a real instance ref
    monkeypatch.setattr(
        "dagster_nomad_plus.run_launcher.ExecuteRunArgs",
        lambda **kwargs: type("A", (), {"get_command_args": lambda self: ["dagster", "api"]})(),
    )

    launcher.launch_run(_Context())  # type: ignore[arg-type]

    assert route.call_count == 1
    body = route.calls.last.request.content
    assert b'"Job"' in body
    assert b'"Type":"batch"' in body
    assert "run-abcdef1234" in added_tags
    assert added_tags["run-abcdef1234"]["nomad/job_id"].startswith("dagster-run-")
