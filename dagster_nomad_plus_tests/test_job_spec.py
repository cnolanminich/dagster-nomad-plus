from dagster_nomad_plus.job_spec import (
    build_run_job_spec,
    build_server_job_spec,
    encode_dispatch_payload,
)


def test_build_run_job_spec_basic():
    spec = build_run_job_spec(
        job_id="dagster-run-abc",
        image="myorg/dagster:1.0",
        command=["dagster", "api", "execute_run", "--payload", "..."],
        env_vars=["FOO=bar", "BAZ"],
        namespace="default",
        region="global",
        datacenters=["dc1"],
        resources={"cpu": 1000, "memory": 2048},
        meta={"dagster/run_id": "abc"},
    )
    assert spec["Type"] == "batch"
    assert spec["Namespace"] == "default"
    assert spec["Region"] == "global"
    assert spec["Meta"]["dagster/run_id"] == "abc"
    group = spec["TaskGroups"][0]
    task = group["Tasks"][0]
    assert task["Driver"] == "docker"
    assert task["Config"]["image"] == "myorg/dagster:1.0"
    assert task["Config"]["force_pull"] is False
    assert task["Env"]["FOO"] == "bar"
    assert "BAZ" in task["Env"]
    assert task["Resources"]["CPU"] == 1000
    assert task["Resources"]["MemoryMB"] == 2048


def test_build_run_job_spec_constraints_and_vault():
    spec = build_run_job_spec(
        job_id="dagster-run-xyz",
        image="img:latest",
        command=["dagster"],
        constraints=[{"attribute": "${attr.kernel.name}", "operator": "=", "value": "linux"}],
        affinities=[
            {"attribute": "${node.class}", "operator": "=", "value": "compute", "weight": 75}
        ],
        vault={"policies": ["dagster"], "namespace": "team", "role": "runs"},
    )
    group = spec["TaskGroups"][0]
    assert group["Constraints"][0]["LTarget"] == "${attr.kernel.name}"
    assert group["Constraints"][0]["RTarget"] == "linux"
    assert group["Affinities"][0]["Weight"] == 75
    vault_block = group["Tasks"][0]["Vault"]
    assert vault_block["Policies"] == ["dagster"]
    assert vault_block["Role"] == "runs"


def test_build_run_job_spec_with_consul_service():
    spec = build_run_job_spec(
        job_id="dagster-run-consul",
        image="img",
        command=["dagster"],
        consul={"service_name": "dagster-run", "tags": ["urgent"]},
    )
    services = spec["TaskGroups"][0]["Services"]
    assert services[0]["Name"] == "dagster-run"
    assert services[0]["Provider"] == "consul"
    assert services[0]["Tags"] == ["urgent"]


def test_build_server_job_spec_nomad_service_provider():
    spec = build_server_job_spec(
        job_id="loc-abc",
        image="loc:1",
        command=["dagster", "code-server", "start"],
        grpc_port=4000,
    )
    group = spec["TaskGroups"][0]
    assert spec["Type"] == "service"
    assert group["Networks"][0]["DynamicPorts"][0]["Label"] == "grpc"
    assert group["Services"][0]["Provider"] == "nomad"
    assert group["Services"][0]["PortLabel"] == "grpc"


def test_extra_task_config_can_override_force_pull():
    spec = build_run_job_spec(
        job_id="dagster-run-fp",
        image="img",
        command=["dagster"],
        extra_task_config={"force_pull": True},
    )
    task = spec["TaskGroups"][0]["Tasks"][0]
    assert task["Config"]["force_pull"] is True


def test_encode_dispatch_payload_is_base64():
    payload = encode_dispatch_payload(["dagster", "api", "execute_run"])
    # base64 encoded output of the joined command
    import base64

    assert base64.b64decode(payload).decode("utf-8") == "dagster\napi\nexecute_run"
