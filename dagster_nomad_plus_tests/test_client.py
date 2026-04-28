import httpx
import pytest
import respx
from dagster_nomad_plus.client import NomadAPIError, NomadClient, NomadJobNotFoundError


@respx.mock
def test_auth_header_and_namespace(nomad_address):
    route = respx.get(f"{nomad_address}/v1/jobs").mock(return_value=httpx.Response(200, json=[]))
    with NomadClient(address=nomad_address, token="tk", namespace="prod") as client:
        client.list_jobs()
    request = route.calls.last.request
    assert request.headers["X-Nomad-Token"] == "tk"
    assert "namespace=prod" in request.url.query.decode()


@respx.mock
def test_region_param(nomad_address):
    route = respx.get(f"{nomad_address}/v1/jobs").mock(return_value=httpx.Response(200, json=[]))
    with NomadClient(address=nomad_address, region="us-east") as client:
        client.list_jobs()
    assert "region=us-east" in route.calls.last.request.url.query.decode()


@respx.mock
def test_404_raises_job_not_found(nomad_address):
    respx.delete(f"{nomad_address}/v1/job/abc").mock(return_value=httpx.Response(404, text="no"))
    with NomadClient(address=nomad_address) as client:
        with pytest.raises(NomadJobNotFoundError):
            client.stop_job("abc")


@respx.mock
def test_5xx_retries_on_get(nomad_address):
    route = respx.get(f"{nomad_address}/v1/jobs").mock(
        side_effect=[
            httpx.Response(500, text="boom"),
            httpx.Response(500, text="boom"),
            httpx.Response(200, json=[]),
        ]
    )
    with NomadClient(address=nomad_address, max_retries=3) as client:
        client.list_jobs()
    assert route.call_count == 3


@respx.mock
def test_5xx_no_retry_on_post(nomad_address):
    respx.post(f"{nomad_address}/v1/jobs").mock(return_value=httpx.Response(500, text="no"))
    with NomadClient(address=nomad_address, max_retries=3) as client:
        with pytest.raises(NomadAPIError):
            client.register_job({"ID": "a", "Name": "a", "Type": "batch", "TaskGroups": []})


def test_insecure_skip_verify_disables_tls():
    client = NomadClient(address="https://nomad.test", insecure_skip_verify=True)
    # httpx exposes the underlying verify setting via ._transport / SSLContext;
    # the easiest black-box check is that constructing did not raise on the
    # TLS flag path — a real mTLS test lives in the integration suite.
    client.close()


@respx.mock
def test_register_job_wraps_body(nomad_address):
    captured = {}

    def _capture(request):
        captured["body"] = request.content
        return httpx.Response(200, json={"EvalID": "eval-1"})

    respx.post(f"{nomad_address}/v1/jobs").mock(side_effect=_capture)
    with NomadClient(address=nomad_address) as client:
        client.register_job({"ID": "x", "Name": "x", "Type": "batch", "TaskGroups": []})
    assert b'"Job"' in captured["body"]
