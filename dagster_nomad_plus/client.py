"""HTTP client for the HashiCorp Nomad API.

Thin wrapper over httpx.Client that:
- Sends the ACL token via X-Nomad-Token for every request (when configured)
- Applies region + namespace query params by default (Nomad treats these
  as request-scoped and overridable per call)
- Supports TLS/mTLS via CA bundle, client cert/key, and server name override
- Retries idempotent GETs on transient 5xx/connection errors
- Normalizes JSON encoding / decoding and maps Nomad API errors to a
  single ``NomadAPIError`` hierarchy
"""

import time
from collections.abc import Generator, Mapping
from contextlib import contextmanager
from typing import Any

import httpx

DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=30.0, pool=5.0)
DEFAULT_MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 0.25


class NomadAPIError(Exception):
    """Base class for Nomad HTTP API failures."""

    def __init__(self, message: str, status_code: int | None = None, body: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body


class NomadJobNotFoundError(NomadAPIError):
    """Raised when the requested Nomad job does not exist (404)."""


class NomadAuth(httpx.Auth):
    """Attaches the Nomad ACL token header when one is configured."""

    requires_request_body = False
    requires_response_body = False

    def __init__(self, token: str | None):
        self._token = token

    def auth_flow(self, request: httpx.Request) -> Generator[httpx.Request, httpx.Response, None]:
        if self._token:
            request.headers["X-Nomad-Token"] = self._token
        yield request


class NomadClient:
    """Wrapper around ``httpx.Client`` specialized for the Nomad HTTP API.

    Construct once per agent / launcher; call ``close()`` or use as a context
    manager to release the underlying connection pool.
    """

    def __init__(
        self,
        address: str,
        *,
        token: str | None = None,
        region: str | None = None,
        namespace: str | None = None,
        ca_cert: str | None = None,
        client_cert: str | None = None,
        client_key: str | None = None,
        server_name: str | None = None,
        insecure_skip_verify: bool = False,
        timeout: httpx.Timeout | float = DEFAULT_TIMEOUT,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ):
        if not address:
            raise ValueError("Nomad address is required")

        verify: str | bool
        if insecure_skip_verify:
            verify = False
        elif ca_cert:
            verify = ca_cert
        else:
            verify = True

        cert: tuple[str, str] | str | None = None
        if client_cert and client_key:
            cert = (client_cert, client_key)
        elif client_cert:
            cert = client_cert

        default_params: dict[str, str] = {}
        if region:
            default_params["region"] = region
        if namespace:
            default_params["namespace"] = namespace

        # ``server_name`` is accepted but not yet wired up: overriding TLS SNI
        # in httpx requires a custom transport with a custom SSLContext. Setting
        # the HTTP ``Host`` header (an earlier attempt) breaks Nomad routing.
        # See https://www.python-httpx.org/advanced/transports/ for the SNI hook.
        del server_name

        self._client = httpx.Client(
            base_url=address.rstrip("/"),
            auth=NomadAuth(token),
            verify=verify,
            cert=cert,
            headers={"Content-Type": "application/json"},
            params=default_params,
            timeout=timeout,
        )
        self._max_retries = max_retries

    # -- lifecycle --------------------------------------------------------

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "NomadClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # -- low-level --------------------------------------------------------

    def request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: Mapping[str, Any] | None = None,
    ) -> httpx.Response:
        method = method.upper()
        attempt = 0
        while True:
            try:
                response = self._client.request(method, path, json=json, params=params)
            except httpx.TransportError as err:
                if method == "GET" and attempt < self._max_retries:
                    time.sleep(RETRY_BACKOFF_BASE * (2**attempt))
                    attempt += 1
                    continue
                raise NomadAPIError(f"Nomad request failed: {err}") from err

            if response.status_code >= 500 and method == "GET" and attempt < self._max_retries:
                time.sleep(RETRY_BACKOFF_BASE * (2**attempt))
                attempt += 1
                continue

            if response.status_code == 404:
                raise NomadJobNotFoundError(
                    f"Nomad API returned 404 for {method} {path}",
                    status_code=404,
                    body=response.text,
                )
            if response.status_code >= 400:
                raise NomadAPIError(
                    f"Nomad API {method} {path} failed with {response.status_code}: {response.text}",
                    status_code=response.status_code,
                    body=response.text,
                )
            return response

    def get_json(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        return self.request("GET", path, params=params).json()

    def post_json(self, path: str, body: Any, params: Mapping[str, Any] | None = None) -> Any:
        response = self.request("POST", path, json=body, params=params)
        if response.content:
            return response.json()
        return None

    def delete(self, path: str, params: Mapping[str, Any] | None = None) -> Any:
        response = self.request("DELETE", path, params=params)
        if response.content:
            return response.json()
        return None

    # -- Nomad convenience wrappers --------------------------------------

    def register_job(self, job: Mapping[str, Any]) -> Mapping[str, Any]:
        """POST /v1/jobs — create or update a job.

        ``job`` should be the full Nomad job spec dict (no ``Job`` wrapper);
        this method wraps it for the API.
        """
        return self.post_json("/v1/jobs", {"Job": dict(job)})

    def dispatch_job(
        self,
        parameterized_job_id: str,
        *,
        payload: str | None = None,
        meta: Mapping[str, str] | None = None,
        id_prefix_template: str | None = None,
    ) -> Mapping[str, Any]:
        """POST /v1/job/{id}/dispatch — dispatch a parameterized job."""
        body: dict[str, Any] = {}
        if payload is not None:
            body["Payload"] = payload
        if meta:
            body["Meta"] = dict(meta)
        if id_prefix_template:
            body["IdPrefixTemplate"] = id_prefix_template
        return self.post_json(f"/v1/job/{parameterized_job_id}/dispatch", body)

    def stop_job(self, job_id: str, *, purge: bool = False) -> Mapping[str, Any]:
        """DELETE /v1/job/{id} — stop (and optionally purge) a job."""
        params: dict[str, Any] = {}
        if purge:
            params["purge"] = "true"
        return self.delete(f"/v1/job/{job_id}", params=params)

    def get_job(self, job_id: str) -> Mapping[str, Any]:
        return self.get_json(f"/v1/job/{job_id}")

    def list_jobs(self, prefix: str | None = None, meta: bool = False) -> list[Mapping[str, Any]]:
        params: dict[str, Any] = {}
        if prefix:
            params["prefix"] = prefix
        if meta:
            params["meta"] = "true"
        return self.get_json("/v1/jobs", params=params)

    def get_allocations(self, job_id: str) -> list[Mapping[str, Any]]:
        return self.get_json(f"/v1/job/{job_id}/allocations")

    def get_allocation(self, alloc_id: str) -> Mapping[str, Any]:
        return self.get_json(f"/v1/allocation/{alloc_id}")

    def get_service(self, service_name: str) -> list[Mapping[str, Any]]:
        """GET /v1/service/{name} — Nomad native service discovery."""
        return self.get_json(f"/v1/service/{service_name}")


@contextmanager
def nomad_client_from_config(config: Mapping[str, Any]) -> Generator[NomadClient, None, None]:
    """Build and auto-close a ``NomadClient`` from a launcher/agent config dict.

    Expected keys (all optional except ``address``): ``address``, ``token``,
    ``region``, ``namespace``, ``tls`` (with ``ca_cert``, ``client_cert``,
    ``client_key``, ``server_name``, ``insecure_skip_verify``).
    """
    tls = config.get("tls") or {}
    client = NomadClient(
        address=config["address"],
        token=config.get("token"),
        region=config.get("region"),
        namespace=config.get("namespace"),
        ca_cert=tls.get("ca_cert"),
        client_cert=tls.get("client_cert"),
        client_key=tls.get("client_key"),
        server_name=tls.get("server_name"),
        insecure_skip_verify=bool(tls.get("insecure_skip_verify", False)),
    )
    try:
        yield client
    finally:
        client.close()
