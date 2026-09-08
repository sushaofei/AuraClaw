import asyncio

import httpx
import pytest

from auraclaw.contracts.errors import CredentialAccessError
from auraclaw.infrastructure.credentials.vault import HashiCorpVault


def test_vault_kv_v2_resolve_and_readiness_do_not_expose_token() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v1/sys/health":
            return httpx.Response(200)
        if request.url.path == "/v1/auth/token/lookup-self":
            return httpx.Response(200, json={"data": {"ttl": 3600}})
        return httpx.Response(
            200,
            json={
                "data": {
                    "data": {
                        "value": "fake-value",
                        "password": "fake-password",
                    }
                }
            },
        )

    async def scenario() -> None:
        vault = HashiCorpVault(
            "https://vault.invalid",
            token="fake-token",
            mount="tenant-secrets",
            transport=httpx.MockTransport(handler),
        )
        try:
            assert await vault.readiness() == (
                True,
                "health HTTP 200; auth HTTP 200",
            )
            assert await vault.resolve("tenant/service") == "fake-value"
            assert await vault.resolve("tenant/service#password") == "fake-password"
        finally:
            await vault.aclose()

    asyncio.run(scenario())
    assert len(requests) == 4
    assert requests[2].url.raw_path == b"/v1/tenant-secrets/data/tenant%2Fservice"
    assert requests[3].url.raw_path == b"/v1/tenant-secrets/data/tenant%2Fservice"
    assert "X-Vault-Token" not in requests[0].headers
    assert all(
        request.headers["X-Vault-Token"] == "fake-token" for request in requests[1:]
    )
    assert all("fake-token" not in str(request.url) for request in requests)


def test_vault_readiness_rejects_expired_static_token() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/sys/health":
            return httpx.Response(200)
        return httpx.Response(403)

    async def scenario() -> None:
        vault = HashiCorpVault(
            "https://vault.invalid",
            token="expired-token",
            transport=httpx.MockTransport(handler),
        )
        try:
            assert await vault.readiness() == (
                False,
                "health HTTP 200; auth HTTP 403",
            )
        finally:
            await vault.aclose()

    asyncio.run(scenario())


def test_vault_approle_reauthenticates_after_token_expiry() -> None:
    issued = 0
    reads: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal issued
        if request.url.path == "/v1/auth/approle/login":
            issued += 1
            return httpx.Response(200, json={"auth": {"client_token": f"token-{issued}"}})
        if request.url.path == "/v1/auth/token/lookup-self":
            return httpx.Response(403)
        if request.url.raw_path == b"/v1/secret/data/vault%2Fservice":
            token = request.headers["X-Vault-Token"]
            reads.append(token)
            if token == "token-1":
                return httpx.Response(403)
            return httpx.Response(200, json={"data": {"data": {"value": "secret"}}})
        raise AssertionError(f"unexpected path: {request.url.path}")

    async def scenario() -> None:
        vault = HashiCorpVault(
            "https://vault.invalid",
            approle_role_id="role-id",
            approle_secret_id="secret-id",
            transport=httpx.MockTransport(handler),
        )
        try:
            assert await vault.resolve("vault/service") == "secret"
        finally:
            await vault.aclose()

    asyncio.run(scenario())
    assert issued == 2
    assert reads == ["token-1", "token-2"]


def test_vault_approle_does_not_reauthenticate_for_path_policy_denial() -> None:
    issued = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal issued
        if request.url.path == "/v1/auth/approle/login":
            issued += 1
            return httpx.Response(200, json={"auth": {"client_token": "valid-token"}})
        if request.url.path == "/v1/auth/token/lookup-self":
            return httpx.Response(200, json={"data": {"ttl": 3600}})
        return httpx.Response(403)

    async def scenario() -> None:
        vault = HashiCorpVault(
            "https://vault.invalid",
            approle_role_id="role-id",
            approle_secret_id="secret-id",
            transport=httpx.MockTransport(handler),
        )
        try:
            with pytest.raises(CredentialAccessError, match="unavailable or revoked"):
                await vault.resolve("forbidden/path")
        finally:
            await vault.aclose()

    asyncio.run(scenario())
    assert issued == 1
