import asyncio

import httpx

from auraclaw.infrastructure.credentials.vault import HashiCorpVault


def test_vault_kv_v2_resolve_and_readiness_do_not_expose_token() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/v1/sys/health":
            return httpx.Response(200)
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
            assert await vault.readiness() == (True, "HTTP 200")
            assert await vault.resolve("tenant/service") == "fake-value"
            assert await vault.resolve("tenant/service#password") == "fake-password"
        finally:
            await vault.aclose()

    asyncio.run(scenario())
    assert len(requests) == 3
    assert requests[1].url.raw_path == b"/v1/tenant-secrets/data/tenant%2Fservice"
    assert requests[2].url.raw_path == b"/v1/tenant-secrets/data/tenant%2Fservice"
    assert all(request.headers["X-Vault-Token"] == "fake-token" for request in requests)
    assert all("fake-token" not in str(request.url) for request in requests)
