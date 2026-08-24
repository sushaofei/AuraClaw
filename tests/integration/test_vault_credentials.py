import asyncio
import os
from pathlib import Path

import pytest
from dotenv import dotenv_values

from auraclaw.config import get_settings
from auraclaw.infrastructure.credentials.vault import HashiCorpVault

ROOT = Path(__file__).resolve().parents[2]
DOTENV = dotenv_values(ROOT / ".env.dev")
SETTINGS = get_settings()


def _vault() -> HashiCorpVault:
    if (
        SETTINGS.credential_vault_addr is None
        or SETTINGS.credential_vault_token is None
    ):
        pytest.skip("Vault endpoint/token not configured")
    return HashiCorpVault(
        SETTINGS.credential_vault_addr,
        token=SETTINGS.credential_vault_token.get_secret_value(),
        mount=SETTINGS.credential_vault_mount,
    )


def test_vault_readiness() -> None:
    async def scenario() -> None:
        vault = _vault()
        try:
            ready, _detail = await vault.readiness()
            assert ready
        finally:
            await vault.aclose()

    asyncio.run(scenario())


def test_vault_resolves_configured_disposable_reference() -> None:
    credential_ref = os.getenv("TEST_VAULT_CREDENTIAL_REF") or DOTENV.get(
        "TEST_VAULT_CREDENTIAL_REF"
    )
    if not credential_ref:
        pytest.skip("TEST_VAULT_CREDENTIAL_REF not configured")
    credential_field = os.getenv("TEST_VAULT_CREDENTIAL_FIELD") or DOTENV.get(
        "TEST_VAULT_CREDENTIAL_FIELD"
    )
    if credential_field:
        credential_ref = f"{credential_ref}#{credential_field}"

    async def scenario() -> None:
        vault = _vault()
        try:
            value = await vault.resolve(credential_ref)
            assert isinstance(value, str)
            assert value
        finally:
            await vault.aclose()

    asyncio.run(scenario())
