from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from auraclaw.composition.identity import build_identity_verifier
from auraclaw.config import Settings, get_settings
from auraclaw.contracts.capabilities import (
    CapabilityStatus,
    CapabilityTrustLevel,
    McpAuthStrategy,
    McpServerDefinition,
)
from auraclaw.contracts.errors import PolicyDeniedError
from auraclaw.contracts.hands import HandsTrustedContext
from auraclaw.contracts.identity import (
    IdentityAuthenticationError,
    IdentityAuthorizationError,
    IdentityErrorReason,
    IdentityVerificationRequest,
    TrustedUserContext,
    VerifiedIdentityEnvelope,
    assertion_jti_digest,
)
from auraclaw.infrastructure.connectors.mcp.transport import ManagedRemoteMcpTransport
from auraclaw.infrastructure.connectors.mcp.wire import McpJsonRpcRequest
from auraclaw.infrastructure.credentials.mcp_egress import ManagedMcpEgressAdapter
from auraclaw.infrastructure.identity import (
    AgentContextSigner,
    DatabaseAssertionReplayGuard,
    DevelopmentHeaderIdentityVerifier,
    SignedAgentContextVerifier,
)
from auraclaw.main import create_app

SIGNING_KEY = b"chaintower-agent-context-signing-key-01"
WORKLOAD = "chaintower-workload-token-value"


class _SharedReplayPool:
    def __init__(self) -> None:
        self.records: dict[str, dict[str, object]] = {}

    async def execute(self, query: str, *args: object) -> str:
        if query.lstrip().startswith("DELETE"):
            return "DELETE 0"
        if query.lstrip().startswith("INSERT"):
            self.records.setdefault(
                str(args[0]), {"command_id": str(args[1]), "expires_at": args[2]}
            )
            return "INSERT 0 1"
        raise AssertionError(query)

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        assert query.lstrip().startswith("SELECT")
        return self.records.get(str(args[0]))


class _TestDatabaseReplayGuard(DatabaseAssertionReplayGuard):
    def __init__(self, pool: _SharedReplayPool) -> None:
        super().__init__("postgresql+asyncpg://unused/unused")
        self._test_pool = pool

    async def pool(self) -> Any:
        return self._test_pool


def _claims(**overrides: object) -> dict[str, object]:
    now = int(datetime.now(UTC).timestamp())
    payload: dict[str, object] = {
        "iss": "chaintower",
        "aud": "auraclaw-task-api",
        "tenant_id": "1",
        "user_id": "101",
        "scopes": ["agent.task.invoke"],
        "iat": now,
        "exp": now + 120,
        "jti": "jti-1",
        "kid": "k1",
    }
    payload.update(overrides)
    return payload


def _signer() -> AgentContextSigner:
    return AgentContextSigner(key_id="k1", signing_key=SIGNING_KEY)


def _verifier() -> SignedAgentContextVerifier:
    return SignedAgentContextVerifier(
        workload_tokens={WORKLOAD: "chaintower"},
        keys={"k1": SIGNING_KEY, "k0": b"chaintower-agent-context-signing-key-00"},
    )


def test_identity_contracts_forbid_extra_fields() -> None:
    with pytest.raises(ValidationError):
        TrustedUserContext(tenant_id="1", user_id="101", access_token="secret")  # type: ignore[call-arg]


def test_verified_envelope_repr_and_dump_omit_raw_assertion() -> None:
    envelope = VerifiedIdentityEnvelope.model_validate(
        {
            "caller": {"kind": "chaintower_workload", "subject": "chaintower"},
            "user": {"tenant_id": "1", "user_id": "101"},
        }
    )
    dumped = envelope.model_dump()
    rendered = repr(envelope)
    assert "Bearer" not in rendered
    assert "assertion" not in dumped or dumped.get("assertion") is None
    request = IdentityVerificationRequest(
        workload_credential=f"Bearer {WORKLOAD}",
        assertion="raw-secret-assertion",
    )
    assert request.model_dump()["assertion"] == "[REDACTED]"
    assert request.model_dump()["workload_credential"] == "[REDACTED]"
    assert "raw-secret-assertion" not in repr(request)


def test_explicit_insecure_headers_accept_tenant_actor_headers() -> None:
    get_settings().storage_backend = "memory"
    get_settings().allow_insecure_identity_headers = True
    with TestClient(create_app(profile="task-api")) as client:
        created = client.post(
            "/v1/tasks",
            headers={"Idempotency-Key": "id-char-1", "X-Tenant-ID": "tenant-dev"},
            json={"goal": "characterization"},
        )
        assert created.status_code == 202
        session_id = created.json()["session_id"]
        denied = client.get(
            f"/v1/tasks/{session_id}",
            headers={"X-Tenant-ID": "other-tenant"},
        )
        assert denied.status_code == 404
        missing = client.post(
            "/v1/tasks",
            headers={"Idempotency-Key": "id-char-default"},
            json={"goal": "defaults"},
        )
        assert missing.status_code == 202


def test_tenant_actor_headers_require_explicit_flag() -> None:
    get_settings().storage_backend = "memory"
    get_settings().allow_insecure_identity_headers = False
    with TestClient(create_app(profile="task-api")) as client:
        created = client.post(
            "/v1/tasks",
            headers={"Idempotency-Key": "id-char-required", "X-Tenant-ID": "tenant-dev"},
            json={"goal": "requires signed identity"},
        )
        assert created.status_code == 401


def test_production_rejects_insecure_identity_header_flag() -> None:
    with pytest.raises(ValidationError, match="insecure identity headers"):
        Settings(
            _env_file=None,
            deployment_profile="production",
            allow_insecure_identity_headers=True,
        )


def test_signed_verifier_covers_failure_matrix() -> None:
    async def scenario() -> None:
        verifier = _verifier()
        token = _signer().sign(_claims())
        allowed = await verifier.verify(
            IdentityVerificationRequest(
                workload_credential=f"Bearer {WORKLOAD}",
                assertion=token,
                command_id="cmd-1",
                operation="write",
            )
        )
        assert allowed.user.tenant_id == "1"
        assert allowed.user.user_id == "101"
        assert allowed.user.dept_id is None

        with_dept = await verifier.verify(
            IdentityVerificationRequest(
                workload_credential=f"Bearer {WORKLOAD}",
                assertion=_signer().sign(_claims(dept_id="9", jti="jti-dept")),
                declared_dept_id="9",
                command_id="cmd-dept",
                operation="write",
            )
        )
        assert with_dept.user.dept_id == "9"
        with pytest.raises(IdentityAuthorizationError) as dept_conflict:
            await verifier.verify(
                IdentityVerificationRequest(
                    workload_credential=f"Bearer {WORKLOAD}",
                    assertion=_signer().sign(_claims(dept_id="9", jti="jti-dept-conflict")),
                    declared_dept_id="8",
                )
            )
        assert dept_conflict.value.reason is IdentityErrorReason.TENANT_SESSION_MISMATCH
        assert allowed.assertion is not None
        assert assertion_jti_digest(allowed.assertion.jti)

        with pytest.raises(IdentityAuthenticationError) as missing:
            await verifier.verify(
                IdentityVerificationRequest(
                    workload_credential=f"Bearer {WORKLOAD}",
                    operation="write",
                    command_id="cmd-2",
                )
            )
        assert missing.value.reason is IdentityErrorReason.MISSING_CREDENTIAL

        with pytest.raises(IdentityAuthenticationError) as workload:
            await verifier.verify(
                IdentityVerificationRequest(
                    workload_credential="Bearer wrong",
                    assertion=token,
                )
            )
        assert workload.value.reason is IdentityErrorReason.WORKLOAD_MISMATCH

        with pytest.raises(IdentityAuthenticationError) as issuer:
            await verifier.verify(
                IdentityVerificationRequest(
                    workload_credential=f"Bearer {WORKLOAD}",
                    assertion=_signer().sign(_claims(iss="other")),
                )
            )
        assert issuer.value.reason is IdentityErrorReason.ISSUER_MISMATCH

        with pytest.raises(IdentityAuthenticationError) as audience:
            await verifier.verify(
                IdentityVerificationRequest(
                    workload_credential=f"Bearer {WORKLOAD}",
                    assertion=_signer().sign(_claims(aud="other-api")),
                )
            )
        assert audience.value.reason is IdentityErrorReason.AUDIENCE_MISMATCH

        with pytest.raises(IdentityAuthenticationError) as kid:
            await verifier.verify(
                IdentityVerificationRequest(
                    workload_credential=f"Bearer {WORKLOAD}",
                    assertion=AgentContextSigner(
                        key_id="unknown", signing_key=SIGNING_KEY
                    ).sign(_claims()),
                )
            )
        assert kid.value.reason is IdentityErrorReason.INVALID_SIGNATURE

        expired = int(datetime.now(UTC).timestamp()) - 120
        with pytest.raises(IdentityAuthenticationError) as expiry:
            await verifier.verify(
                IdentityVerificationRequest(
                    workload_credential=f"Bearer {WORKLOAD}",
                    assertion=_signer().sign(_claims(iat=expired - 60, exp=expired)),
                )
            )
        assert expiry.value.reason is IdentityErrorReason.EXPIRED

        with pytest.raises(IdentityAuthorizationError) as scope:
            await verifier.verify(
                IdentityVerificationRequest(
                    workload_credential=f"Bearer {WORKLOAD}",
                    assertion=_signer().sign(_claims(scopes=["other.scope"])),
                )
            )
        assert scope.value.reason is IdentityErrorReason.SCOPE_DENIED

        with pytest.raises(IdentityAuthorizationError) as tenant:
            await verifier.verify(
                IdentityVerificationRequest(
                    workload_credential=f"Bearer {WORKLOAD}",
                    assertion=token,
                    declared_tenant_id="2",
                )
            )
        assert tenant.value.reason is IdentityErrorReason.TENANT_SESSION_MISMATCH

        bound = _signer().sign(_claims(jti="jti-session", session_id="ses-1"))
        with pytest.raises(IdentityAuthorizationError) as session:
            await verifier.verify(
                IdentityVerificationRequest(
                    workload_credential=f"Bearer {WORKLOAD}",
                    assertion=bound,
                    bound_session_id="ses-2",
                )
            )
        assert session.value.reason is IdentityErrorReason.TENANT_SESSION_MISMATCH

        with pytest.raises(IdentityAuthorizationError) as unbound_session:
            await verifier.verify(
                IdentityVerificationRequest(
                    workload_credential=f"Bearer {WORKLOAD}",
                    assertion=_signer().sign(_claims(jti="jti-unbound-session")),
                    bound_session_id="ses-1",
                )
            )
        assert (
            unbound_session.value.reason
            is IdentityErrorReason.TENANT_SESSION_MISMATCH
        )

        replay_token = _signer().sign(_claims(jti="jti-replay"))
        await verifier.verify(
            IdentityVerificationRequest(
                workload_credential=f"Bearer {WORKLOAD}",
                assertion=replay_token,
                command_id="cmd-a",
                operation="write",
            )
        )
        with pytest.raises(IdentityAuthenticationError) as replayed:
            await verifier.verify(
                IdentityVerificationRequest(
                    workload_credential=f"Bearer {WORKLOAD}",
                    assertion=replay_token,
                    command_id="cmd-b",
                    operation="write",
                )
            )
        assert replayed.value.reason is IdentityErrorReason.REPLAYED

        rotated = AgentContextSigner(
            key_id="k0",
            signing_key=b"chaintower-agent-context-signing-key-00",
        ).sign(_claims(jti="jti-rotated", kid="k0"))
        previous = await verifier.verify(
            IdentityVerificationRequest(
                workload_credential=f"Bearer {WORKLOAD}",
                assertion=rotated,
            )
        )
        assert previous.assertion is not None
        assert previous.assertion.key_id == "k0"

    asyncio.run(scenario())


def test_database_replay_guard_rejects_cross_replica_command_reuse() -> None:
    async def scenario() -> None:
        shared_pool = _SharedReplayPool()
        first = _TestDatabaseReplayGuard(shared_pool)
        second = _TestDatabaseReplayGuard(shared_pool)
        expires_at = int(datetime.now(UTC).timestamp()) + 120
        await first.remember_write("shared-jti", "cmd-a", expires_at=expires_at)
        await second.remember_write("shared-jti", "cmd-a", expires_at=expires_at)
        with pytest.raises(IdentityAuthenticationError) as replayed:
            await second.remember_write(
                "shared-jti", "cmd-b", expires_at=expires_at
            )
        assert replayed.value.reason is IdentityErrorReason.REPLAYED

    asyncio.run(scenario())


def test_development_header_adapter_is_explicit() -> None:
    async def scenario() -> None:
        verifier = DevelopmentHeaderIdentityVerifier()
        envelope = await verifier.verify(IdentityVerificationRequest())
        assert envelope.user.tenant_id == "local"
        assert envelope.user.user_id == "local-user"
        settings = Settings(
            _env_file=None,
            deployment_profile="development",
            allow_insecure_identity_headers=True,
        )
        assert isinstance(
            build_identity_verifier(settings), DevelopmentHeaderIdentityVerifier
        )
        implicit = Settings(_env_file=None, deployment_profile="development")
        assert implicit.insecure_identity_headers_enabled is False
        assert isinstance(
            build_identity_verifier(implicit), SignedAgentContextVerifier
        )
        production = Settings(
            _env_file=None,
            deployment_profile="production",
            chaintower_workload_token=WORKLOAD,
            agent_context_signing_keys_json='{"k1":"chaintower-agent-context-signing-key-01"}',
        )
        assert isinstance(
            build_identity_verifier(production), SignedAgentContextVerifier
        )
        uplink = Settings(
            _env_file=None,
            deployment_profile="production",
            test_uplink_insecure_identity=True,
            chaintower_workload_token=WORKLOAD,
            agent_context_signing_keys_json='{"k1":"chaintower-agent-context-signing-key-01"}',
        )
        assert uplink.insecure_identity_headers_enabled is True
        assert isinstance(
            build_identity_verifier(uplink), DevelopmentHeaderIdentityVerifier
        )

    asyncio.run(scenario())


def test_production_task_api_requires_signed_context() -> None:
    from pydantic import SecretStr

    get_settings().storage_backend = "memory"
    app = create_app(profile="task-api")
    app.state.identity_verifier = build_identity_verifier(
        Settings(
            _env_file=None,
            deployment_profile="production",
            storage_backend="memory",
            chaintower_workload_token=SecretStr(WORKLOAD),
            agent_context_signing_keys_json='{"k1":"chaintower-agent-context-signing-key-01"}',
        )
    )
    token = _signer().sign(_claims(tenant_id="tenant-1", user_id="user-1", jti="api-1"))
    with TestClient(app) as client:
        denied = client.post(
            "/v1/tasks",
            headers={"Idempotency-Key": "prod-header-only", "X-Tenant-ID": "tenant-1"},
            json={"goal": "spoof"},
        )
        assert denied.status_code == 401
        created = client.post(
            "/v1/tasks",
            headers={
                "Idempotency-Key": "prod-signed-1",
                "Authorization": f"Bearer {WORKLOAD}",
                "X-CT-Agent-Context": token,
            },
            json={"goal": "trusted"},
        )
        assert created.status_code == 202
        session_id = created.json()["session_id"]
        forged = client.get(
            f"/v1/tasks/{session_id}",
            headers={
                "Authorization": f"Bearer {WORKLOAD}",
                "X-CT-Agent-Context": _signer().sign(
                    _claims(
                        tenant_id="tenant-2",
                        user_id="user-1",
                        session_id=session_id,
                        jti="api-2",
                    )
                ),
            },
        )
        assert forged.status_code == 404
        conflict = client.get(
            f"/v1/tasks/{session_id}",
            headers={
                "Authorization": f"Bearer {WORKLOAD}",
                "X-CT-Agent-Context": token,
                "X-Tenant-ID": "tenant-2",
            },
        )
        assert conflict.status_code == 403
        dept_conflict = client.post(
            "/v1/tasks",
            headers={
                "Idempotency-Key": "prod-signed-dept",
                "Authorization": f"Bearer {WORKLOAD}",
                "X-CT-Agent-Context": _signer().sign(
                    _claims(
                        tenant_id="tenant-1",
                        user_id="user-1",
                        dept_id="9",
                        jti="api-dept",
                    )
                ),
                "X-Dept-ID": "8",
            },
            json={"goal": "trusted"},
        )
        assert dept_conflict.status_code == 403


def test_mcp_workload_trusted_context_does_not_require_oauth() -> None:
    server = McpServerDefinition(
        server_id="chaintower-mcp",
        tenant_id="1",
        title="chaintower",
        endpoint="https://mcp.chaintower.example/mcp",
        credential_ref="vault/chaintower-mcp#workload",
        auth_strategy=McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT,
        trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
        allowed_tool_prefixes=("order.",),
        allowed_resource_schemes=("order",),
        allowed_prompt_prefixes=("order.",),
        status=CapabilityStatus.ACTIVE,
        enabled=True,
    )
    assert server.resolved_auth_strategy is McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT
    adapter = ManagedMcpEgressAdapter(server)
    assert adapter.credential_scope == "https://mcp.chaintower.example"


class _AllowPolicy:
    async def evaluate_action(self, **arguments: object) -> object:
        from auraclaw.action.ports import PolicyEvaluation
        from auraclaw.contracts.tools import PolicyDecision

        del arguments
        return PolicyEvaluation(
            decision=PolicyDecision.ALLOW,
            decision_id="policy-1",
            policy_version="v1",
        )


def test_mcp_transport_rejects_argument_identity_override_and_missing_user() -> None:
    async def scenario() -> None:
        calls: list[dict[str, object]] = []

        class Credentials:
            async def invoke(self, **arguments: object) -> dict[str, object]:
                calls.append(arguments)
                return {"jsonrpc": "2.0", "id": arguments["request"]["id"], "result": {}}  # type: ignore[index]

            def redact(self, value: object) -> object:
                return value

        server = McpServerDefinition(
            server_id="chaintower-mcp",
            tenant_id="1",
            title="chaintower",
            endpoint="https://mcp.chaintower.example/mcp",
            credential_ref="vault/chaintower-mcp#workload",
            auth_strategy=McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT,
            trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
            allowed_tool_prefixes=("order.",),
            allowed_resource_schemes=("order",),
            allowed_prompt_prefixes=("order.",),
            status=CapabilityStatus.ACTIVE,
            enabled=True,
        )
        transport = ManagedRemoteMcpTransport(
            server, credentials=Credentials(), policy=_AllowPolicy()
        )
        trusted = HandsTrustedContext(
            tenant_id="1",
            root_session_id="root",
            session_id="ses",
            run_id="run",
            runtime_id="rt",
            lease_id="lease",
            fencing_token=1,
            user_id="101",
            dept_id="9",
        )
        from auraclaw.infrastructure.connectors.mcp.wire import McpTrustedContext

        mcp_trusted = McpTrustedContext(
            tenant_id=trusted.tenant_id,
            root_session_id=trusted.root_session_id,
            session_id=trusted.session_id,
            run_id=trusted.run_id,
            runtime_id=trusted.runtime_id,
            lease_id=trusted.lease_id,
            fencing_token=trusted.fencing_token,
            user_id=trusted.user_id,
            dept_id=trusted.dept_id,
        )
        await transport.send(
            McpJsonRpcRequest(id="1", method="tools/list"),
            trusted_context=mcp_trusted,
        )
        assert calls[0]["request"]["_auraclaw_identity"]["user_id"] == "101"  # type: ignore[index]
        assert calls[0]["request"]["_auraclaw_identity"]["dept_id"] == "9"  # type: ignore[index]
        with pytest.raises(PolicyDeniedError, match="dept_id"):
            await transport.send(
                McpJsonRpcRequest(
                    id="1b",
                    method="tools/call",
                    params={"name": "order.get", "arguments": {"dept_id": "8"}},
                ),
                trusted_context=mcp_trusted,
            )
        missing_dept = mcp_trusted.model_copy(update={"dept_id": None})
        await transport.send(
            McpJsonRpcRequest(id="1c", method="tools/list"),
            trusted_context=missing_dept,
        )
        assert calls[-1]["request"]["_auraclaw_identity"]["dept_id"] is None  # type: ignore[index]
        with pytest.raises(PolicyDeniedError, match="tenant_id"):
            await transport.send(
                McpJsonRpcRequest(
                    id="2",
                    method="tools/call",
                    params={"name": "order.get", "arguments": {"tenant_id": "2"}},
                ),
                trusted_context=mcp_trusted,
            )
        missing_user = mcp_trusted.model_copy(update={"user_id": None})
        with pytest.raises(PolicyDeniedError, match="trusted user"):
            await transport.send(
                McpJsonRpcRequest(
                    id="3",
                    method="tools/call",
                    params={"name": "order.get", "arguments": {}},
                ),
                trusted_context=missing_user,
            )

    asyncio.run(scenario())


def test_internal_workload_cannot_spoof_service_identity() -> None:
    from auraclaw.contracts.internal import InternalRequestContext, ServiceIdentity
    from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
    from auraclaw.internal.http import create_contract_app
    from auraclaw.internal.routes import session_routes
    from auraclaw.internal.security import InMemoryFencingTokenLedger, LeaseAssertionVerifier
    from auraclaw.session.internal_service import SessionInternalService

    service = SessionInternalService(
        InMemoryEventStore(),
        lease_verifier=LeaseAssertionVerifier(
            {"k": b"lease-assertion-signing-key-00000001"},
            ledger=InMemoryFencingTokenLedger(),
        ),
    )
    app = create_contract_app(
        "session",
        session_routes(service),
        workload_identities={"task-token": ServiceIdentity.TASK_API},
    )
    with TestClient(app) as client:
        denied = client.post(
            "/internal/v1/session/append",
            headers={
                "Authorization": "Bearer task-token",
                "X-AuraClaw-Contract-Version": "2026-07-22",
            },
            json={
                "context": InternalRequestContext(
                    tenant_id="t1",
                    service_identity=ServiceIdentity.AGENT_RUNTIME,
                    request_id="r1",
                    correlation_id="c1",
                    causation_id="c1",
                ).model_dump(mode="json"),
                "root_session_id": "s1",
                "session_id": "s1",
                "command_id": "cmd",
                "expected_version": 0,
                "operation": "create_task",
                "actor_type": "user",
                "actor_id": "u1",
                "events": [{"type": "session.created", "payload": {"goal": "x"}}],
            },
        )
        assert denied.status_code == 401
