from __future__ import annotations

from auraclaw.contracts.delivery import DeliveryJob, ResultSinkConfig, SinkResponse
from auraclaw.contracts.tools import RiskLevel, ToolCapability, ToolInvocation, ToolPermission
from auraclaw.infrastructure.clients.credential import RemoteCredentialProxy
from auraclaw.infrastructure.clients.policy import RemotePolicyClient


class CredentialProxyWebhookSink:
    sink_type = "webhook"

    def __init__(
        self,
        policy: RemotePolicyClient,
        credentials: RemoteCredentialProxy,
    ) -> None:
        self._policy = policy
        self._credentials = credentials

    async def deliver(self, job: DeliveryJob, config: ResultSinkConfig) -> SinkResponse:
        if config.credential_ref is None:
            return SinkResponse(False, False, "webhook credential_ref is required")
        capability = ToolCapability(
            name="webhook",
            version="1",
            description="controlled delivery egress",
            input_schema={},
            output_schema={},
            permission=ToolPermission.WRITE_AUTONOMOUS,
            risk_level=RiskLevel.HIGH,
            runtime_location="credential_proxy",
            allowed_credential_operations=("deliver",),
        )
        invocation = ToolInvocation(
            tool_invocation_id=job.delivery_id,
            tenant_id=job.tenant_id,
            root_session_id=job.root_session_id,
            session_id=job.session_id,
            run_id=job.run_id or job.delivery_id,
            tool_name=capability.name,
            tool_version=capability.version,
            arguments={"target_ref": config.target_ref, "payload": job.payload},
            expected_side_effect="deliver",
            idempotency_key=job.delivery_id,
            deadline=None,
            fencing_token=1,
            actor_id="delivery-worker",
            credential_ref=config.credential_ref,
        )
        evaluation = await self._policy.evaluate(capability, invocation)
        if evaluation.decision.value not in {"allow", "allow_with_constraints"}:
            return SinkResponse(False, False, "delivery policy denied egress")
        response = await self._credentials.invoke(
            tenant_id=job.tenant_id,
            session_id=job.session_id,
            tool_name="webhook",
            credential_ref=config.credential_ref,
            operation="deliver",
            request={
                "target_url": config.target_ref,
                "delivery_id": job.delivery_id,
                "payload": job.payload,
            },
            policy_decision_id=evaluation.decision_id,
        )
        return SinkResponse(
            bool(response.get("succeeded")),
            bool(response.get("retryable")),
            str(response.get("summary", "credential proxy response")),
        )
