from __future__ import annotations

from auraclaw.action.mcp_internal_service import McpRegistryInternalService
from auraclaw.action.skill_internal_service import SkillPublicationInternalService
from auraclaw.admin.internal_service import OwnerAdminService
from auraclaw.artifact.internal_service import ArtifactInternalService
from auraclaw.contracts.internal import (
    AdminOperationRequest,
    AdminOperationResponse,
    ApprovalCommandRequest,
    ApprovalValidationResponse,
    ArtifactCreateUploadRequest,
    ArtifactDeleteRequest,
    ArtifactDeleteResponse,
    ArtifactDownloadRequest,
    ArtifactDownloadResponse,
    ArtifactFinalizeRequest,
    ArtifactFinalizeResponse,
    ArtifactSkillOrphanClaimRequest,
    ArtifactSkillOrphanClaimResponse,
    ArtifactSkillOrphanResolveRequest,
    ArtifactSkillOrphanResolveResponse,
    ArtifactSkillPublicationBindRequest,
    ArtifactSkillPublicationBindResponse,
    ArtifactSkillPublicationClaimRequest,
    ArtifactSkillPublicationClaimResponse,
    ArtifactUploadResponse,
    AssignmentAbandonRequest,
    AssignmentAbandonResponse,
    AssignmentClaimRequest,
    AssignmentClaimResponse,
    AssignmentDispositionRequest,
    AssignmentDispositionResponse,
    AssignmentRenewRequest,
    AssignmentRenewResponse,
    CancellationRequest,
    CancellationResponse,
    CheckpointResponse,
    CollaborationCommandRequest,
    CollaborationCommandResponse,
    CredentialInvokeRequest,
    CredentialInvokeResponse,
    CredentialResourceRequest,
    CredentialResourceResponse,
    LoadCheckpointRequest,
    McpEgressCommandRequest,
    McpEgressCommandResponse,
    McpRegistryAdminRequest,
    McpRegistryAdminResponse,
    McpRegistrySnapshotRequest,
    McpRegistrySnapshotResponse,
    ModelCancelRequest,
    ModelCancelResponse,
    ModelGenerateRequest,
    ModelGenerateResponse,
    ModelStreamEvent,
    OutboxClaimRequest,
    OutboxClaimResponse,
    OutboxDispositionRequest,
    OutboxDispositionResponse,
    PolicyEvaluateRequest,
    PolicyEvaluateResponse,
    PolicyValidateDecisionRequest,
    PolicyValidateDecisionResponse,
    RuntimeHeartbeatRequest,
    RuntimeHeartbeatResponse,
    RuntimeRegistrationRequest,
    SaveCheckpointRequest,
    SessionAppendRequest,
    SessionAppendResponse,
    SessionFeedRequest,
    SessionFeedResponse,
    SessionRootFeedRequest,
    SessionRootFeedResponse,
    SkillActiveBindingReferenceRequest,
    SkillActiveBindingReferenceResponse,
    SkillAdminSnapshotInternalRequest,
    SkillAdminSnapshotInternalResponse,
    SkillAdmissionListInternalRequest,
    SkillAdmissionListInternalResponse,
    SkillAdmissionMetricsInternalRequest,
    SkillAdmissionMetricsInternalResponse,
    SkillBindingReferenceRequest,
    SkillBindingReferenceResponse,
    SkillInstallationInternalRequest,
    SkillInstallationInternalResponse,
    SkillPackageStateInternalRequest,
    SkillPackageStateInternalResponse,
    SkillPublishArtifactInternalRequest,
    SkillPublisherInternalResponse,
    SkillPublisherRegisterInternalRequest,
    SkillPublisherRevokeKeyInternalRequest,
    SkillPublisherRotateKeyInternalRequest,
    SkillPublisherStateInternalRequest,
    SkillPublisherStatusInternalRequest,
    SkillPublishInternalRequest,
    SkillPublishInternalResponse,
    SkillPurgeInternalRequest,
    SkillPurgeInternalResponse,
    SkillRestoreInternalRequest,
    SkillRestoreInternalResponse,
    SkillRevokeInternalRequest,
    SkillRevokeInternalResponse,
    SkillSourceConfigureInternalRequest,
    SkillSourceInternalResponse,
    SkillSourceReadInternalRequest,
    SkillSourceRetireInternalRequest,
    SkillSourceSyncInternalRequest,
    SkillStateInternalRequest,
    SkillStateInternalResponse,
    ValidateLeaseRequest,
    ValidateLeaseResponse,
)
from auraclaw.control.internal_service import ControlInternalService
from auraclaw.credential_proxy.internal_service import CredentialProxyInternalService
from auraclaw.internal.http import (
    ContractRoute,
    StreamContractRoute,
    contract_route,
    stream_contract_route,
)
from auraclaw.model_gateway.internal_service import ModelGatewayInternalService
from auraclaw.policy.internal_service import PolicyInternalService
from auraclaw.session.collaboration_internal_service import CollaborationInternalService
from auraclaw.session.internal_service import SessionInternalService


def session_routes(service: SessionInternalService) -> dict[str, ContractRoute]:
    return {
        "/internal/v1/session/append": contract_route(
            SessionAppendRequest, SessionAppendResponse, service.append
        ),
        "/internal/v1/session/feed": contract_route(
            SessionFeedRequest, SessionFeedResponse, service.feed
        ),
        "/internal/v1/session/root-feed": contract_route(
            SessionRootFeedRequest, SessionRootFeedResponse, service.root_feed
        ),
        "/internal/v1/session/skill-bindings/reference": contract_route(
            SkillBindingReferenceRequest,
            SkillBindingReferenceResponse,
            service.skill_binding_reference,
        ),
        "/internal/v1/session/skill-bindings/active-reference": contract_route(
            SkillActiveBindingReferenceRequest,
            SkillActiveBindingReferenceResponse,
            service.skill_active_binding_reference,
        ),
        "/internal/v1/session/outbox/claim": contract_route(
            OutboxClaimRequest, OutboxClaimResponse, service.claim_outbox
        ),
        "/internal/v1/session/outbox/disposition": contract_route(
            OutboxDispositionRequest,
            OutboxDispositionResponse,
            service.disposition_outbox,
        ),
    }


def collaboration_routes(
    service: CollaborationInternalService,
) -> dict[str, ContractRoute]:
    return {
        "/internal/v1/collaboration/command": contract_route(
            CollaborationCommandRequest,
            CollaborationCommandResponse,
            service.command,
        )
    }


def control_routes(service: ControlInternalService) -> dict[str, ContractRoute]:
    return {
        "/internal/v1/control/runtimes/register": contract_route(
            RuntimeRegistrationRequest,
            RuntimeHeartbeatResponse,
            service.register_runtime,
        ),
        "/internal/v1/control/runtimes/heartbeat": contract_route(
            RuntimeHeartbeatRequest,
            RuntimeHeartbeatResponse,
            service.heartbeat,
        ),
        "/internal/v1/control/assignments/claim": contract_route(
            AssignmentClaimRequest,
            AssignmentClaimResponse,
            service.claim_assignments,
        ),
        "/internal/v1/control/assignments/disposition": contract_route(
            AssignmentDispositionRequest,
            AssignmentDispositionResponse,
            service.disposition_assignment,
        ),
        "/internal/v1/control/assignments/renew": contract_route(
            AssignmentRenewRequest,
            AssignmentRenewResponse,
            service.renew_assignment,
        ),
        "/internal/v1/control/assignments/abandon": contract_route(
            AssignmentAbandonRequest,
            AssignmentAbandonResponse,
            service.abandon_assignment,
        ),
        "/internal/v1/control/checkpoints/save": contract_route(
            SaveCheckpointRequest, CheckpointResponse, service.save_checkpoint
        ),
        "/internal/v1/control/checkpoints/load": contract_route(
            LoadCheckpointRequest, CheckpointResponse, service.load_checkpoint
        ),
        "/internal/v1/control/cancellation/request": contract_route(
            CancellationRequest, CancellationResponse, service.request_cancel
        ),
        "/internal/v1/control/cancellation/status": contract_route(
            CancellationRequest, CancellationResponse, service.is_cancelled
        ),
        "/internal/v1/control/leases/validate": contract_route(
            ValidateLeaseRequest, ValidateLeaseResponse, service.validate_lease
        ),
    }


def model_routes(service: ModelGatewayInternalService) -> dict[str, ContractRoute]:
    return {
        "/internal/v1/model/generate": contract_route(
            ModelGenerateRequest, ModelGenerateResponse, service.generate
        ),
        "/internal/v1/model/cancel": contract_route(
            ModelCancelRequest, ModelCancelResponse, service.cancel
        ),
    }


def model_stream_routes(
    service: ModelGatewayInternalService,
) -> dict[str, StreamContractRoute]:
    return {
        "/internal/v1/model/stream": stream_contract_route(
            ModelGenerateRequest, ModelStreamEvent, service.generate_stream
        ),
    }


def policy_routes(service: PolicyInternalService) -> dict[str, ContractRoute]:
    return {
        "/internal/v1/policy/evaluate": contract_route(
            PolicyEvaluateRequest, PolicyEvaluateResponse, service.evaluate
        ),
        "/internal/v1/policy/approvals/command": contract_route(
            ApprovalCommandRequest, ApprovalValidationResponse, service.approval
        ),
        "/internal/v1/policy/decisions/validate": contract_route(
            PolicyValidateDecisionRequest,
            PolicyValidateDecisionResponse,
            service.validate_decision,
        ),
    }


def credential_routes(
    service: CredentialProxyInternalService,
) -> dict[str, ContractRoute]:
    return {
        "/internal/v1/credentials/invoke": contract_route(
            CredentialInvokeRequest, CredentialInvokeResponse, service.invoke
        ),
        "/internal/v1/credentials/resource": contract_route(
            CredentialResourceRequest, CredentialResourceResponse, service.resource
        ),
        "/internal/v1/credentials/mcp-egress": contract_route(
            McpEgressCommandRequest, McpEgressCommandResponse, service.mcp_egress
        ),
    }


def mcp_registry_routes(
    service: McpRegistryInternalService,
) -> dict[str, ContractRoute]:
    return {
        "/command": contract_route(
            McpRegistryAdminRequest, McpRegistryAdminResponse, service.command
        ),
        "/snapshot": contract_route(
            McpRegistrySnapshotRequest, McpRegistrySnapshotResponse, service.snapshot
        ),
    }


def artifact_routes(service: ArtifactInternalService) -> dict[str, ContractRoute]:
    return {
        "/internal/v1/artifacts/uploads/create": contract_route(
            ArtifactCreateUploadRequest, ArtifactUploadResponse, service.create_upload
        ),
        "/internal/v1/artifacts/uploads/finalize": contract_route(
            ArtifactFinalizeRequest, ArtifactFinalizeResponse, service.finalize
        ),
        "/internal/v1/artifacts/download": contract_route(
            ArtifactDownloadRequest, ArtifactDownloadResponse, service.download
        ),
        "/internal/v1/artifacts/delete": contract_route(
            ArtifactDeleteRequest, ArtifactDeleteResponse, service.delete
        ),
        "/internal/v1/artifacts/skills/claim-publication": contract_route(
            ArtifactSkillPublicationClaimRequest,
            ArtifactSkillPublicationClaimResponse,
            service.claim_skill_publication,
        ),
        "/internal/v1/artifacts/skills/bind-publication": contract_route(
            ArtifactSkillPublicationBindRequest,
            ArtifactSkillPublicationBindResponse,
            service.bind_skill_publication,
        ),
        "/internal/v1/artifacts/skills/orphans/claim": contract_route(
            ArtifactSkillOrphanClaimRequest,
            ArtifactSkillOrphanClaimResponse,
            service.claim_skill_orphans,
        ),
        "/internal/v1/artifacts/skills/orphans/resolve": contract_route(
            ArtifactSkillOrphanResolveRequest,
            ArtifactSkillOrphanResolveResponse,
            service.resolve_skill_orphan,
        ),
    }


def skill_publication_routes(
    service: SkillPublicationInternalService,
) -> dict[str, ContractRoute]:
    return {
        "/admissions": contract_route(
            SkillAdmissionListInternalRequest,
            SkillAdmissionListInternalResponse,
            service.list_admissions,
        ),
        "/admission-metrics": contract_route(
            SkillAdmissionMetricsInternalRequest,
            SkillAdmissionMetricsInternalResponse,
            service.admission_metrics,
        ),
        "/publish": contract_route(
            SkillPublishInternalRequest,
            SkillPublishInternalResponse,
            service.publish,
        ),
        "/publish-artifact": contract_route(
            SkillPublishArtifactInternalRequest,
            SkillPublishInternalResponse,
            service.publish_artifact,
        ),
        "/installation": contract_route(
            SkillInstallationInternalRequest,
            SkillInstallationInternalResponse,
            service.change_installation,
        ),
        "/revoke": contract_route(
            SkillRevokeInternalRequest,
            SkillRevokeInternalResponse,
            service.revoke,
        ),
        "/restore": contract_route(
            SkillRestoreInternalRequest,
            SkillRestoreInternalResponse,
            service.restore,
        ),
        "/package": contract_route(
            SkillPackageStateInternalRequest,
            SkillPackageStateInternalResponse,
            service.package_state,
        ),
        "/purge": contract_route(
            SkillPurgeInternalRequest,
            SkillPurgeInternalResponse,
            service.purge,
        ),
        "/state": contract_route(
            SkillStateInternalRequest,
            SkillStateInternalResponse,
            service.state,
        ),
        "/sources/configure": contract_route(
            SkillSourceConfigureInternalRequest,
            SkillSourceInternalResponse,
            service.configure_source,
        ),
        "/sources/retire": contract_route(
            SkillSourceRetireInternalRequest,
            SkillSourceInternalResponse,
            service.retire_source,
        ),
        "/sources/read": contract_route(
            SkillSourceReadInternalRequest,
            SkillSourceInternalResponse,
            service.read_sources,
        ),
        "/sources/sync": contract_route(
            SkillSourceSyncInternalRequest,
            SkillSourceInternalResponse,
            service.sync_source,
        ),
        "/publishers/register": contract_route(
            SkillPublisherRegisterInternalRequest,
            SkillPublisherInternalResponse,
            service.register_publisher,
        ),
        "/publishers/rotate-key": contract_route(
            SkillPublisherRotateKeyInternalRequest,
            SkillPublisherInternalResponse,
            service.rotate_publisher_key,
        ),
        "/publishers/revoke-key": contract_route(
            SkillPublisherRevokeKeyInternalRequest,
            SkillPublisherInternalResponse,
            service.revoke_publisher_key,
        ),
        "/publishers/status": contract_route(
            SkillPublisherStatusInternalRequest,
            SkillPublisherInternalResponse,
            service.change_publisher_status,
        ),
        "/publishers/state": contract_route(
            SkillPublisherStateInternalRequest,
            SkillPublisherInternalResponse,
            service.publisher_state,
        ),
        "/admin-snapshot": contract_route(
            SkillAdminSnapshotInternalRequest,
            SkillAdminSnapshotInternalResponse,
            service.admin_snapshot,
        ),
    }


def admin_routes(service: OwnerAdminService) -> dict[str, ContractRoute]:
    return {
        "/internal/v1/admin/operations": contract_route(
            AdminOperationRequest, AdminOperationResponse, service.execute
        )
    }
