

class AuraClawError(Exception):
    code = "auraclaw_error"
    status_code = 500

    def __init__(
        self,
        message: str,
        *,
        detail: str | None = None,
        retry_after: int | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail
        self.retry_after = retry_after


class NotFoundError(AuraClawError):
    code = "not_found"
    status_code = 404


class VersionConflictError(AuraClawError):
    code = "version_conflict"
    status_code = 409


class StaleCapabilitySnapshotError(VersionConflictError):
    code = "stale_capability_snapshot"


class InvalidTransitionError(AuraClawError):
    code = "invalid_transition"
    status_code = 409


class UnauthenticatedError(AuraClawError):
    code = "unauthenticated"
    status_code = 401


class AuthorizationError(AuraClawError):
    code = "authorization_denied"
    status_code = 403


class LeaseConflictError(AuraClawError):
    code = "lease_conflict"
    status_code = 409


class FencingTokenError(AuraClawError):
    code = "stale_fencing_token"
    status_code = 409


class RuntimeCancelledError(AuraClawError):
    code = "runtime_cancelled"
    status_code = 409


class BudgetExceededError(AuraClawError):
    code = "runtime_budget_exceeded"
    status_code = 409


class ModelAuthenticationError(AuraClawError):
    code = "model_authentication_failed"
    status_code = 502


class ModelRateLimitError(AuraClawError):
    code = "model_rate_limited"
    status_code = 429


class ModelTimeoutError(AuraClawError):
    code = "model_timeout"
    status_code = 504


class ModelProviderError(AuraClawError):
    code = "model_provider_error"
    status_code = 502


class SchemaValidationError(AuraClawError):
    code = "tool_schema_invalid"
    status_code = 422


class PolicyDeniedError(AuraClawError):
    code = "policy_denied"
    status_code = 403


class SkillContentRejectedError(PolicyDeniedError):
    def __init__(self, finding_code: str) -> None:
        super().__init__("Skill package failed content security policy")
        self.code = f"skill_content_{finding_code}"


class ApprovalValidationError(AuraClawError):
    code = "approval_invalid"
    status_code = 409


class ArtifactAccessError(AuraClawError):
    code = "artifact_access_denied"
    status_code = 403


class SandboxViolationError(AuraClawError):
    code = "sandbox_violation"
    status_code = 403


class CredentialAccessError(AuraClawError):
    code = "credential_access_denied"
    status_code = 403


class CollaborationValidationError(AuraClawError):
    code = "collaboration_invalid"
    status_code = 409


class SyncInvokeBusyError(AuraClawError):
    code = "sync_invoke_busy"
    status_code = 429

    def __init__(
        self,
        message: str = "too many synchronous waits",
        *,
        detail: str | None = None,
        retry_after: int = 2,
    ) -> None:
        super().__init__(message, detail=detail, retry_after=retry_after)


class ResourceBusyError(AuraClawError):
    code = "resource_gateway_busy"
    status_code = 429

    def __init__(self, message: str = "resource gateway capacity is exhausted") -> None:
        super().__init__(message, retry_after=1)
