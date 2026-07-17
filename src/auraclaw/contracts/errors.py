

class AuraClawError(Exception):
    code = "auraclaw_error"
    status_code = 500

    def __init__(self, message: str, *, detail: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.detail = detail


class NotFoundError(AuraClawError):
    code = "not_found"
    status_code = 404


class VersionConflictError(AuraClawError):
    code = "version_conflict"
    status_code = 409


class InvalidTransitionError(AuraClawError):
    code = "invalid_transition"
    status_code = 409


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


class SchemaValidationError(AuraClawError):
    code = "tool_schema_invalid"
    status_code = 422


class PolicyDeniedError(AuraClawError):
    code = "policy_denied"
    status_code = 403


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
