

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
