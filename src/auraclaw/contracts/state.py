from enum import StrEnum


class SessionStatus(StrEnum):
    CREATED = "created"
    PENDING = "pending"
    RUNNABLE = "runnable"
    RUNNING = "running"
    WAITING_FOR_HUMAN = "waiting_for_human"
    PAUSED = "paused"
    RETRY_WAIT = "retry_wait"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class Visibility(StrEnum):
    USER = "user"
    INTERNAL = "internal"
    SECRET = "secret"


TERMINAL_SESSION_STATUSES = {
    SessionStatus.COMPLETED,
    SessionStatus.FAILED,
    SessionStatus.CANCELLED,
}
