from enum import StrEnum


class SessionStatus(StrEnum):
    CREATED = "created"
    READY = "ready"
    PENDING = "pending"
    RUNNABLE = "runnable"
    RUNNING = "running"
    WAITING_FOR_HUMAN = "waiting_for_human"
    PAUSED = "paused"
    RETRY_WAIT = "retry_wait"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    CLOSED = "closed"


class RunStatus(StrEnum):
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
    SessionStatus.CLOSED,
}


TERMINAL_RUN_STATUSES = {
    RunStatus.COMPLETED,
    RunStatus.FAILED,
    RunStatus.CANCELLED,
}
