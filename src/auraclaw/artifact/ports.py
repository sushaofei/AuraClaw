from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Literal, Protocol

if TYPE_CHECKING:
    from auraclaw.artifact.internal_service import PendingUpload


class ObjectPresigner(Protocol):
    def presign(
        self,
        method: str,
        object_key: str,
        *,
        ttl: timedelta = timedelta(minutes=10),
        now: datetime | None = None,
        query_params: Mapping[str, str] | None = None,
    ) -> tuple[str, datetime]: ...


class ObjectMultipartClient(Protocol):
    async def aclose(self) -> None: ...

    async def create(
        self,
        object_key: str,
        *,
        expected_size: int,
        part_size: int,
    ) -> tuple[str, tuple[str, ...]]: ...

    async def complete(
        self,
        object_key: str,
        upload_id: str,
        parts: tuple[dict[str, object], ...],
    ) -> None: ...

    async def abort(self, object_key: str, upload_id: str) -> bool: ...


class ObjectVerifier(Protocol):
    async def aclose(self) -> None: ...

    async def verify(self, pending: PendingUpload) -> bool: ...

    async def inspect(
        self, pending: PendingUpload
    ) -> Literal[
        "clean", "missing", "size_mismatch", "checksum_mismatch", "unavailable"
    ]: ...

    async def delete(self, pending: PendingUpload) -> bool: ...

    async def readiness(self) -> tuple[bool, str]: ...
