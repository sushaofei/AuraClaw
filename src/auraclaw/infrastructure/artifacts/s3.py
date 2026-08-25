from __future__ import annotations

import hashlib
import hmac
import math
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from typing import Literal
from urllib.parse import quote, urlencode, urlsplit, urlunsplit
from xml.etree import ElementTree

import httpx

from auraclaw.artifact.internal_service import PendingUpload
from auraclaw.contracts.errors import ArtifactAccessError


class S3CompatiblePresigner:
    """Minimal AWS SigV4 presigner for S3-compatible object stores."""

    def __init__(
        self,
        endpoint: str,
        *,
        access_key: str,
        secret_key: str,
        bucket: str,
        region: str,
        path_style: bool = True,
    ) -> None:
        self._endpoint = endpoint.rstrip("/")
        self._access_key = access_key
        self._secret_key = secret_key
        self._bucket = bucket
        self._region = region
        self._path_style = path_style

    def presign(
        self,
        method: str,
        object_key: str,
        *,
        ttl: timedelta = timedelta(minutes=10),
        now: datetime | None = None,
        query_params: Mapping[str, str] | None = None,
    ) -> tuple[str, datetime]:
        issued = (now or datetime.now(UTC)).astimezone(UTC)
        expires_at = issued + ttl
        expires = max(1, min(int(ttl.total_seconds()), 604800))
        timestamp = issued.strftime("%Y%m%dT%H%M%SZ")
        date = issued.strftime("%Y%m%d")
        scope = f"{date}/{self._region}/s3/aws4_request"
        parsed = urlsplit(self._endpoint)
        object_parts = tuple(object_key.strip("/").split("/"))
        path_parts = (self._bucket, *object_parts) if self._path_style else object_parts
        canonical_uri = "/" + "/".join(
            quote(part, safe="-_.~") for part in path_parts
        )
        netloc = parsed.netloc if self._path_style else f"{self._bucket}.{parsed.netloc}"
        query = {
            **dict(query_params or {}),
            "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
            "X-Amz-Credential": f"{self._access_key}/{scope}",
            "X-Amz-Date": timestamp,
            "X-Amz-Expires": str(expires),
            "X-Amz-SignedHeaders": "host",
        }
        canonical_query = urlencode(sorted(query.items()), quote_via=quote, safe="-_.~")
        canonical_request = "\n".join(
            (
                method.upper(),
                canonical_uri,
                canonical_query,
                f"host:{netloc}\n",
                "host",
                "UNSIGNED-PAYLOAD",
            )
        )
        string_to_sign = "\n".join(
            (
                "AWS4-HMAC-SHA256",
                timestamp,
                scope,
                hashlib.sha256(canonical_request.encode()).hexdigest(),
            )
        )
        signing_key = self._signing_key(date)
        query["X-Amz-Signature"] = hmac.new(
            signing_key, string_to_sign.encode(), hashlib.sha256
        ).hexdigest()
        final_query = urlencode(sorted(query.items()), quote_via=quote, safe="-_.~")
        return (
            urlunsplit((parsed.scheme, netloc, canonical_uri, final_query, "")),
            expires_at,
        )

    def _signing_key(self, date: str) -> bytes:
        date_key = hmac.new(
            f"AWS4{self._secret_key}".encode(), date.encode(), hashlib.sha256
        ).digest()
        region_key = hmac.new(date_key, self._region.encode(), hashlib.sha256).digest()
        service_key = hmac.new(region_key, b"s3", hashlib.sha256).digest()
        return hmac.new(service_key, b"aws4_request", hashlib.sha256).digest()


class S3CompatibleMultipartClient:
    """S3-compatible multipart lifecycle using only short-lived signed requests."""

    def __init__(
        self,
        presigner: S3CompatiblePresigner,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._presigner = presigner
        self._client = client or httpx.AsyncClient(timeout=30.0)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def create(
        self,
        object_key: str,
        *,
        expected_size: int,
        part_size: int,
    ) -> tuple[str, tuple[str, ...]]:
        part_count = max(1, math.ceil(expected_size / part_size))
        if part_count > 10_000:
            raise ArtifactAccessError("artifact multipart upload exceeds 10000 parts")
        url, _ = self._presigner.presign(
            "POST", object_key, query_params={"uploads": ""}
        )
        try:
            response = await self._client.post(url)
        except httpx.HTTPError as exc:
            raise ArtifactAccessError("artifact multipart initialization failed") from exc
        if response.is_error:
            raise ArtifactAccessError("artifact multipart initialization failed")
        try:
            root = ElementTree.fromstring(response.content)
            upload_id = next(
                element.text
                for element in root.iter()
                if element.tag.rsplit("}", 1)[-1] == "UploadId" and element.text
            )
        except (ElementTree.ParseError, StopIteration) as exc:
            raise ArtifactAccessError(
                "object store returned no multipart upload id"
            ) from exc
        part_urls = tuple(
            self._presigner.presign(
                "PUT",
                object_key,
                query_params={
                    "partNumber": str(part_number),
                    "uploadId": upload_id,
                },
            )[0]
            for part_number in range(1, part_count + 1)
        )
        return upload_id, part_urls

    async def complete(
        self,
        object_key: str,
        upload_id: str,
        parts: tuple[dict[str, object], ...],
    ) -> None:
        normalized: list[tuple[int, str]] = []
        for part in parts:
            try:
                number = int(str(part["part_number"]))
                etag = str(part["etag"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ArtifactAccessError("invalid multipart completion part") from exc
            if number < 1 or not etag:
                raise ArtifactAccessError("invalid multipart completion part")
            normalized.append((number, etag))
        if not normalized or len({number for number, _ in normalized}) != len(normalized):
            raise ArtifactAccessError("multipart completion parts are missing or duplicated")
        root = ElementTree.Element("CompleteMultipartUpload")
        for number, etag in sorted(normalized):
            item = ElementTree.SubElement(root, "Part")
            ElementTree.SubElement(item, "PartNumber").text = str(number)
            ElementTree.SubElement(item, "ETag").text = etag
        url, _ = self._presigner.presign(
            "POST", object_key, query_params={"uploadId": upload_id}
        )
        try:
            response = await self._client.post(
                url,
                content=ElementTree.tostring(root, encoding="utf-8", xml_declaration=True),
                headers={"Content-Type": "application/xml"},
            )
        except httpx.HTTPError as exc:
            raise ArtifactAccessError("artifact multipart completion failed") from exc
        if response.is_error:
            raise ArtifactAccessError("artifact multipart completion failed")

    async def abort(self, object_key: str, upload_id: str) -> bool:
        url, _ = self._presigner.presign(
            "DELETE", object_key, query_params={"uploadId": upload_id}
        )
        try:
            response = await self._client.delete(url)
        except httpx.HTTPError:
            return False
        return response.status_code in {200, 202, 204, 404}


class S3CompatibleObjectVerifier:
    def __init__(
        self,
        presigner: S3CompatiblePresigner,
        *,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._presigner = presigner
        self._client = client or httpx.AsyncClient(timeout=5.0)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def verify(self, pending: PendingUpload) -> bool:
        return await self.inspect(pending) == "clean"

    async def inspect(
        self, pending: PendingUpload
    ) -> Literal["clean", "missing", "size_mismatch", "checksum_mismatch", "unavailable"]:
        url, _ = self._presigner.presign("HEAD", pending.object_key)
        try:
            response = await self._client.head(url)
        except httpx.HTTPError:
            return "unavailable"
        if response.status_code == 404:
            return "missing"
        if response.status_code != 200:
            return "unavailable"
        if int(response.headers.get("Content-Length", "-1")) != pending.expected_size:
            return "size_mismatch"
        get_url, _ = self._presigner.presign("GET", pending.object_key)
        digest = hashlib.sha256()
        size = 0
        try:
            async with self._client.stream("GET", get_url) as downloaded:
                if downloaded.status_code != 200:
                    return "missing" if downloaded.status_code == 404 else "unavailable"
                async for chunk in downloaded.aiter_bytes():
                    size += len(chunk)
                    if size > pending.expected_size:
                        return "size_mismatch"
                    digest.update(chunk)
        except httpx.HTTPError:
            return "unavailable"
        if size != pending.expected_size:
            return "size_mismatch"
        if digest.hexdigest().lower() != pending.expected_checksum.lower():
            return "checksum_mismatch"
        return "clean"

    async def delete(self, pending: PendingUpload) -> bool:
        url, _ = self._presigner.presign("DELETE", pending.object_key)
        try:
            response = await self._client.delete(url)
        except httpx.HTTPError:
            return False
        return response.status_code in {200, 202, 204, 404}

    async def readiness(self) -> tuple[bool, str]:
        url, _ = self._presigner.presign("HEAD", "health/readiness-probe")
        try:
            response = await self._client.head(url)
        except httpx.HTTPError as exc:
            return False, type(exc).__name__
        reachable = response.status_code in {200, 404}
        return reachable, f"HTTP {response.status_code}"
