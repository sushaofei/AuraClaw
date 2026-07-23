from __future__ import annotations

import hashlib
import hmac
from datetime import UTC, datetime, timedelta
from urllib.parse import quote, urlencode, urlsplit, urlunsplit


class SeaweedFSS3Presigner:
    """Minimal AWS SigV4 presigner for SeaweedFS' S3-compatible endpoint."""

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
