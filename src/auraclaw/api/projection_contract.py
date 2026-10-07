from fastapi import Response, status


def apply_projection_contract(
    response: Response,
    *,
    projection_version: int,
    min_version: int | None,
    if_none_match: str | None,
    stale_retry_after_seconds: int = 1,
    allow_not_modified: bool = True,
) -> bool:
    """Apply the uniform read-your-writes contract for one Session resource."""

    etag = f'W/"{projection_version}"'
    response.headers["ETag"] = etag
    response.headers["X-Projection-Version"] = str(projection_version)
    is_fresh = min_version is None or projection_version >= min_version
    if not is_fresh:
        response.status_code = status.HTTP_202_ACCEPTED
        response.headers["Retry-After"] = str(stale_retry_after_seconds)
    elif allow_not_modified and if_none_match == etag:
        response.status_code = status.HTTP_304_NOT_MODIFIED
    return is_fresh
