from collections.abc import Iterator, Mapping
from contextlib import AbstractContextManager, contextmanager
from dataclasses import dataclass
from typing import BinaryIO, Protocol, cast
from urllib.parse import urljoin

import urllib3
from urllib3 import PoolManager, Timeout

REDIRECT_STATUSES = frozenset({301, 302, 303, 307, 308})


class StreamingHttpResponse(Protocol):
    status: int
    headers: Mapping[str, str]
    final_url: str
    body: BinaryIO


class StreamingHttpClient(Protocol):
    def open(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        connect_timeout_seconds: float,
        read_timeout_seconds: float,
        max_redirects: int = 5,
    ) -> AbstractContextManager[StreamingHttpResponse]: ...


@dataclass(slots=True)
class _StreamingResponse:
    status: int
    headers: Mapping[str, str]
    final_url: str
    body: BinaryIO


class Urllib3StreamingHttpClient:
    def __init__(self, pool: PoolManager) -> None:
        self._pool = pool

    @contextmanager
    def open(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        connect_timeout_seconds: float,
        read_timeout_seconds: float,
        max_redirects: int = 5,
    ) -> Iterator[StreamingHttpResponse]:
        current_url = url
        response: urllib3.response.BaseHTTPResponse | None = None
        for redirect_count in range(max_redirects + 1):
            response = self._pool.request(
                "GET",
                current_url,
                headers=dict(headers or {}),
                preload_content=False,
                redirect=False,
                retries=False,
                timeout=Timeout(
                    connect=connect_timeout_seconds,
                    read=read_timeout_seconds,
                ),
            )
            if response.status not in REDIRECT_STATUSES:
                break
            location = response.headers.get("location")
            response.close()
            response.release_conn()
            response = None
            if location is None or redirect_count == max_redirects:
                raise RuntimeError("HTTP redirect chain is invalid or too long")
            current_url = urljoin(current_url, location)

        if response is None:
            raise RuntimeError("HTTP response was not created")
        try:
            yield _StreamingResponse(
                status=response.status,
                headers=cast(Mapping[str, str], response.headers),
                final_url=current_url,
                body=cast(BinaryIO, response),
            )
        finally:
            response.close()
            response.release_conn()
