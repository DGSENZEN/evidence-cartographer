from collections.abc import Mapping
from typing import BinaryIO, get_type_hints

from urllib3 import PoolManager

from evidence_cartographer.infrastructure.http import (
    StreamingHttpClient,
    StreamingHttpResponse,
    Urllib3StreamingHttpClient,
)


def test_urllib3_client_satisfies_streaming_protocol() -> None:
    client = Urllib3StreamingHttpClient(PoolManager())
    typed: StreamingHttpClient = client
    assert typed is client


def test_response_protocol_exposes_streaming_metadata() -> None:
    hints = get_type_hints(StreamingHttpResponse)
    assert hints["status"] is int
    assert hints["headers"] == Mapping[str, str]
    assert hints["final_url"] is str
    assert hints["body"] is BinaryIO
