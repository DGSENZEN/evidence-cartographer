from datetime import UTC, datetime
from importlib.metadata import version
from io import BytesIO
from typing import Any

import pytest
from minio import Minio
from minio.datatypes import Object
from minio.error import S3Error
from urllib3 import HTTPHeaderDict

from evidence_cartographer.application.errors import SinglePutSizeLimitError
from evidence_cartographer.infrastructure.conditional_object_client import (
    MAX_SINGLE_PUT_SIZE_BYTES,
    ConditionalObjectClient,
    ConditionalObjectExistsError,
    ConditionalObjectMetadata,
    ConditionalPutError,
    MinioConditionalObjectClient,
)


class StatRecordingMinio(Minio):
    def stat_object(self, bucket_name: str, object_name: str) -> Object:
        return Object(
            bucket_name=bucket_name,
            object_name=object_name,
            size=7,
            metadata={
                "x-amz-meta-ec-sha256": "a" * 64,
                "x-amz-meta-ec-size": "7",
            },
        )


class FakeHttpResponse:
    def __init__(
        self,
        status: int,
        *,
        data: bytes = b"",
        headers: dict[str, str] | None = None,
    ) -> None:
        self.status = status
        self.data = data
        self.headers = HTTPHeaderDict(headers or {})


class RecordingHttpClient:
    def __init__(self, *responses: FakeHttpResponse | Exception) -> None:
        self.responses = list(responses)
        self.requests: list[dict[str, Any]] = []

    def urlopen(self, method: str, url: str, **kwargs: Any) -> FakeHttpResponse:
        request_kwargs = dict(kwargs)
        body = request_kwargs.pop("body")
        self.requests.append(
            {
                "method": method,
                "url": url,
                "body": body.read(),
                **request_kwargs,
            }
        )
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    def clear(self) -> None:
        return None


def make_raw_client(http_client: RecordingHttpClient) -> Minio:
    client = Minio(
        "minio.test",
        access_key="access",
        secret_key="secret",
        region="us-east-1",
    )
    client._http = http_client  # type: ignore[assignment]
    return client


def make_reconciling_client(
    http_client: RecordingHttpClient,
    stat: Object,
) -> Minio:
    class ReconcilingMinio(Minio):
        def stat_object(self, bucket_name: str, object_name: str) -> Object:
            assert stat.bucket_name == bucket_name
            assert stat.object_name == object_name
            return stat

    client = ReconcilingMinio(
        "minio.test",
        access_key="access",
        secret_key="secret",
        region="us-east-1",
    )
    client._http = http_client  # type: ignore[assignment]
    return client


def missing_object_error(bucket_name: str, object_name: str) -> S3Error:
    return S3Error(
        response=None,  # type: ignore[arg-type]
        code="NoSuchKey",
        message="not found",
        resource=object_name,
        request_id="stat-request",
        host_id="stat-host",
        bucket_name=bucket_name,
        object_name=object_name,
    )


def make_sequenced_stat_client(
    http_client: RecordingHttpClient,
    *stats: Object | Exception,
) -> Minio:
    remaining_stats = list(stats)

    class SequencedStatMinio(Minio):
        def stat_object(self, bucket_name: str, object_name: str) -> Object:
            stat = remaining_stats.pop(0)
            if isinstance(stat, Exception):
                raise stat
            assert stat.bucket_name == bucket_name
            assert stat.object_name == object_name
            return stat

    client = SequencedStatMinio(
        "minio.test",
        access_key="access",
        secret_key="secret",
        region="us-east-1",
    )
    client._http = http_client  # type: ignore[assignment]
    return client


def test_real_minio_client_composes_through_owned_conditional_adapter() -> None:
    raw_client = StatRecordingMinio(
        "minio.test",
        access_key="access",
        secret_key="secret",
        region="us-east-1",
    )
    client: ConditionalObjectClient = MinioConditionalObjectClient(raw_client)

    assert isinstance(client, ConditionalObjectClient)
    assert not isinstance(raw_client, ConditionalObjectClient)
    assert client.stat_object("bronze", "raw/source.csv") == (
        ConditionalObjectMetadata(
            size_bytes=7,
            sha256="a" * 64,
            declared_size_bytes=7,
        )
    )


def test_private_minio_request_contract_uses_verified_exact_versions() -> None:
    assert version("minio") == "7.2.20"
    assert version("urllib3") == "2.7.0"


def test_rejects_payload_larger_than_the_single_put_ceiling() -> None:
    client = MinioConditionalObjectClient(
        Minio(
            "minio.test",
            access_key="access",
            secret_key="secret",
            region="us-east-1",
        )
    )

    with pytest.raises(SinglePutSizeLimitError) as raised:
        client.put_object_if_absent(
            "bronze",
            "raw/source.csv",
            BytesIO(),
            MAX_SINGLE_PUT_SIZE_BYTES + 1,
            "a" * 64,
        )

    assert raised.value.size_bytes == 5_368_709_121
    assert raised.value.max_size_bytes == 5_368_709_120
    assert raised.value.object_uri == "s3://bronze/raw/source.csv"


def test_single_put_is_conditional_tagged_and_disables_transport_retries() -> None:
    payload = b"source bytes"
    digest = "b" * 64
    http_client = RecordingHttpClient(FakeHttpResponse(200))
    client = MinioConditionalObjectClient(make_raw_client(http_client))

    client.put_object_if_absent(
        "bronze",
        "raw/source.csv",
        BytesIO(payload),
        len(payload),
        digest,
        content_type="text/csv",
    )

    [request] = http_client.requests
    assert request["method"] == "PUT"
    assert request["body"] == payload
    assert request["retries"] is False
    assert request["redirect"] is False
    assert request["preload_content"] is True
    headers = request["headers"]
    assert headers["If-None-Match"] == "*"
    assert headers["Content-Length"] == str(len(payload))
    assert headers["Content-Type"] == "text/csv"
    assert headers["x-amz-meta-ec-sha256"] == digest
    assert headers["x-amz-meta-ec-size"] == str(len(payload))


def test_retries_http_409_with_a_rewound_and_freshly_signed_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    payload = b"retry me"
    conflict = FakeHttpResponse(
        409,
        data=(
            b"<Error><Code>OperationAborted</Code>"
            b"<Message>conflicting operation</Message>"
            b"<RequestId>request-1</RequestId></Error>"
        ),
        headers={"content-type": "application/xml"},
    )
    http_client = RecordingHttpClient(conflict, FakeHttpResponse(200))
    client = MinioConditionalObjectClient(make_raw_client(http_client))
    request_times = iter(
        (
            datetime(2026, 7, 27, 12, 0, tzinfo=UTC),
            datetime(2026, 7, 27, 12, 0, 1, tzinfo=UTC),
        )
    )
    monkeypatch.setattr(
        "evidence_cartographer.infrastructure.conditional_object_client."
        "minio_time.utcnow",
        lambda: next(request_times),
    )

    client.put_object_if_absent(
        "bronze",
        "raw/source.csv",
        BytesIO(payload),
        len(payload),
        "c" * 64,
    )

    assert [request["body"] for request in http_client.requests] == [
        payload,
        payload,
    ]
    assert [request["headers"]["x-amz-date"] for request in http_client.requests] == [
        "20260727T120000Z",
        "20260727T120001Z",
    ]
    assert (
        http_client.requests[0]["headers"]["Authorization"]
        != http_client.requests[1]["headers"]["Authorization"]
    )


@pytest.mark.parametrize(
    "response",
    [
        FakeHttpResponse(
            412,
            data=(
                b"<Error><Code>PreconditionFailed</Code>"
                b"<Message>object exists</Message>"
                b"<RequestId>request-412</RequestId></Error>"
            ),
            headers={"content-type": "application/xml"},
        ),
        FakeHttpResponse(
            503,
            data=(
                b"<Error><Code>SlowDown</Code><Message>retry later</Message></Error>"
            ),
            headers={"content-type": "application/xml"},
        ),
        TimeoutError("response lost after upload"),
    ],
)
def test_reconciles_ambiguous_or_412_result_when_stored_metadata_matches(
    response: FakeHttpResponse | Exception,
) -> None:
    payload = b"committed bytes"
    digest = "d" * 64
    stat = Object(
        bucket_name="bronze",
        object_name="raw/source.csv",
        size=len(payload),
        metadata={
            "x-amz-meta-ec-sha256": digest,
            "x-amz-meta-ec-size": str(len(payload)),
        },
    )
    http_client = RecordingHttpClient(response)
    client = MinioConditionalObjectClient(make_reconciling_client(http_client, stat))

    client.put_object_if_absent(
        "bronze",
        "raw/source.csv",
        BytesIO(payload),
        len(payload),
        digest,
    )
    assert len(http_client.requests) == 1


def test_412_with_different_metadata_preserves_typed_s3_diagnostics() -> None:
    payload = b"our bytes"
    response = FakeHttpResponse(
        412,
        data=(
            b"<Error><Code>PreconditionFailed</Code>"
            b"<Message>object already exists</Message>"
            b"<RequestId>request-412</RequestId>"
            b"<HostId>host-412</HostId></Error>"
        ),
        headers={"content-type": "application/xml"},
    )
    stat = Object(
        bucket_name="bronze",
        object_name="raw/source.csv",
        size=5,
        metadata={
            "x-amz-meta-ec-sha256": "e" * 64,
            "x-amz-meta-ec-size": "5",
        },
    )
    client = MinioConditionalObjectClient(
        make_reconciling_client(RecordingHttpClient(response), stat)
    )

    with pytest.raises(ConditionalObjectExistsError) as raised:
        client.put_object_if_absent(
            "bronze",
            "raw/source.csv",
            BytesIO(payload),
            len(payload),
            "f" * 64,
        )

    assert raised.value.diagnostic.status_code == 412
    assert raised.value.diagnostic.code == "PreconditionFailed"
    assert raised.value.diagnostic.message == "object already exists"
    assert raised.value.diagnostic.request_id == "request-412"
    assert raised.value.diagnostic.host_id == "host-412"


@pytest.mark.parametrize(
    "first_result",
    [
        FakeHttpResponse(
            412,
            data=(
                b"<Error><Code>PreconditionFailed</Code>"
                b"<Message>object exists</Message></Error>"
            ),
            headers={"content-type": "application/xml"},
        ),
        TimeoutError("response lost"),
    ],
)
def test_retries_when_reconciliation_finds_no_object(
    first_result: FakeHttpResponse | Exception,
) -> None:
    payload = b"retry after stat"
    http_client = RecordingHttpClient(first_result, FakeHttpResponse(200))
    client = MinioConditionalObjectClient(
        make_sequenced_stat_client(
            http_client,
            missing_object_error("bronze", "raw/source.csv"),
        )
    )

    client.put_object_if_absent(
        "bronze",
        "raw/source.csv",
        BytesIO(payload),
        len(payload),
        "1" * 64,
    )

    assert [request["body"] for request in http_client.requests] == [
        payload,
        payload,
    ]


def test_exhausted_409_retries_raise_typed_diagnostics() -> None:
    responses = tuple(
        FakeHttpResponse(
            409,
            data=(
                b"<Error><Code>OperationAborted</Code>"
                b"<Message>conflicting operation</Message>"
                b"<RequestId>request-409</RequestId></Error>"
            ),
            headers={"content-type": "application/xml"},
        )
        for _ in range(3)
    )
    http_client = RecordingHttpClient(*responses)
    client = MinioConditionalObjectClient(make_raw_client(http_client))

    with pytest.raises(ConditionalPutError) as raised:
        client.put_object_if_absent(
            "bronze",
            "raw/source.csv",
            BytesIO(b"source"),
            6,
            "2" * 64,
        )

    assert len(http_client.requests) == 3
    assert raised.value.attempts == 3
    assert raised.value.diagnostic.status_code == 409
    assert raised.value.diagnostic.code == "OperationAborted"
    assert raised.value.diagnostic.message == "conflicting operation"
    assert raised.value.diagnostic.request_id == "request-409"
