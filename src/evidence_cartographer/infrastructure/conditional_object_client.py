from dataclasses import dataclass
from enum import Enum
from typing import BinaryIO, Protocol, runtime_checkable
from urllib.parse import urlunsplit

from minio import Minio
from minio import time as minio_time
from minio.error import S3Error
from minio.helpers import DictType
from minio.signer import sign_v4_s3
from urllib3.response import BaseHTTPResponse

from evidence_cartographer.application.errors import SinglePutSizeLimitError

MAX_SINGLE_PUT_SIZE_BYTES = 5 * 1024**3
MAX_CONDITIONAL_PUT_ATTEMPTS = 3
MISSING_OBJECT_CODES = frozenset({"NoSuchKey", "NoSuchObject", "NoSuchResource"})
RETRYABLE_STATUS_CODES = frozenset({409, 500, 502, 503, 504})
AMBIGUOUS_STATUS_CODES = frozenset({500, 502, 503, 504})


@dataclass(frozen=True, slots=True)
class ConditionalObjectMetadata:
    size_bytes: int
    sha256: str | None
    declared_size_bytes: int | None


@dataclass(frozen=True, slots=True)
class ConditionalPutDiagnostic:
    status_code: int | None
    code: str
    message: str
    request_id: str | None = None
    host_id: str | None = None


class ConditionalObjectExistsError(Exception):
    def __init__(self, diagnostic: ConditionalPutDiagnostic) -> None:
        self.diagnostic = diagnostic
        super().__init__(
            f"conditional object create failed: HTTP {diagnostic.status_code}, "
            f"code={diagnostic.code}, message={diagnostic.message}, "
            f"request_id={diagnostic.request_id}"
        )


class ConditionalPutError(Exception):
    def __init__(
        self,
        diagnostic: ConditionalPutDiagnostic,
        attempts: int,
        *,
        reconciliation_error: Exception | None = None,
    ) -> None:
        self.diagnostic = diagnostic
        self.attempts = attempts
        self.reconciliation_error = reconciliation_error
        reconciliation_detail = (
            f", reconciliation_error={reconciliation_error}"
            if reconciliation_error is not None
            else ""
        )
        super().__init__(
            f"conditional object create failed after {attempts} attempt(s): "
            f"HTTP {diagnostic.status_code}, code={diagnostic.code}, "
            f"message={diagnostic.message}, request_id={diagnostic.request_id}"
            f"{reconciliation_detail}"
        )


class _ReconciliationState(Enum):
    MATCH = "match"
    ABSENT = "absent"
    CONFLICT = "conflict"


@runtime_checkable
class ConditionalObjectClient(Protocol):
    def stat_object(
        self,
        bucket_name: str,
        object_name: str,
    ) -> ConditionalObjectMetadata: ...

    def put_object_if_absent(
        self,
        bucket_name: str,
        object_name: str,
        data: BinaryIO,
        length: int,
        sha256: str,
        content_type: str = "application/octet-stream",
    ) -> None: ...


class MinioConditionalObjectClient(ConditionalObjectClient):
    def __init__(self, client: Minio) -> None:
        self._client = client

    def stat_object(
        self,
        bucket_name: str,
        object_name: str,
    ) -> ConditionalObjectMetadata:
        stat = self._client.stat_object(bucket_name, object_name)
        metadata = {key.lower(): value for key, value in (stat.metadata or {}).items()}
        declared_size = metadata.get("x-amz-meta-ec-size")
        return ConditionalObjectMetadata(
            size_bytes=stat.size or 0,
            sha256=metadata.get("x-amz-meta-ec-sha256"),
            declared_size_bytes=(
                int(declared_size) if declared_size is not None else None
            ),
        )

    def put_object_if_absent(
        self,
        bucket_name: str,
        object_name: str,
        data: BinaryIO,
        length: int,
        sha256: str,
        content_type: str = "application/octet-stream",
    ) -> None:
        if length > MAX_SINGLE_PUT_SIZE_BYTES:
            raise SinglePutSizeLimitError(
                f"s3://{bucket_name}/{object_name}",
                length,
                MAX_SINGLE_PUT_SIZE_BYTES,
            )
        start_position = data.tell()
        for attempt in range(MAX_CONDITIONAL_PUT_ATTEMPTS):
            data.seek(start_position)
            try:
                response = self._send_once(
                    bucket_name,
                    object_name,
                    data,
                    length,
                    sha256,
                    content_type,
                )
            except Exception as exc:
                diagnostic = _exception_diagnostic(exc)
                try:
                    state = self._reconcile_object(
                        bucket_name,
                        object_name,
                        length,
                        sha256,
                    )
                except Exception as reconciliation_error:
                    raise ConditionalPutError(
                        diagnostic,
                        attempt + 1,
                        reconciliation_error=reconciliation_error,
                    ) from exc
                if state is _ReconciliationState.MATCH:
                    return
                if state is _ReconciliationState.CONFLICT:
                    raise ConditionalObjectExistsError(diagnostic) from exc
                if attempt + 1 < MAX_CONDITIONAL_PUT_ATTEMPTS:
                    continue
                raise ConditionalPutError(diagnostic, attempt + 1) from exc
            if response.status in (200, 204):
                return
            diagnostic = _response_diagnostic(response)
            if response.status == 412:
                try:
                    state = self._reconcile_object(
                        bucket_name,
                        object_name,
                        length,
                        sha256,
                    )
                except Exception as reconciliation_error:
                    raise ConditionalPutError(
                        diagnostic,
                        attempt + 1,
                        reconciliation_error=reconciliation_error,
                    ) from reconciliation_error
                if state is _ReconciliationState.MATCH:
                    return
                if state is _ReconciliationState.CONFLICT:
                    raise ConditionalObjectExistsError(diagnostic)
                if attempt + 1 < MAX_CONDITIONAL_PUT_ATTEMPTS:
                    continue
                raise ConditionalPutError(diagnostic, attempt + 1)
            if response.status in AMBIGUOUS_STATUS_CODES:
                try:
                    state = self._reconcile_object(
                        bucket_name,
                        object_name,
                        length,
                        sha256,
                    )
                except Exception as reconciliation_error:
                    raise ConditionalPutError(
                        diagnostic,
                        attempt + 1,
                        reconciliation_error=reconciliation_error,
                    ) from reconciliation_error
                if state is _ReconciliationState.MATCH:
                    return
                if state is _ReconciliationState.CONFLICT:
                    raise ConditionalObjectExistsError(diagnostic)
                if attempt + 1 < MAX_CONDITIONAL_PUT_ATTEMPTS:
                    continue
                raise ConditionalPutError(diagnostic, attempt + 1)
            if response.status in RETRYABLE_STATUS_CODES:
                if attempt + 1 < MAX_CONDITIONAL_PUT_ATTEMPTS:
                    continue
                raise ConditionalPutError(diagnostic, attempt + 1)
            raise ConditionalPutError(diagnostic, attempt + 1)

    def _reconcile_object(
        self,
        bucket_name: str,
        object_name: str,
        length: int,
        sha256: str,
    ) -> _ReconciliationState:
        try:
            metadata = self.stat_object(bucket_name, object_name)
        except S3Error as exc:
            if exc.code in MISSING_OBJECT_CODES:
                return _ReconciliationState.ABSENT
            raise
        if (
            metadata.size_bytes == length
            and metadata.declared_size_bytes == length
            and metadata.sha256 == sha256
        ):
            return _ReconciliationState.MATCH
        return _ReconciliationState.CONFLICT

    def _send_once(
        self,
        bucket_name: str,
        object_name: str,
        data: BinaryIO,
        length: int,
        sha256: str,
        content_type: str,
    ) -> BaseHTTPResponse:
        region = self._client._get_region(bucket_name)
        url = self._client._base_url.build(
            method="PUT",
            region=region,
            bucket_name=bucket_name,
            object_name=object_name,
        )
        request_time = minio_time.utcnow()
        headers: DictType = {
            "Content-Length": str(length),
            "Content-Type": content_type,
            "Host": url.netloc,
            "If-None-Match": "*",
            "User-Agent": self._client._user_agent,
            "x-amz-date": minio_time.to_amz_date(request_time),
            "x-amz-meta-ec-sha256": sha256,
            "x-amz-meta-ec-size": str(length),
        }
        credentials = (
            self._client._provider.retrieve() if self._client._provider else None
        )
        if credentials:
            content_sha256 = (
                "UNSIGNED-PAYLOAD" if self._client._base_url.is_https else sha256
            )
            headers["x-amz-content-sha256"] = content_sha256
            if credentials.session_token:
                headers["X-Amz-Security-Token"] = credentials.session_token
            headers = sign_v4_s3(
                method="PUT",
                url=url,
                region=region,
                headers=headers,
                credentials=credentials,
                content_sha256=content_sha256,
                date=request_time,
            )
        return self._client._http.urlopen(
            "PUT",
            urlunsplit(url),
            body=data,
            headers=headers,
            preload_content=True,
            retries=False,
            redirect=False,
        )


def _response_diagnostic(response: BaseHTTPResponse) -> ConditionalPutDiagnostic:
    try:
        error = S3Error.fromxml(response)
    except Exception:
        return ConditionalPutDiagnostic(
            status_code=response.status,
            code=f"HTTP{response.status}",
            message=(
                response.data.decode("utf-8", errors="replace")
                if response.data
                else "object store returned an empty error response"
            ),
            request_id=response.headers.get("x-amz-request-id"),
            host_id=response.headers.get("x-amz-id-2"),
        )
    return ConditionalPutDiagnostic(
        status_code=response.status,
        code=error.code or f"HTTP{response.status}",
        message=error.message or "object store rejected conditional create",
        request_id=error.request_id,
        host_id=error.host_id,
    )


def _exception_diagnostic(exc: Exception) -> ConditionalPutDiagnostic:
    return ConditionalPutDiagnostic(
        status_code=None,
        code=type(exc).__name__,
        message=str(exc) or "conditional object request failed",
    )
