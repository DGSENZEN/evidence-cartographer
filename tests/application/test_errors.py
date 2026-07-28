import pytest

from evidence_cartographer.application.errors import (
    AcquisitionError,
    ContractError,
    DownloadIntegrityError,
    DownloadSizeLimitError,
    EvidenceCartographerError,
    HttpDownloadError,
    MappingError,
    MetCsvParseError,
    MetCsvSchemaError,
    QualityError,
    StorageError,
)


@pytest.mark.parametrize(
    "error_type",
    [AcquisitionError, ContractError, StorageError, MappingError, QualityError],
)
def test_project_failures_share_a_typed_base_exception(
    error_type: type[EvidenceCartographerError],
) -> None:
    assert issubclass(error_type, EvidenceCartographerError)
    assert isinstance(error_type("synthetic failure"), EvidenceCartographerError)


def test_source_acquisition_errors_remain_typed() -> None:
    assert issubclass(HttpDownloadError, AcquisitionError)
    assert issubclass(DownloadIntegrityError, AcquisitionError)
    assert issubclass(DownloadSizeLimitError, AcquisitionError)
    assert issubclass(MetCsvSchemaError, ContractError)
    assert issubclass(MetCsvParseError, ContractError)
