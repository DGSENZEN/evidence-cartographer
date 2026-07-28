class EvidenceCartographerError(Exception):
    """Base class for expected project failures."""


class AcquisitionError(EvidenceCartographerError):
    """A source acquisition failed."""


class ContractError(EvidenceCartographerError):
    """A source contract could not be applied."""


class StorageError(EvidenceCartographerError):
    """A storage boundary failed."""


class MappingError(EvidenceCartographerError):
    """A source record could not be mapped."""


class QualityError(EvidenceCartographerError):
    """A data-quality operation failed."""


class HttpDownloadError(AcquisitionError):
    """A source HTTP request failed or returned a terminal status."""


class DownloadIntegrityError(AcquisitionError):
    """A downloaded artifact is empty, truncated, or length-mismatched."""


class DownloadSizeLimitError(AcquisitionError):
    """A downloaded artifact exceeds the supported artifact ceiling."""


class MetCsvSchemaError(ContractError):
    """The Met CSV header does not satisfy the versioned contract."""


class MetCsvParseError(ContractError):
    """The Met CSV cannot be segmented into readable records."""


class ArtifactNotFoundError(StorageError):
    """The local acquisition artifact is missing or is not a regular file."""


class ArtifactIntegrityError(StorageError):
    """The acquired artifact does not match its expected checksum."""


class ArtifactStagingError(StorageError):
    """The acquisition artifact could not be staged for upload."""


class EvidenceStagingError(StorageError):
    """The record-evidence payload could not be staged for upload."""


class ObjectAlreadyExistsError(StorageError):
    """A deterministic Bronze object key already exists."""


class ManifestSerializationError(StorageError):
    """Record evidence could not be serialized into the Bronze manifest."""


class ObjectStoreError(StorageError):
    """The object store rejected a Bronze operation."""


class SinglePutSizeLimitError(StorageError):
    """A payload exceeds the supported conditional single-PUT ceiling."""

    def __init__(
        self,
        object_uri: str,
        size_bytes: int,
        max_size_bytes: int,
    ) -> None:
        self.object_uri = object_uri
        self.size_bytes = size_bytes
        self.max_size_bytes = max_size_bytes
        super().__init__(
            f"{object_uri} is {size_bytes} bytes; conditional single-PUT supports "
            f"at most {max_size_bytes} bytes"
        )
