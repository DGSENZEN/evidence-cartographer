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


class ArtifactNotFoundError(StorageError):
    """The local acquisition artifact is missing or is not a regular file."""


class ArtifactIntegrityError(StorageError):
    """The acquired artifact does not match its expected checksum."""


class ObjectAlreadyExistsError(StorageError):
    """A deterministic Bronze object key already exists."""


class ManifestSerializationError(StorageError):
    """Record evidence could not be serialized into the Bronze manifest."""


class ObjectStoreError(StorageError):
    """The object store rejected a Bronze operation."""
