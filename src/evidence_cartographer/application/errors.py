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
