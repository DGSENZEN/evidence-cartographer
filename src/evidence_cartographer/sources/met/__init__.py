from evidence_cartographer.domain.enums import SourceName
from evidence_cartographer.sources.catalog import SourceDescriptor

MET_SOURCE = SourceDescriptor(
    source=SourceName.MET,
    display_name="The Metropolitan Museum of Art",
    bulk_format="csv",
    supports_incremental_api=True,
    contract_version="1.0.0",
)
