from evidence_cartographer.domain.enums import SourceName
from evidence_cartographer.sources.catalog import SourceDescriptor

AIC_SOURCE = SourceDescriptor(
    source=SourceName.AIC,
    display_name="Art Institute of Chicago",
    bulk_format="json",
    supports_incremental_api=True,
    contract_version="1.0.0",
)
