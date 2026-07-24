from evidence_cartographer.domain.enums import SourceName
from evidence_cartographer.sources.aic import AIC_SOURCE
from evidence_cartographer.sources.met import MET_SOURCE


def test_met_descriptor_separates_bulk_and_api_acquisition() -> None:
    assert MET_SOURCE.source is SourceName.MET
    assert MET_SOURCE.bulk_format == "csv"
    assert MET_SOURCE.supports_incremental_api


def test_aic_descriptor_separates_bulk_and_api_acquisition() -> None:
    assert AIC_SOURCE.source is SourceName.AIC
    assert AIC_SOURCE.bulk_format == "json"
    assert AIC_SOURCE.supports_incremental_api
