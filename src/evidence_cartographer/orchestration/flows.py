from collections.abc import Callable

from prefect import Flow, flow

from evidence_cartographer.application.retry import RetryDecider
from evidence_cartographer.domain.enums import IngestionMode, SourceName

PipelineRunner = Callable[[SourceName, IngestionMode, RetryDecider], None]


def build_ingestion_flow(
    run_pipeline: PipelineRunner,
    retry_decider: RetryDecider,
) -> Flow[..., None]:
    @flow(name="collection-ingestion")
    def ingestion_flow(source: SourceName, mode: IngestionMode) -> None:
        run_pipeline(source, mode, retry_decider)

    return ingestion_flow
