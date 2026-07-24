from collections.abc import Callable

from prefect import Flow, flow

from evidence_cartographer.domain.enums import IngestionMode, SourceName

PipelineRunner = Callable[[SourceName, IngestionMode], None]


def build_ingestion_flow(run_pipeline: PipelineRunner) -> Flow[..., None]:
    @flow(name="collection-ingestion")
    def ingestion_flow(source: SourceName, mode: IngestionMode) -> None:
        run_pipeline(source, mode)

    return ingestion_flow
