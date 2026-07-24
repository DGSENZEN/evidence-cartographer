from evidence_cartographer.domain.enums import IngestionMode, SourceName
from evidence_cartographer.orchestration.deployments import PrefectDeploymentConfig


def test_prefect_deployment_config_carries_schedule_and_pipeline_inputs() -> None:
    config = PrefectDeploymentConfig(
        source=SourceName.AIC,
        mode=IngestionMode.INCREMENTAL,
        cron="0 3 * * *",
        timezone="America/Chicago",
        work_pool_name="local-process",
    )

    assert config.source is SourceName.AIC
    assert config.mode is IngestionMode.INCREMENTAL
    assert config.cron == "0 3 * * *"
    assert config.timezone == "America/Chicago"
    assert config.work_pool_name == "local-process"


def test_prefect_deployment_config_allows_no_work_pool() -> None:
    config = PrefectDeploymentConfig(
        source=SourceName.MET,
        mode=IngestionMode.FULL_SNAPSHOT,
        cron="0 2 * * 0",
        timezone="UTC",
    )

    assert config.work_pool_name is None
