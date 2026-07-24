from pytest import MonkeyPatch

from evidence_cartographer.infrastructure.settings import Settings


def test_settings_load_nested_environment_values(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("EC_POSTGRES__PASSWORD", "test-secret")
    monkeypatch.setenv("EC_OBJECT_STORE__ENDPOINT", "minio.internal:9000")

    settings = Settings(_env_file=None)

    assert settings.postgres.password.get_secret_value() == "test-secret"
    assert settings.object_store.endpoint == "minio.internal:9000"
    assert settings.refresh.weekly_full_cron == "0 2 * * 0"
    assert settings.refresh.daily_incremental_cron == "0 3 * * *"
