from pydantic import ValidationError
from pytest import MonkeyPatch, raises

from evidence_cartographer.infrastructure.settings import Settings


def test_settings_load_nested_environment_values(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("EC_POSTGRES__PASSWORD", "test-secret")
    monkeypatch.setenv("EC_OBJECT_STORE__ENDPOINT", "minio.internal:9000")
    monkeypatch.setenv("EC_OBJECT_STORE__ACCESS_KEY", "test-access-key")
    monkeypatch.setenv("EC_OBJECT_STORE__SECRET_KEY", "test-secret-key")

    settings = Settings(_env_file=None)

    assert settings.postgres.password.get_secret_value() == "test-secret"
    assert str(settings.postgres.password) == "**********"
    assert settings.object_store.endpoint == "minio.internal:9000"
    assert settings.object_store.access_key == "test-access-key"
    assert settings.object_store.secret_key.get_secret_value() == "test-secret-key"
    assert str(settings.object_store.secret_key) == "**********"
    assert settings.refresh.weekly_full_cron == "0 2 * * 0"
    assert settings.refresh.daily_incremental_cron == "0 3 * * *"


def test_settings_require_credentials(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.delenv("EC_POSTGRES__PASSWORD", raising=False)
    monkeypatch.delenv("EC_OBJECT_STORE__ACCESS_KEY", raising=False)
    monkeypatch.delenv("EC_OBJECT_STORE__SECRET_KEY", raising=False)

    with raises(ValidationError):
        Settings(_env_file=None)
