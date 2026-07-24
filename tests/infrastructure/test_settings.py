from pydantic import ValidationError
from pytest import MonkeyPatch, raises

from evidence_cartographer.infrastructure.settings import ImageCacheSelection, Settings


def test_settings_load_nested_environment_values(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("EC_POSTGRES__PASSWORD", "test-secret")
    monkeypatch.setenv("EC_OBJECT_STORE__ENDPOINT", "minio.internal:9000")
    monkeypatch.setenv("EC_OBJECT_STORE__ACCESS_KEY", "test-access-key")
    monkeypatch.setenv("EC_OBJECT_STORE__SECRET_KEY", "test-secret-key")
    monkeypatch.setenv("EC_LAKE__BRONZE_PREFIX", "lake/raw")
    monkeypatch.setenv("EC_LAKE__MANIFEST_PREFIX", "lake/manifests")
    monkeypatch.setenv("EC_LAKE__QUARANTINE_PREFIX", "lake/quarantine")
    monkeypatch.setenv("EC_LAKE__SILVER_PREFIX", "lake/silver")
    monkeypatch.setenv("EC_LAKE__GOLD_PREFIX", "lake/gold")
    monkeypatch.setenv("EC_PREFECT__API_URL", "http://127.0.0.1:4200/api")
    monkeypatch.setenv("EC_PREFECT__WORK_POOL_NAME", "local-process")
    monkeypatch.setenv("EC_CONTRACTS__MET_VERSION", "1.1.0")
    monkeypatch.setenv("EC_CONTRACTS__AIC_VERSION", "1.2.0")
    monkeypatch.setenv("EC_IMAGE_CACHE__ENABLED", "true")
    monkeypatch.setenv("EC_IMAGE_CACHE__PREFIX", "cached-images")
    monkeypatch.setenv("EC_IMAGE_CACHE__SELECTION", "primary_only")

    settings = Settings(_env_file=None)

    assert settings.postgres.password.get_secret_value() == "test-secret"
    assert str(settings.postgres.password) == "**********"
    assert settings.object_store.endpoint == "minio.internal:9000"
    assert settings.object_store.access_key == "test-access-key"
    assert settings.object_store.secret_key.get_secret_value() == "test-secret-key"
    assert str(settings.object_store.secret_key) == "**********"
    assert settings.refresh.weekly_full_cron == "0 2 * * 0"
    assert settings.refresh.daily_incremental_cron == "0 3 * * *"
    assert settings.lake.bronze_prefix == "lake/raw"
    assert settings.lake.manifest_prefix == "lake/manifests"
    assert settings.lake.quarantine_prefix == "lake/quarantine"
    assert settings.lake.silver_prefix == "lake/silver"
    assert settings.lake.gold_prefix == "lake/gold"
    assert settings.prefect.api_url == "http://127.0.0.1:4200/api"
    assert settings.prefect.work_pool_name == "local-process"
    assert settings.contracts.met_version == "1.1.0"
    assert settings.contracts.aic_version == "1.2.0"
    assert settings.image_cache.enabled
    assert settings.image_cache.prefix == "cached-images"
    assert settings.image_cache.selection is ImageCacheSelection.PRIMARY_ONLY


def test_settings_require_credentials(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.delenv("EC_POSTGRES__PASSWORD", raising=False)
    monkeypatch.delenv("EC_OBJECT_STORE__ACCESS_KEY", raising=False)
    monkeypatch.delenv("EC_OBJECT_STORE__SECRET_KEY", raising=False)

    with raises(ValidationError):
        Settings(_env_file=None)


def test_settings_reject_empty_credentials(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("EC_POSTGRES__PASSWORD", "")
    monkeypatch.setenv("EC_OBJECT_STORE__ACCESS_KEY", "")
    monkeypatch.setenv("EC_OBJECT_STORE__SECRET_KEY", "")

    with raises(ValidationError):
        Settings(_env_file=None)


def test_empty_optional_prefect_values_normalize_to_none(
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setenv("EC_POSTGRES__PASSWORD", "test-secret")
    monkeypatch.setenv("EC_OBJECT_STORE__ACCESS_KEY", "test-access-key")
    monkeypatch.setenv("EC_OBJECT_STORE__SECRET_KEY", "test-secret-key")
    monkeypatch.setenv("EC_PREFECT__API_URL", "")
    monkeypatch.setenv("EC_PREFECT__WORK_POOL_NAME", "")

    settings = Settings(_env_file=None)

    assert settings.prefect.api_url is None
    assert settings.prefect.work_pool_name is None
