"""Unit tests for hr_pipeline.config."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from hr_pipeline.config import Environment, Settings

_ENV_VARS = ["HR_ENV", "DUCKDB_PATH", "HR_RAW_DATA_DIR", "GCP_PROJECT", "BQ_LOCATION"]


def test_from_env_defaults_to_local(monkeypatch):
    for var in _ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    settings = Settings.from_env()
    assert settings.environment is Environment.LOCAL
    assert settings.is_production is False
    assert settings.dbt_target == "dev"


def test_production_requires_gcp_project(monkeypatch):
    monkeypatch.setenv("HR_ENV", "production")
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    with pytest.raises(ValueError, match="GCP_PROJECT"):
        Settings.from_env()


def test_production_target_resolves(monkeypatch):
    monkeypatch.setenv("HR_ENV", "production")
    monkeypatch.setenv("GCP_PROJECT", "demo-project")
    settings = Settings.from_env()
    assert settings.is_production is True
    assert settings.dbt_target == "prod"


def test_invalid_environment_is_rejected(monkeypatch):
    monkeypatch.setenv("HR_ENV", "staging")
    with pytest.raises(ValueError, match="Invalid HR_ENV"):
        Settings.from_env()


def test_staging_dir_is_derived(local_settings):
    assert local_settings.staging_dir.name == "_staging"
    assert local_settings.staging_dir.parent == local_settings.raw_data_dir.parent


def test_settings_is_immutable(local_settings):
    with pytest.raises(FrozenInstanceError):  # frozen dataclass
        local_settings.environment = Environment.PRODUCTION
