import os
import pytest
from src.util.config import Config

def test_config_default_values(monkeypatch):
    """
    Tests that the Config class uses default values when
    environment variables are not set.
    """
    # Ensure that none of the environment variables we are testing are actually set
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.delenv("POSTGRES_PORT", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)

    # Since variables are read at module import time,
    # we need to reload the module for monkeypatch changes to take effect.
    import importlib
    from src.util import config
    importlib.reload(config)

    # Now we can access the reloaded class
    cfg = config.Config

    assert cfg.KAFKA_BOOTSTRAP_SERVERS == 'localhost:9093'
    assert cfg.POSTGRES_PORT == 5432
    assert cfg.LOG_LEVEL == 'INFO'

def test_config_custom_env_vars(monkeypatch):
    """
    Tests that the Config class correctly reads values
    from environment variables when they are set.
    """
    # Set custom values for environment variables
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-test:9092")
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    # Reload the config module to apply the new environment variables
    import importlib
    from src.util import config
    importlib.reload(config)
    
    cfg = config.Config

    assert cfg.KAFKA_BOOTSTRAP_SERVERS == "kafka-test:9092"
    assert cfg.POSTGRES_PORT == 5433
    assert cfg.LOG_LEVEL == "DEBUG"