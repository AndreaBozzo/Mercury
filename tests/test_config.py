import pytest

from core.config import ConfigurationError, env_bool, env_positive_float


def test_env_bool_uses_default_and_parses_explicit_values(monkeypatch):
    monkeypatch.delenv("MERCURY_TEST_FLAG", raising=False)
    assert env_bool("MERCURY_TEST_FLAG", True) is True

    monkeypatch.setenv("MERCURY_TEST_FLAG", "off")
    assert env_bool("MERCURY_TEST_FLAG", True) is False


def test_env_bool_rejects_typos_instead_of_changing_safety_mode(monkeypatch):
    monkeypatch.setenv("MERCURY_TEST_FLAG", "flase")

    with pytest.raises(ConfigurationError, match="boolean"):
        env_bool("MERCURY_TEST_FLAG", True)


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "not-a-number"])
def test_positive_float_rejects_unsafe_values(monkeypatch, value):
    monkeypatch.setenv("MERCURY_TEST_LIMIT", value)

    with pytest.raises(ConfigurationError, match="positive finite"):
        env_positive_float("MERCURY_TEST_LIMIT", 1.0)
