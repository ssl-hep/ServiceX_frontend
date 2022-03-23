from servicex.ConfigSettings import ConfigSettings
from pathlib import Path

import pytest


@pytest.fixture
def confuse_config():
    config = ConfigSettings("servicex_test_settings", "servicex")
    config.clear()
    config.read(user=False)
    return config


def get_it(c: ConfigSettings):
    return c["testing_value"].get(int)


def test_package_default(confuse_config):
    assert get_it(confuse_config) == 10


def test_local_file_default(confuse_config):
    config_file = Path(".servicex_test_settings")
    try:
        with config_file.open("w") as f:
            f.write("testing_value: 20\n")

        confuse_config.clear()
        confuse_config.read()

        assert get_it(confuse_config) == 20

    finally:
        config_file.unlink()


def test_home_file_default(confuse_config):
    config_file = Path.home() / ".servicex_test_settings"
    try:
        with config_file.open("w") as f:
            f.write("testing_value: 30\n")

        confuse_config.clear()
        confuse_config.read()

        assert get_it(confuse_config) == 30

    finally:
        config_file.unlink()


def test_local_more_important_than_home(confuse_config):
    "Make sure that we pick the local directory over the home one"
    local_config_file = Path(".servicex_test_settings")
    home_config_file = Path.home() / ".servicex_test_settings"
    try:
        with home_config_file.open("w") as f:
            f.write("testing_value: 30\n")
        with local_config_file.open("w") as f:
            f.write("testing_value: 20\n")

        confuse_config.clear()
        confuse_config.read()

        assert get_it(confuse_config) == 20

    finally:
        local_config_file.unlink()
        home_config_file.unlink()


def test_one_level_up(confuse_config):
    "Make sure the config file that is one file up is found"
    config_file = Path(".").resolve().parent / (".servicex_test_settings")
    try:
        with config_file.open("w") as f:
            f.write("testing_value: 30\n")

        confuse_config.clear()
        confuse_config.read()

        assert get_it(confuse_config) == 30

    finally:
        config_file.unlink()


def test_local_more_importnat_than_one_level_up(confuse_config):
    "Assure that our local file is found first"
    one_up_config_file = Path(".").resolve().parent / (".servicex_test_settings")
    config_file = Path(".").resolve() / (".servicex_test_settings")
    try:
        with config_file.open("w") as f:
            f.write("testing_value: 30\n")
        with one_up_config_file.open("w") as f:
            f.write("testing_value: 20\n")

        confuse_config.clear()
        confuse_config.read()

        assert get_it(confuse_config) == 30

    finally:
        config_file.unlink()
        one_up_config_file.unlink()
