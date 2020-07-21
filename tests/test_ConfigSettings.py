from servicex.ConfigSettings import ConfigSettings
from pathlib import Path

import pytest


@pytest.fixture
def confuse_config():
    config = ConfigSettings('servicex_test_settings', "servicex")
    config.clear()
    config.read(user=False)
    return config


def get_it(c: ConfigSettings):
    return c['testing_value'].get(int)


def test_package_default(confuse_config):
    assert get_it(confuse_config) == 10


def test_local_file_default(confuse_config):
    config_file = Path('.servicex_test_settings')
    try:
        with config_file.open('w') as f:
            f.write('testing_value: 20\n')

        confuse_config.clear()
        confuse_config.read()

        assert get_it(confuse_config) == 20

    finally:
        config_file.unlink()


def test_home_file_default(confuse_config):
    config_file = Path.home() / '.servicex_test_settings'
    try:
        with config_file.open('w') as f:
            f.write('testing_value: 30\n')

        confuse_config.clear()
        confuse_config.read()

        assert get_it(confuse_config) == 30

    finally:
        config_file.unlink()
