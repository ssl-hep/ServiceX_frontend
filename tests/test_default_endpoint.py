from unittest.mock import patch

from servicex.servicex_client import ServiceXClient


def test_default_endpoint(codegen_list):
    with patch('servicex.servicex_client.ServiceXClient.get_code_generators',
               return_value=codegen_list):
        sx = ServiceXClient(config_path="tests/example_config.yaml")
        assert sx.servicex.url == "http://localhost:5000"


def test_first_endpoint(codegen_list):
    with patch('servicex.servicex_client.ServiceXClient.get_code_generators',
               return_value=codegen_list):
        sx = ServiceXClient(config_path="tests/example_config_default_endpoint.yaml")
        assert sx.servicex.url == "https://servicex.af.uchicago.edu"
