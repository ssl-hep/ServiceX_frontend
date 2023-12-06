from unittest.mock import patch
import pytest
from pathlib import Path
import yaml

from servicex.models import TransformedResults
# from servicex.dataset import Dataset

from servicex.databinder.databinder_configuration \
    import load_databinder_config, _set_default_values, \
    _support_old_option_names
from servicex.databinder.databinder_requests import DataBinderRequests
from servicex.databinder.databinder_outputs import OutputHandler
from servicex.databinder.databinder_deliver import DataBinderDeliver
from servicex.databinder.databinder import DataBinder


def test_load_config():
    config = {
        "General":
        {
            "ServiceX": "servicex",
            "Codegen": "python",
            "Delivery": "ObjectStore"
        },
        "Sample":
        [
            {
                "Name": "sampleA",
                "RucioDID": "user.kchoi:sampleA",
                "Function": "DEF_a"
            },
            {
                "Name": "sampleB",
                "XRootDFiles": "root://a.root",
                "Columns": "el_pt",
                "Codegen": "uproot"
            }
        ],
        "Definition":
        {
            "DEF_a": "a"
        }
    }
    new_config = load_databinder_config(config)
    assert new_config["Sample"][0]["Function"] == "a"

    with open("temp_databinder.yaml", "w") as f:
        yaml.dump(new_config, f, default_flow_style=False)
    new_config = load_databinder_config("temp_databinder.yaml")
    assert new_config["Sample"][0]["Function"] == "a"


def test_config_default_value():
    config = {
        "General": {
            "IgnoreLocalCache": True
        },
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.kchoi:sampleA",
                "Function": "DEF_a"
            }
        ]
    }
    new_config = _set_default_values(config)
    assert new_config["General"]["Delivery"] == "localpath"


def test_config_old_option_names():
    config = {
        "General": {
            "ServiceXName": "servicex",
            "Transformer": "uproot",
            "IgnoreServiceXCache": True
        },
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.kchoi:sampleA",
                "Transformer": "python",
                "Function": "DEF_a",
                "IgnoreServiceXCache": True
            }
        ]
    }
    new_config = _support_old_option_names(config)
    assert new_config["General"]["ServiceX"] == "servicex"


@patch('servicex.databinder.databinder_requests.DataBinderRequests._get_client')
def test_requests_python_transformer(_get_client):
    config = {
        "General":
        {
            "ServiceX": "servicex",
            "OutputFormat": "root",
            "Delivery": "objectstore"
        },
        "Sample":
        [
            {
                "Name": "sampleA",
                "RucioDID": "user.kchoi:sampleA",
                "Codegen": "python",
                "Function": "DEF_a",
                "NFiles": "5",
                "IgnoreLocalCache": "False"
            },
            {
                "Name": "sampleB",
                "XRootDFiles": "root://a.root",
                "Function": "DEF_a",
                "Codegen": "python",
                "IgnoreLocalCache": "False"
            }
        ]
    }
    reqs = DataBinderRequests(config).get_requests()
    assert reqs[1]["sample_name"] == "sampleB"
    assert len(reqs) == 2
    assert len(reqs[0].keys()) == 3
    # assert isinstance(reqs[0]["ds_query"], Dataset)


def test_output_handler():
    config = {
        "General":
        {
            "OutputDirectory": "./temp_dir2",
            "Delivery": "objectstore",
            "OutFilesetName": "out_dict"
        },
        "Sample":
        [
            {
                "Name": "sampleA",
            },
            {
                "Name": "sampleB",
            }
        ]
    }
    result = TransformedResults
    result.title = "sampleA"
    result.signed_url_list = ["a", "b"]
    result.file_list = ["c", "d"]

    out = OutputHandler(config)

    out.update_out_dict("objectstore", result)
    assert len(out.out_dict['samples']['sampleA']) == 2

    out.write_out_dict()
    with open(Path("temp_dir2/out_dict.yml"), "r") as f:
        out_yaml = yaml.safe_load(f)
    assert len(out_yaml["samples"]["sampleA"]) == 2


@pytest.mark.asyncio
@patch('servicex.databinder.databinder_requests.DataBinderRequests._get_client')
async def test_deliver(_get_client):
    config = {
        "General":
        {
            "ServiceX": "servicex",
            "OutputDirectory": "./temp_dir2",
            "Delivery": "objectstore",
            "OutFilesetName": "out_dict",
            "OutputFormat": "root"
        },
        "Sample":
        [
            {
                "Name": "sampleA",
                "RucioDID": "user.kchoi:sampleA",
                "Codegen": "python",
                "Function": "DEF_a",
                "NFiles": "5",
                "IgnoreLocalCache": "False"
            },
            {
                "Name": "sampleB",
                "XRootDFiles": "root://a.root",
                "Function": "DEF_a",
                "Codegen": "python",
                "IgnoreLocalCache": "False"
            }
        ]
    }
    deliv = DataBinderDeliver(config)

    assert deliv._requests[0]['sample_name'] == "sampleA"
    # assert deliv.deliver_and_copy()


@patch('servicex.databinder.databinder_requests.DataBinderRequests._get_client')
def test_databinder(_get_client):
    config = {
        "General":
        {
            "ServiceX": "servicex",
            "OutputDirectory": "./temp_dir2",
            "OutFilesetName": "out_dict",
        },
        "Sample":
        [
            {
                "Name": "sampleA",
                "RucioDID": "user.kchoi:sampleA",
                "Codegen": "python",
                "Function": "DEF_a",
                "NFiles": "5",
            },
            {
                "Name": "sampleB",
                "XRootDFiles": "root://a.root",
                "Function": "DEF_a",
                "Codegen": "python",
            }
        ]
    }
    sx_db = DataBinder(config)

    assert sx_db._config["General"]["OutputFormat"] == "root"
