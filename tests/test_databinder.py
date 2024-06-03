import pytest
from unittest.mock import patch
from pydantic import ValidationError

from servicex import ServiceXSpec, FileListDataset, RucioDatasetIdentifier


def basic_spec(samples=None):
    return {
        "General": {
            "ServiceX": "servicex",
            "Codegen": "python",
        },
        "Sample": samples
        or [{"Name": "sampleA", "XRootDFiles": "root://a.root", "Function": "a"}],
    }


def test_load_config():
    config = {
        "General": {
            "ServiceX": "servicex",
            "Codegen": "python",
            "Delivery": "LocalCache",
        },
        "Sample": [
            {"Name": "sampleA", "RucioDID": "user.kchoi:sampleA", "Function": "DEF_a"},
            {
                "Name": "sampleB",
                "XRootDFiles": "root://a.root",
                "Columns": "el_pt",
                "Codegen": "uproot",
                "Function": "DEF_a",
            },
        ],
        "Definition": {"DEF_a": "a"},
    }
    new_config = ServiceXSpec.model_validate(config)
    assert new_config.Sample[0].Function == "a"


def test_single_root_file():

    spec = ServiceXSpec.model_validate(
        basic_spec(
            samples=[
                {
                    "Name": "sampleA",
                    "XRootDFiles": "root://eospublic.cern.ch//file1.root",
                    "Function": "a",
                }
            ]
        )
    )

    assert isinstance(spec.Sample[0].dataset_identifier, FileListDataset)
    assert spec.Sample[0].dataset_identifier.files == [
        "root://eospublic.cern.ch//file1.root"
    ]


def test_list_of_root_files():
    spec = ServiceXSpec.model_validate(
        basic_spec(
            samples=[
                {
                    "Name": "sampleA",
                    "XRootDFiles": [
                        "root://eospublic.cern.ch//file1.root",
                        "root://eospublic.cern.ch//file2.root",
                    ],
                    "Function": "a",
                }
            ]
        )
    )

    assert isinstance(spec.Sample[0].dataset_identifier, FileListDataset)
    assert spec.Sample[0].dataset_identifier.files == [
        "root://eospublic.cern.ch//file1.root",
        "root://eospublic.cern.ch//file2.root",
    ]


def test_rucio_did():
    spec = ServiceXSpec.model_validate(
        basic_spec(
            samples=[
                {
                    "Name": "sampleA",
                    "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                    "Function": "a",
                }
            ]
        )
    )

    assert isinstance(spec.Sample[0].dataset_identifier, RucioDatasetIdentifier)
    assert (
        spec.Sample[0].dataset_identifier.did
        == "rucio://user.ivukotic:user.ivukotic.single_top_tW__nominal"
    )


def test_rucio_did_numfiles():
    spec = ServiceXSpec.model_validate(
        basic_spec(
            samples=[
                {
                    "Name": "sampleA",
                    "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                    "NFiles": 10,
                    "Function": "a",
                }
            ]
        )
    )

    assert isinstance(spec.Sample[0].dataset_identifier, RucioDatasetIdentifier)
    assert (
        spec.Sample[0].dataset_identifier.did
        == "rucio://user.ivukotic:user.ivukotic.single_top_tW__nominal?files=10"
    )


def test_invalid_dataset_identifier():
    with pytest.raises(ValidationError):
        ServiceXSpec.model_validate(
            basic_spec(
                samples=[
                    {
                        "Name": "sampleA",
                        "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                        "XRootDFiles": "root://eospublic.cern.ch//file1.root",
                        "NFiles": 10,
                        "Function": "a",
                    }
                ]
            )
        )

    with pytest.raises(ValidationError):
        ServiceXSpec.model_validate(
            basic_spec(
                samples=[
                    {
                        "Name": "sampleA",
                        "NFiles": 10,
                        "Function": "a",
                    }
                ]
            )
        )


def test_string_query(transformed_result):
    from servicex import deliver
    spec = ServiceXSpec.model_validate({
        "General": {
            "ServiceX": "testing4",
            "Codegen": "uproot-raw",
        },
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": "[{'treename': 'nominal'}]"
            }
        ]
    })
    with patch('servicex.dataset_group.DatasetGroup.as_files',
               return_value=[transformed_result]):
        deliver(spec, config_path='tests/example_config.yaml')


def test_funcadl_query(transformed_result):
    from servicex import deliver
    from servicex.func_adl.func_adl_dataset import FuncADLQuery
    spec = ServiceXSpec.model_validate({
        "General": {
            "ServiceX": "testing4",
            "Codegen": "uproot",
        },
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": FuncADLQuery().Select(lambda e: {"lep_pt": e["lep_pt"]}),
                "Tree": "nominal"
            }
        ]
    })
    with patch('servicex.dataset_group.DatasetGroup.as_files',
               return_value=[transformed_result]):
        deliver(spec, config_path='tests/example_config.yaml')


def test_python_query(transformed_result):
    from servicex import deliver
    string_function = """
def run_query(input_filenames=None):
    print("Greetings from your query")
    return []
"""
    spec = ServiceXSpec.model_validate({
        "General": {
            "ServiceX": "testing4",
            "Codegen": "uproot-raw",
        },
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Function": string_function
            }
        ]
    })
    with patch('servicex.dataset_group.DatasetGroup.as_files',
               return_value=[transformed_result]):
        deliver(spec, config_path='tests/example_config.yaml')


def test_generic_query():
    from servicex.servicex_client import ServiceXClient
    spec = ServiceXSpec.model_validate({
        "General": {
            "ServiceX": "testing4",
            "Codegen": "uproot-raw",
        },
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": "[{'treename': 'nominal'}]"
            }
        ]
    })
    sx = ServiceXClient(backend=spec.General.ServiceX, config_path='tests/example_config.yaml')
    query = sx.generic_query(dataset_identifier=spec.Sample[0].RucioDID,
                             codegen=spec.General.Codegen, query=spec.Sample[0].Query)
    assert query.generate_selection_string() == "[{'treename': 'nominal'}]"
    query.query_string_generator = None
    with pytest.raises(RuntimeError):
        query.generate_selection_string()
    with pytest.raises(ValueError):
        query = sx.generic_query(dataset_identifier=spec.Sample[0].RucioDID,
                                 codegen=spec.General.Codegen, query=5)
    with pytest.raises(NameError):
        query = sx.generic_query(dataset_identifier=spec.Sample[0].RucioDID,
                                 codegen='nonsense', query=spec.Sample[0].Query)
