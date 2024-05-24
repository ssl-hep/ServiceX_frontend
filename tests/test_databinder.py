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
            {"Name": "sampleA", "RucioDID": "user.kchoi:sampleA", "Function": "a"},
            {
                "Name": "sampleB",
                "XRootDFiles": "root://a.root",
                "Columns": "el_pt",
                "Codegen": "uproot",
                "Function": "a",
            },
        ],
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
    spec = ServiceXSpec.parse_obj(
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
    spec = ServiceXSpec.parse_obj(
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
    spec = ServiceXSpec.parse_obj(
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
        ServiceXSpec.parse_obj(
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
        ServiceXSpec.parse_obj(
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


def test_submit_mapping(transformed_result):
    from servicex import deliver
    spec = {
        "General": {
            "ServiceX": "testing4",
            "Codegen": "uproot-raw",
        },
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": "[{'treename': 'nominal'}]",
                "Codegen": "uproot-raw"
            }
        ]
    }
    with patch('servicex.dataset_group.DatasetGroup.as_files',
               return_value=[transformed_result]):
        deliver(spec, config_path='tests/example_config.yaml')


def test_yaml(tmp_path):
    from servicex.databinder.databinder_configuration import load_databinder_config
    # Nominal paths
    with open(path := (tmp_path / "python.yaml"), "w") as f:
        f.write("""
General:
  ServiceX: "servicex-uc-af"
  Codegen: python
  OutputFormat: root-file
  Delivery: LocalCache

Sample:
  - Name: ttH
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: !Python |
        def run_query(input_filenames=None):
            return []
    Codegen: python
  - Name: ttH2
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: !FuncADL |
                Select(lambda e: {'lep_pt': e['lep_pt']})
    Codegen: uproot
""")
        f.flush()
        result = load_databinder_config(path)
        assert type(result.Sample[0].Query).__name__ == 'PythonQuery'
        assert type(result.Sample[1].Query).__name__ == 'FuncADLQuery'

    # Python syntax error
    with open(path := (tmp_path / "python.yaml"), "w") as f:
        f.write("""
General:
  ServiceX: "servicex-uc-af"
  Codegen: python
  OutputFormat: root-file
  Delivery: LocalCache

Sample:
  - Name: ttH
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: !Python |
        def run_query(input_filenames=None):
            i ==== 18 # syntax error
""")
        f.flush()
        with pytest.raises(SyntaxError):
            load_databinder_config(path)


def test_yaml_include(tmp_path):
    from servicex.databinder.databinder_configuration import load_databinder_config
    # Create two files, one has definitions for the other and is included by it
    with (open(tmp_path / "definitions.yaml", "w") as f1,
          open(path2 := (tmp_path / "parent.yaml"), "w") as f2):
        f1.write("""
- &DEF_facility servicex-uc-af
- &DEF_query !Python |
        def run_query(input_filenames=None):
            return []
""")
        f2.write("""
Definitions:
    !include definitions.yaml

General:
  ServiceX: *DEF_facility
  Codegen: python
  OutputFormat: root-file
  Delivery: LocalCache

Sample:
  - Name: ttH
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: *DEF_query
""")
        f1.flush()
        f2.flush()
        load_databinder_config(path2)
