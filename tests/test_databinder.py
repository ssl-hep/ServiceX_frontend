import pytest
from unittest.mock import patch
from pydantic import ValidationError

from servicex import ServiceXSpec, FileListDataset, RucioDatasetIdentifier


def basic_spec(samples=None):
    return {
        "General": {
            "Codegen": "python",
        },
        "Sample": samples
        or [{"Name": "sampleA", "XRootDFiles": "root://a.root", "Query": "a"}],
    }


def test_load_config():
    config = {
        "General": {
            "Codegen": "python",
            "Delivery": "LocalCache",
        },
        "Sample": [
            {"Name": "sampleA", "RucioDID": "user.kchoi:sampleA", "Query": "a"},
            {
                "Name": "sampleB",
                "XRootDFiles": "root://a.root",
                "Query": "a",
            },
        ],
    }
    new_config = ServiceXSpec.model_validate(config)
    assert new_config.Sample[0].Query == "a"


def test_single_root_file():

    spec = ServiceXSpec.model_validate(
        basic_spec(
            samples=[
                {
                    "Name": "sampleA",
                    "XRootDFiles": "root://eospublic.cern.ch//file1.root",
                    "Query": "a",
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
                    "Query": "a",
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
                    "Query": "a",
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
                    "Query": "a",
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
                        "Query": "a",
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
                        "Query": "a",
                    }
                ]
            )
        )


def test_submit_mapping(transformed_result, codegen_list):
    from servicex import deliver
    spec = {
        "General": {
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
               return_value=[transformed_result]), \
         patch('servicex.servicex_client.ServiceXClient.get_code_generators',
               return_value=codegen_list):
        deliver(spec, config_path='tests/example_config.yaml')


def test_yaml(tmp_path):
    from servicex.servicex_client import _load_ServiceXSpec
    # Nominal paths
    with open(path := (tmp_path / "python.yaml"), "w") as f:
        f.write("""
General:
  OutputFormat: root-file
  Delivery: LocalCache

Sample:
  - Name: ttH
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: !PythonFunction |
        def run_query(input_filenames=None):
            return []
    Codegen: python
  - Name: ttH2
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: !FuncADL_Uproot |
                Select(lambda e: {'lep_pt': e['lep_pt']})
  - Name: ttH3
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: !UprootRaw |
                [{"treename": "nominal"}]
""")
        f.flush()
        result = _load_ServiceXSpec(path)
        assert type(result.Sample[0].Query).__name__ == 'PythonQuery'
        assert type(result.Sample[1].Query).__name__ == 'FuncADLQuery_Uproot'
        assert type(result.Sample[2].Query).__name__ == 'UprootRawQuery'

    # Path from string
    result2 = _load_ServiceXSpec(str(path))
    assert type(result2.Sample[0].Query).__name__ == 'PythonQuery'

    # Python syntax error
    with open(path := (tmp_path / "python.yaml"), "w") as f:
        f.write("""
General:
  OutputFormat: root-file
  Delivery: LocalCache

Sample:
  - Name: ttH
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: !PythonFunction |
        def run_query(input_filenames=None):
            i ==== 18 # syntax error
""")
        f.flush()
        with pytest.raises(SyntaxError):
            _load_ServiceXSpec(path)


def test_yaml_include(tmp_path):
    from servicex.servicex_client import _load_ServiceXSpec
    # Create two files, one has definitions for the other and is included by it
    with open(tmp_path / "definitions.yaml", "w") as f1, \
         open(path2 := (tmp_path / "parent.yaml"), "w") as f2:
        f1.write("""
- &DEF_query !PythonFunction |
        def run_query(input_filenames=None):
            return []
""")
        f2.write("""
Definitions:
    !include definitions.yaml

General:
  OutputFormat: root-file
  Delivery: LocalCache

Sample:
  - Name: ttH
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: *DEF_query
""")
        f1.flush()
        f2.flush()
        _load_ServiceXSpec(path2)


def test_funcadl_query(transformed_result, codegen_list):
    from servicex import deliver
    from servicex import FuncADL_Uproot
    spec = ServiceXSpec.model_validate({
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": FuncADL_Uproot().Select(lambda e: {"lep_pt": e["lep_pt"]}),
                "Tree": "nominal"
            }
        ]
    })
    with patch('servicex.dataset_group.DatasetGroup.as_files',
               return_value=[transformed_result]), \
         patch('servicex.servicex_client.ServiceXClient.get_code_generators',
               return_value=codegen_list):
        deliver(spec, config_path='tests/example_config.yaml')


def test_query_with_codegen_override(transformed_result, codegen_list):
    from servicex import deliver
    from servicex import FuncADL_Uproot
    # first, with General override
    spec = ServiceXSpec.model_validate({
        "General": {
            "Codegen": "does-not-exist"
        },
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": FuncADL_Uproot().Select(lambda e: {"lep_pt": e["lep_pt"]}),
                "Tree": "nominal"
            }
        ]
    })
    with patch('servicex.dataset_group.DatasetGroup.as_files',
               return_value=[transformed_result]), \
         patch('servicex.servicex_client.ServiceXClient.get_code_generators',
               return_value=codegen_list):
        with pytest.raises(NameError) as excinfo:
            deliver(spec, config_path='tests/example_config.yaml')
        # if this has propagated correctly, the override worked
        assert excinfo.value.args[0].startswith('does-not-exist')

    # second, with sample-level override
    spec = ServiceXSpec.model_validate({
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": FuncADL_Uproot().Select(lambda e: {"lep_pt": e["lep_pt"]}),
                "Tree": "nominal",
                "Codegen": "does-not-exist"
            }
        ]
    })
    with patch('servicex.dataset_group.DatasetGroup.as_files',
               return_value=[transformed_result]), \
         patch('servicex.servicex_client.ServiceXClient.get_code_generators',
               return_value=codegen_list):
        with pytest.raises(NameError) as excinfo:
            deliver(spec, config_path='tests/example_config.yaml')
        # if this has propagated correctly, the override worked
        assert excinfo.value.args[0].startswith('does-not-exist')


def test_databinder_load_dict():
    from servicex import FuncADL_Uproot
    from servicex.servicex_client import _load_ServiceXSpec
    _load_ServiceXSpec({
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": FuncADL_Uproot().Select(lambda e: {"lep_pt": e["lep_pt"]}),
                "Tree": "nominal"
            }
        ]
    })


def test_python_query(transformed_result, codegen_list):
    from servicex import PythonFunction, deliver

    def run_query(input_filenames=None):
        print("Greetings from your query")
        return []

    query = PythonFunction().with_uproot_function(run_query)

    spec = ServiceXSpec.model_validate({
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": query
            }
        ]
    })
    with patch('servicex.dataset_group.DatasetGroup.as_files',
               return_value=[transformed_result]), \
         patch('servicex.servicex_client.ServiceXClient.get_code_generators',
               return_value=codegen_list):
        deliver(spec, config_path='tests/example_config.yaml')


def test_uproot_raw_query(transformed_result, codegen_list):
    from servicex import deliver
    from servicex import UprootRaw
    spec = ServiceXSpec.model_validate({
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": UprootRaw([{"treename": "nominal"}])
            }
        ]
    })
    with patch('servicex.dataset_group.DatasetGroup.as_files',
               return_value=[transformed_result]), \
         patch('servicex.servicex_client.ServiceXClient.get_code_generators',
               return_value=codegen_list):
        deliver(spec, config_path='tests/example_config.yaml')


def test_generic_query(codegen_list):
    from servicex.servicex_client import ServiceXClient
    spec = ServiceXSpec.model_validate({
        "General": {
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
    with patch('servicex.servicex_client.ServiceXClient.get_code_generators',
               return_value=codegen_list):
        sx = ServiceXClient(config_path='tests/example_config.yaml')
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
        with pytest.raises(RuntimeError):
            # no codegen specified by generic class
            query = sx.generic_query(dataset_identifier=spec.Sample[0].RucioDID,
                                     query=spec.Sample[0].Query)
