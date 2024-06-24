import pytest
from unittest.mock import patch
from pydantic import ValidationError

from servicex import ServiceXSpec, FileListDataset, RucioDatasetIdentifier, dataset


def basic_spec(samples=None):
    return {
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
    assert spec.Sample[0].dataset_identifier.did is None
    assert spec.Sample[0].dataset_identifier.files == [
        "root://eospublic.cern.ch//file1.root"
    ]

    spec = ServiceXSpec.model_validate(
        basic_spec(
            samples=[
                {
                    "Name": "sampleA",
                    "Dataset": dataset.FileList("root://eospublic.cern.ch//file1.root"),
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

    spec = ServiceXSpec.model_validate(
        basic_spec(
            samples=[
                {
                    "Name": "sampleA",
                    "Dataset": dataset.FileList([
                        "root://eospublic.cern.ch//file1.root",
                        "root://eospublic.cern.ch//file2.root",
                    ]),
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

    spec = ServiceXSpec.model_validate(
        basic_spec(
            samples=[
                {
                    "Name": "sampleA",
                    "Dataset": dataset.Rucio("user.ivukotic:user.ivukotic.single_top_tW__nominal"),
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


def test_cernopendata():
    spec = ServiceXSpec.model_validate({
        "Sample": [
            {
                "Name": "sampleA",
                "Dataset": dataset.CERNOpenData(1507),
                "Function": "a"
            }
        ]
    })
    assert spec.Sample[0].dataset_identifier.did == "cernopendata://1507"


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
    Query: !Python |
        def run_query(input_filenames=None):
            return []
    Codegen: python
  - Name: ttH2
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: !FuncADL_Uproot |
                Select(lambda e: {'lep_pt': e['lep_pt']})
""")
        f.flush()
        result = _load_ServiceXSpec(path)
        assert type(result.Sample[0].Query).__name__ == 'PythonQuery'
        assert type(result.Sample[1].Query).__name__ == 'FuncADLQuery_Uproot'

    # Python syntax error
    with open(path := (tmp_path / "python.yaml"), "w") as f:
        f.write("""
General:
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
            _load_ServiceXSpec(path)


def test_yaml_include(tmp_path):
    from servicex.servicex_client import _load_ServiceXSpec
    # Create two files, one has definitions for the other and is included by it
    with open(tmp_path / "definitions.yaml", "w") as f1, \
         open(path2 := (tmp_path / "parent.yaml"), "w") as f2:
        f1.write("""
- &DEF_query !Python |
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
    from servicex.func_adl.func_adl_dataset import FuncADLQuery_Uproot
    spec = ServiceXSpec.model_validate({
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": FuncADLQuery_Uproot().Select(lambda e: {"lep_pt": e["lep_pt"]}),
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
    from servicex.func_adl.func_adl_dataset import FuncADLQuery_Uproot
    # first, with General override
    spec = ServiceXSpec.model_validate({
        "General": {
            "Codegen": "does-not-exist"
        },
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": FuncADLQuery_Uproot().Select(lambda e: {"lep_pt": e["lep_pt"]}),
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
                "Query": FuncADLQuery_Uproot().Select(lambda e: {"lep_pt": e["lep_pt"]}),
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
    from servicex.func_adl.func_adl_dataset import FuncADLQuery_Uproot
    from servicex.servicex_client import _load_ServiceXSpec
    _load_ServiceXSpec({
        "General": {
            "ServiceX": "servicex-uc-af",
        },
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": FuncADLQuery_Uproot().Select(lambda e: {"lep_pt": e["lep_pt"]}),
                "Tree": "nominal"
            }
        ]
    })


def test_python_query(transformed_result, codegen_list):
    from servicex import PythonQuery, deliver

    def run_query(input_filenames=None):
        print("Greetings from your query")
        return []

    query = PythonQuery().with_uproot_function(run_query)

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
    from servicex.uproot_raw.uproot_raw import UprootRawQuery
    spec = ServiceXSpec.model_validate({
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": UprootRawQuery([{"treename": "nominal"}])
            }
        ]
    })
    with patch('servicex.dataset_group.DatasetGroup.as_files',
               return_value=[transformed_result]), \
         patch('servicex.servicex_client.ServiceXClient.get_code_generators',
               return_value=codegen_list):
        deliver(spec, config_path='tests/example_config.yaml')


def test_fail_with_tree_on_non_funcadl_query():
    from servicex.uproot_raw.uproot_raw import UprootRawQuery
    with pytest.raises(ValueError):
        ServiceXSpec.model_validate({
            "Sample": [
                {
                    "Name": "sampleA",
                    "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                    "Query": UprootRawQuery([{"treename": "nominal"}]),
                    "Tree": "nominal"
                }
            ]
        })


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
