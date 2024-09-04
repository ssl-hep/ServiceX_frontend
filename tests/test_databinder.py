import pytest
from unittest.mock import patch
from pydantic import ValidationError

from servicex import ServiceXSpec, dataset
from servicex.query_core import ServiceXException
from servicex.servicex_client import ReturnValueException
from servicex.dataset import FileList, Rucio


def basic_spec(samples=None):
    return {
        "Sample": samples
        or [{"Name": "sampleA", "XRootDFiles": "root://a.root", "Query": "a"}],
    }


def test_long_sample_name():
    config = {
        "Sample": [
            {
                "Name": "long_sample_name_long_sample_name_long_sample_name_\
                         long_sample_name_long_sample_name_long_sample_name_\
                         long_sample_name_long_sample_name_long_sample_name",
                "XRootDFiles": "root://a.root",
                "Query": "a",
            },
        ],
    }
    new_config = ServiceXSpec.model_validate(config)
    assert len(new_config.Sample[0].Name) == 128


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

    assert isinstance(spec.Sample[0].dataset_identifier, FileList)
    assert spec.Sample[0].dataset_identifier.files == [
        "root://eospublic.cern.ch//file1.root"
    ]
    assert spec.Sample[0].dataset_identifier.did is None


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

    assert isinstance(spec.Sample[0].dataset_identifier, FileList)
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

    assert isinstance(spec.Sample[0].dataset_identifier, Rucio)
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

    assert isinstance(spec.Sample[0].dataset_identifier, Rucio)
    assert (
        spec.Sample[0].dataset_identifier.did
        == "rucio://user.ivukotic:user.ivukotic.single_top_tW__nominal?files=10"
    )


def test_dataset_rucio_did_numfiles():
    spec = ServiceXSpec.model_validate(
        basic_spec(
            samples=[
                {
                    "Name": "sampleA",
                    "Dataset": dataset.Rucio("user.ivukotic:user.ivukotic.single_top_tW__nominal"),
                    "NFiles": 12,
                    "Query": "a",
                }
            ]
        )
    )

    assert isinstance(spec.Sample[0].dataset_identifier, Rucio)
    assert (
        spec.Sample[0].dataset_identifier.did
        == "rucio://user.ivukotic:user.ivukotic.single_top_tW__nominal?files=12"
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
        results = deliver(spec, config_path='tests/example_config.yaml')
        assert list(results['sampleA']) == ['1.parquet']


def test_submit_mapping_signed_urls(transformed_result_signed_url, codegen_list):
    from servicex import deliver
    spec = {
        "General": {
            "Delivery": "URLs"
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
    with patch('servicex.dataset_group.DatasetGroup.as_signed_urls',
               return_value=[transformed_result_signed_url]), \
         patch('servicex.servicex_client.ServiceXClient.get_code_generators',
               return_value=codegen_list):
        results = deliver(spec, config_path='tests/example_config.yaml')
        assert list(results['sampleA']) == ['https://dummy.junk.io/1.parquet',
                                            'https://dummy.junk.io/2.parquet']


def test_submit_mapping_failure(transformed_result, codegen_list):
    from servicex import deliver
    spec = {
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
               return_value=[ServiceXException("dummy")]), \
         patch('servicex.servicex_client.ServiceXClient.get_code_generators',
               return_value=codegen_list):
        results = deliver(spec, config_path='tests/example_config.yaml')
        assert len(results) == 1
        with pytest.raises(ReturnValueException):
            # should expect an exception to be thrown on access
            for _ in results['sampleA']:
                pass


def test_submit_mapping_failure_signed_urls(codegen_list):
    from servicex import deliver
    spec = {
        "General": {"Delivery": "URLs"},
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": "[{'treename': 'nominal'}]",
                "Codegen": "uproot-raw"
            }
        ]
    }
    with patch('servicex.dataset_group.DatasetGroup.as_signed_urls',
               return_value=[ServiceXException("dummy")]), \
         patch('servicex.servicex_client.ServiceXClient.get_code_generators',
               return_value=codegen_list):
        results = deliver(spec, config_path='tests/example_config.yaml', return_exceptions=False)
        assert len(results) == 1
        with pytest.raises(ReturnValueException):
            # should expect an exception to be thrown on access
            for _ in results['sampleA']:
                pass


def test_yaml(tmp_path):
    from servicex.servicex_client import _load_ServiceXSpec
    from servicex.dataset import FileList, Rucio, CERNOpenData
    # Nominal paths
    with open(path := (tmp_path / "python.yaml"), "w") as f:
        f.write("""
General:
  OutputFormat: root-ttree
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
  - Name: ttH4
    Dataset: !Rucio user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: !UprootRaw '[{"treename": "nominal"}]'
  - Name: ttH5
    Dataset: !FileList ["/path/to/file1.root", "/path/to/file2.root"]
    Query: !UprootRaw '[{"treename": "nominal"}]'
  - Name: ttH6
    Dataset: !CERNOpenData 1507
    Query: !UprootRaw '[{"treename": "nominal"}]'
""")
        f.flush()
        result = _load_ServiceXSpec(path)
        assert type(result.Sample[0].Query).__name__ == 'PythonFunction'
        assert type(result.Sample[1].Query).__name__ == 'FuncADLQuery_Uproot'
        assert type(result.Sample[2].Query).__name__ == 'UprootRawQuery'
        assert isinstance(result.Sample[3].dataset_identifier, Rucio)
        assert (result.Sample[3].dataset_identifier.did
                == 'rucio://user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11')
        assert isinstance(result.Sample[4].dataset_identifier, FileList)
        assert (result.Sample[4].dataset_identifier.files
                == ["/path/to/file1.root", "/path/to/file2.root"])
        assert isinstance(result.Sample[5].dataset_identifier, CERNOpenData)
        assert result.Sample[5].dataset_identifier.did == 'cernopendata://1507'

    # Path from string
    result2 = _load_ServiceXSpec(str(path))
    assert type(result2.Sample[0].Query).__name__ == 'PythonFunction'

    # Python syntax error
    with open(path := (tmp_path / "python.yaml"), "w") as f:
        f.write("""
General:
  OutputFormat: root-ttree
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
  OutputFormat: root-ttree
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
    from servicex.query import FuncADL_Uproot  # type: ignore
    spec = ServiceXSpec.model_validate({
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": FuncADL_Uproot().FromTree("nominal")
                                         .Select(lambda e: {"lep_pt": e["lep_pt"]})
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
    from servicex.query import FuncADL_Uproot  # type: ignore
    # first, with General override
    spec = ServiceXSpec.model_validate({
        "General": {
            "Codegen": "does-not-exist"
        },
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": FuncADL_Uproot().FromTree("nominal")
                                         .Select(lambda e: {"lep_pt": e["lep_pt"]})
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
                "Query": FuncADL_Uproot().FromTree("nominal")
                                         .Select(lambda e: {"lep_pt": e["lep_pt"]}),
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
    from servicex.query import FuncADL_Uproot  # type: ignore
    from servicex.servicex_client import _load_ServiceXSpec
    _load_ServiceXSpec({
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": FuncADL_Uproot().FromTree("nominal")
                                         .Select(lambda e: {"lep_pt": e["lep_pt"]})
            }
        ]
    })


def test_python_query(transformed_result, codegen_list):
    from servicex import deliver
    from servicex.query import PythonFunction  # type: ignore

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
    from servicex.query import UprootRaw  # type: ignore
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


def test_uproot_raw_query_parquet(transformed_result, codegen_list):
    from servicex import deliver
    from servicex.query import UprootRaw  # type: ignore
    spec = ServiceXSpec.model_validate({
        "General": {
            "OutputFormat": "parquet"
        },
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": UprootRaw([{"treename": "nominal"}])
            }
        ]
    })
    print(spec)
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
        query = sx.generic_query(dataset_identifier=spec.Sample[0].RucioDID,
                                 result_format=spec.General.OutputFormat.to_ResultFormat(),
                                 codegen=spec.General.Codegen, query=spec.Sample[0].Query)
        assert query.result_format == 'root-file'
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


def test_entrypoint_import():
    """ This will check that we have at least the Python transformer defined in servicex.query """
    from servicex.query import PythonFunction  # type: ignore # noqa: F401
