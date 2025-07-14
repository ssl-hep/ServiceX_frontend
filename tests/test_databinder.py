import pytest
from pytest_asyncio import fixture
from unittest.mock import patch
from pydantic import ValidationError

from servicex import ServiceXSpec, dataset, OutputFormat
from servicex.query_core import ServiceXException
from servicex.servicex_client import ReturnValueException
from servicex.dataset import FileList, Rucio


@fixture
def network_patches(codegen_list):
    import contextlib

    with contextlib.ExitStack() as _fixture:
        _fixture.enter_context(
            patch(
                "servicex.servicex_adapter.ServiceXAdapter.get_servicex_capabilities",
                return_value=[
                    "poll_local_transformation_results",
                    "long_sample_titles_10240",
                ],
            )
        )
        _fixture.enter_context(
            patch(
                "servicex.servicex_client.ServiceXClient.get_code_generators",
                return_value=codegen_list,
            )
        )
        _fixture.enter_context(
            patch(
                "servicex.servicex_adapter.ServiceXAdapter._get_authorization",
                return_value={"Authorization": "Bearer aaa"},
            )
        )

        yield _fixture


def basic_spec(samples=None):
    return {
        "Sample": samples
        or [{"Name": "sampleA", "XRootDFiles": "root://a.root", "Query": "a"}],
    }


def test_long_sample_name():
    long_sample_name = "a_16_char_string" * 50
    config = {
        "Sample": [
            {
                "Name": long_sample_name,
                "XRootDFiles": "root://a.root",
                "Query": "a",
            },
        ],
    }
    new_config = ServiceXSpec.model_validate(config)
    assert len(new_config.Sample[0].Name) == 800
    with pytest.raises(ValueError):
        new_config.Sample[0].validate_title(512)
    new_config.Sample[0].validate_title(None)
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


def test_output_format():
    spec = basic_spec()
    spec["General"] = {"OutputFormat": "root-ttree"}
    ServiceXSpec.model_validate(spec)
    spec["General"] = {"OutputFormat": "root-rntuple"}
    ServiceXSpec.model_validate(spec)
    spec["General"] = {"OutputFormat": "parquet"}
    ServiceXSpec.model_validate(spec)
    spec["General"] = {"OutputFormat": OutputFormat.root_ttree}
    ServiceXSpec.model_validate(spec)
    spec["General"] = {"OutputFormat": OutputFormat.root_rntuple}
    ServiceXSpec.model_validate(spec)
    spec["General"] = {"OutputFormat": OutputFormat.parquet}
    ServiceXSpec.model_validate(spec)
    with pytest.raises(ValidationError):
        spec["General"] = {"OutputFormat": "root-tree"}
        ServiceXSpec.model_validate(spec)


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
                    "Dataset": dataset.Rucio(
                        "user.ivukotic:user.ivukotic.single_top_tW__nominal"
                    ),
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


def test_dataset_zerofiles():
    # with an actual dataset, giving no files should throw a validation error
    with pytest.raises(ValidationError):
        spec = ServiceXSpec.model_validate(
            basic_spec(
                samples=[
                    {
                        "Name": "sampleA",
                        "Dataset": dataset.Rucio(
                            "user.ivukotic:user.ivukotic.single_top_tW__nominal"
                        ),
                        "NFiles": 0,
                        "Query": "a",
                    }
                ]
            )
        )

    with pytest.raises(ValidationError):
        spec = ServiceXSpec.model_validate(
            basic_spec(
                samples=[
                    {
                        "Name": "sampleA",
                        "Dataset": dataset.Rucio(
                            "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                            num_files=0,
                        ),
                        "Query": "a",
                    }
                ]
            )
        )

    # and num files should be ignored for fileset
    spec = ServiceXSpec.model_validate(
        basic_spec(
            samples=[
                {
                    "Name": "sampleA",
                    "Dataset": dataset.FileList(
                        [
                            "root://eospublic.cern.ch//file1.root",
                            "root://eospublic.cern.ch//file2.root",
                        ]
                    ),
                    "Query": "a",
                }
            ]
        )
    )
    assert spec.Sample[0].dataset_identifier.num_files is None


def test_cernopendata():
    spec = ServiceXSpec.model_validate(
        {
            "Sample": [
                {
                    "Name": "sampleA",
                    "Dataset": dataset.CERNOpenData(1507),
                    "Function": "a",
                }
            ]
        }
    )
    assert spec.Sample[0].dataset_identifier.did == "cernopendata://1507"


def test_xrootd():
    spec = ServiceXSpec.model_validate(
        {
            "Sample": [
                {
                    "Name": "sampleA",
                    "Dataset": dataset.XRootD("root://blablabla/*/?.root"),
                    "Function": "a",
                }
            ]
        }
    )
    assert spec.Sample[0].dataset_identifier.did == "xrootd://root://blablabla/*/?.root"


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


def test_submit_mapping(transformed_result, network_patches, with_event_loop):
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
                "Codegen": "uproot-raw",
            }
        ],
    }
    with patch(
        "servicex.dataset_group.DatasetGroup.as_files",
        return_value=[transformed_result],
    ):
        results = deliver(spec, config_path="tests/example_config.yaml")
        assert list(results["sampleA"]) == ["1.parquet"]


def test_submit_mapping_signed_urls(
    transformed_result_signed_url, network_patches, with_event_loop
):
    from servicex import deliver

    spec = {
        "General": {"Delivery": "URLs"},
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": "[{'treename': 'nominal'}]",
                "Codegen": "uproot-raw",
            }
        ],
    }
    with patch(
        "servicex.dataset_group.DatasetGroup.as_signed_urls",
        return_value=[transformed_result_signed_url],
    ):
        results = deliver(spec, config_path="tests/example_config.yaml")
        assert list(results["sampleA"]) == [
            "https://dummy.junk.io/1.parquet",
            "https://dummy.junk.io/2.parquet",
        ]


def test_submit_mapping_failure(transformed_result, network_patches, with_event_loop):
    from servicex import deliver

    spec = {
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": "[{'treename': 'nominal'}]",
                "Codegen": "uproot-raw",
            }
        ]
    }
    with patch(
        "servicex.dataset_group.DatasetGroup.as_files",
        return_value=[ServiceXException("dummy")],
    ):
        results = deliver(spec, config_path="tests/example_config.yaml")
        assert len(results) == 1
        with pytest.raises(ReturnValueException):
            # should expect an exception to be thrown on access
            for _ in results["sampleA"]:
                pass


def test_submit_mapping_failure_signed_urls(network_patches, with_event_loop):
    from servicex import deliver

    spec = {
        "General": {"Delivery": "URLs"},
        "Sample": [
            {
                "Name": "sampleA",
                "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                "Query": "[{'treename': 'nominal'}]",
                "Codegen": "uproot-raw",
            }
        ],
    }
    with patch(
        "servicex.dataset_group.DatasetGroup.as_signed_urls",
        return_value=[ServiceXException("dummy")],
    ):
        results = deliver(
            spec, config_path="tests/example_config.yaml", return_exceptions=False
        )
        assert len(results) == 1
        with pytest.raises(ReturnValueException):
            # should expect an exception to be thrown on access
            for _ in results["sampleA"]:
                pass


def test_yaml(tmp_path):
    from servicex.servicex_client import _load_ServiceXSpec
    from servicex.dataset import FileList, Rucio, CERNOpenData

    # Nominal paths
    with open(path := (tmp_path / "python.yaml"), "w") as f:
        f.write(
            """
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
                .FromTree("nominal")
  - Name: ttH3
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: !UprootRaw |
                [{"treename": "nominal"}]
  - Name: ttH4
    Dataset: !Rucio user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v112
    Query: !UprootRaw '[{"treename": "nominal"}]'
  - Name: ttH5
    Dataset: !FileList ["/path/to/file1.root", "/path/to/file2.root"]
    Query: !UprootRaw '[{"treename": "nominal"}]'
  - Name: ttH6
    Dataset: !CERNOpenData 1507
    Query: !UprootRaw '[{"treename": "nominal"}]'
  - Name: ttH7
    Dataset: !XRootD root://eosatlas.cern.ch//eos/atlas/path/*/file.root
  - Name: ttH8
    Dataset: !Rucio user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v113
    Query: !TopCP 'reco="examples/reco.yaml"'
"""
        )
        f.flush()
        result = _load_ServiceXSpec(path)
        assert type(result.Sample[0].Query).__name__ == "PythonFunction"
        assert type(result.Sample[1].Query).__name__ == "FuncADLQuery_Uproot"
        assert type(result.Sample[2].Query).__name__ == "UprootRawQuery"
        assert type(result.Sample[7].Query).__name__ == "TopCPQuery"
        assert isinstance(result.Sample[3].dataset_identifier, Rucio)
        assert (
            result.Sample[3].dataset_identifier.did
            == "rucio://user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v112"
        )
        assert isinstance(result.Sample[4].dataset_identifier, FileList)
        assert result.Sample[4].dataset_identifier.files == [
            "/path/to/file1.root",
            "/path/to/file2.root",
        ]
        assert isinstance(result.Sample[5].dataset_identifier, CERNOpenData)
        assert result.Sample[5].dataset_identifier.did == "cernopendata://1507"
        assert (
            result.Sample[6].dataset_identifier.did
            == "xrootd://root://eosatlas.cern.ch//eos/atlas/path/*/file.root"
        )

    # Path from string
    result2 = _load_ServiceXSpec(str(path))
    assert type(result2.Sample[0].Query).__name__ == "PythonFunction"

    # Python syntax error
    with open(path := (tmp_path / "python.yaml"), "w") as f:
        f.write(
            """
General:
  OutputFormat: root-ttree
  Delivery: LocalCache

Sample:
  - Name: ttH
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: !PythonFunction |
        def run_query(input_filenames=None):
            i ==== 18 # syntax error
"""
        )
        f.flush()
        with pytest.raises(SyntaxError):
            _load_ServiceXSpec(path)

    # Duplicate samples with different names but same dataset and query
    with open(path := (tmp_path / "python.yaml"), "w") as f:
        f.write(
            """
General:
  OutputFormat: root-ttree
  Delivery: LocalCache

Sample:
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
    """
        )
        f.flush()
        with pytest.raises(RuntimeError):
            _load_ServiceXSpec(path)

    # Duplicate samples with different names but same datasets (multiple) and query
    # change the order of the datasets
    with open(path := (tmp_path / "python.yaml"), "w") as f:
        f.write(
            """
General:
  OutputFormat: root-ttree
  Delivery: LocalCache

Sample:
  - Name: ttH3
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: !UprootRaw |
                [{"treename": "nominal"}]
  - Name: ttH5
    Dataset: !FileList ["/path/to/file2.root", "/path/to/file1.root"]
    Query: !UprootRaw '[{"treename": "nominal"}]'
  - Name: ttH5
    Dataset: !FileList ["/path/to/file1.root", "/path/to/file2.root"]
    Query: !UprootRaw '[{"treename": "nominal"}]'
    """
        )
        f.flush()
        with pytest.raises(RuntimeError):
            _load_ServiceXSpec(path)

    # Samples with different names but same datasets(multiple) and query
    # different NFiles
    with open(path := (tmp_path / "python.yaml"), "w") as f:
        f.write(
            """
General:
  OutputFormat: root-ttree
  Delivery: LocalCache

Sample:
  - Name: ttH5
    Dataset: !FileList ["/path/to/file2.root", "/path/to/file1.root"]
    NFiles: 3
    Query: !UprootRaw '[{"treename": "nominal"}]'
  - Name: ttH6
    NFiles: 1
    Dataset: !FileList ["/path/to/file1.root", "/path/to/file2.root"]
    Query: !UprootRaw '[{"treename": "nominal"}]'
    """
        )
        f.flush()
        result = _load_ServiceXSpec(path)
        assert type(result.Sample[0].Query).__name__ == "UprootRawQuery"
        assert type(result.Sample[1].Query).__name__ == "UprootRawQuery"

    # Samples with different names but same datasets(multiple) and
    # different queries
    with open(path := (tmp_path / "python.yaml"), "w") as f:
        f.write(
            """
General:
  OutputFormat: root-ttree
  Delivery: LocalCache

Sample:
  - Name: ttH5
    Dataset: !FileList ["/path/to/file2.root", "/path/to/file1.root"]
    Query: !UprootRaw '[{"treename": "nominal"}]'
  - Name: ttH6
    Dataset: !FileList ["/path/to/file1.root", "/path/to/file2.root"]
    Query: !UprootRaw '[{"treename": "CollectionTree"}]'
    """
        )
        f.flush()
        result = _load_ServiceXSpec(path)
        assert type(result.Sample[0].Query).__name__ == "UprootRawQuery"
        assert type(result.Sample[1].Query).__name__ == "UprootRawQuery"


def test_yaml_include(tmp_path):
    from servicex.servicex_client import _load_ServiceXSpec

    # Create two files, one has definitions for the other and is included by it
    with (
        open(tmp_path / "definitions.yaml", "w") as f1,
        open(path2 := (tmp_path / "parent.yaml"), "w") as f2,
    ):
        f1.write(
            """
- &DEF_query !PythonFunction |
        def run_query(input_filenames=None):
            return []
"""
        )
        f2.write(
            """
Definitions:
    !include definitions.yaml

General:
  OutputFormat: root-ttree
  Delivery: LocalCache

Sample:
  - Name: ttH
    RucioDID: user.kchoi:user.kchoi.fcnc_tHq_ML.ttH.v11
    Query: *DEF_query
"""
        )
        f1.flush()
        f2.flush()
        _load_ServiceXSpec(path2)


def test_funcadl_query(transformed_result, network_patches, with_event_loop):
    from servicex import deliver
    from servicex.query import FuncADL_Uproot  # type: ignore

    spec = ServiceXSpec.model_validate(
        {
            "Sample": [
                {
                    "Name": "sampleA",
                    "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                    "Query": FuncADL_Uproot()
                    .FromTree("nominal")
                    .Select(lambda e: {"lep_pt": e["lep_pt"]}),
                }
            ]
        }
    )
    with patch(
        "servicex.dataset_group.DatasetGroup.as_files",
        return_value=[transformed_result],
    ):
        deliver(spec, config_path="tests/example_config.yaml")


def test_query_with_codegen_override(
    transformed_result, network_patches, with_event_loop
):
    from servicex import deliver
    from servicex.query import FuncADL_Uproot  # type: ignore

    # first, with General override
    spec = ServiceXSpec.model_validate(
        {
            "General": {"Codegen": "does-not-exist"},
            "Sample": [
                {
                    "Name": "sampleA",
                    "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                    "Query": FuncADL_Uproot()
                    .FromTree("nominal")
                    .Select(lambda e: {"lep_pt": e["lep_pt"]}),
                }
            ],
        }
    )
    with patch(
        "servicex.dataset_group.DatasetGroup.as_files",
        return_value=[transformed_result],
    ):
        with pytest.raises(NameError) as excinfo:
            deliver(spec, config_path="tests/example_config.yaml")
        # if this has propagated correctly, the override worked
        assert excinfo.value.args[0].startswith("does-not-exist")

    # second, with sample-level override
    spec = ServiceXSpec.model_validate(
        {
            "Sample": [
                {
                    "Name": "sampleA",
                    "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                    "Query": FuncADL_Uproot()
                    .FromTree("nominal")
                    .Select(lambda e: {"lep_pt": e["lep_pt"]}),
                    "Codegen": "does-not-exist",
                }
            ]
        }
    )
    with patch(
        "servicex.dataset_group.DatasetGroup.as_files",
        return_value=[transformed_result],
    ):
        with pytest.raises(NameError) as excinfo:
            deliver(spec, config_path="tests/example_config.yaml")
        # if this has propagated correctly, the override worked
        assert excinfo.value.args[0].startswith("does-not-exist")


def test_databinder_load_dict():
    from servicex.query import FuncADL_Uproot  # type: ignore
    from servicex.servicex_client import _load_ServiceXSpec

    _load_ServiceXSpec(
        {
            "Sample": [
                {
                    "Name": "sampleA",
                    "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                    "Query": FuncADL_Uproot()
                    .FromTree("nominal")
                    .Select(lambda e: {"lep_pt": e["lep_pt"]}),
                }
            ]
        }
    )


def test_python_query(transformed_result, network_patches, with_event_loop):
    from servicex import deliver
    from servicex.query import PythonFunction  # type: ignore

    def run_query(input_filenames=None):
        print("Greetings from your query")
        return []

    query = PythonFunction().with_uproot_function(run_query)

    spec = ServiceXSpec.model_validate(
        {
            "Sample": [
                {
                    "Name": "sampleA",
                    "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                    "Query": query,
                }
            ]
        }
    )
    with patch(
        "servicex.dataset_group.DatasetGroup.as_files",
        return_value=[transformed_result],
    ):
        deliver(spec, config_path="tests/example_config.yaml")


def test_uproot_raw_query(transformed_result, network_patches, with_event_loop):
    from servicex import deliver
    from servicex.query import UprootRaw  # type: ignore

    spec = ServiceXSpec.model_validate(
        {
            "Sample": [
                {
                    "Name": "sampleA",
                    "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                    "Query": UprootRaw([{"treename": "nominal"}]),
                }
            ]
        }
    )
    with patch(
        "servicex.dataset_group.DatasetGroup.as_files",
        return_value=[transformed_result],
    ):
        deliver(spec, config_path="tests/example_config.yaml")


def test_uproot_raw_query_parquet(transformed_result, network_patches, with_event_loop):
    from servicex import deliver
    from servicex.query import UprootRaw  # type: ignore

    spec = ServiceXSpec.model_validate(
        {
            "General": {"OutputFormat": "parquet"},
            "Sample": [
                {
                    "Name": "sampleA",
                    "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                    "Query": UprootRaw([{"treename": "nominal"}]),
                }
            ],
        }
    )
    print(spec)
    with patch(
        "servicex.dataset_group.DatasetGroup.as_files",
        return_value=[transformed_result],
    ):
        deliver(spec, config_path="tests/example_config.yaml")


def test_uproot_raw_query_rntuple(transformed_result, network_patches, with_event_loop):
    from servicex import deliver
    from servicex.query import UprootRaw  # type: ignore

    spec = ServiceXSpec.model_validate(
        {
            "General": {"OutputFormat": "root-rntuple"},
            "Sample": [
                {
                    "Name": "sampleA",
                    "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                    "Query": UprootRaw([{"treename": "nominal"}]),
                }
            ],
        }
    )
    with patch(
        "servicex.dataset_group.DatasetGroup.as_files",
        return_value=[transformed_result],
    ):
        deliver(spec, config_path="tests/example_config.yaml")


def test_generic_query(network_patches):
    from servicex.servicex_client import ServiceXClient

    spec = ServiceXSpec.model_validate(
        {
            "General": {
                "Codegen": "uproot-raw",
            },
            "Sample": [
                {
                    "Name": "sampleA",
                    "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                    "Query": "[{'treename': 'nominal'}]",
                }
            ],
        }
    )
    sx = ServiceXClient(config_path="tests/example_config.yaml")
    query = sx.generic_query(
        dataset_identifier=spec.Sample[0].RucioDID,
        codegen=spec.General.Codegen,
        query=spec.Sample[0].Query,
    )
    assert query.generate_selection_string() == "[{'treename': 'nominal'}]"
    query = sx.generic_query(
        dataset_identifier=spec.Sample[0].RucioDID,
        result_format=spec.General.OutputFormat.to_ResultFormat(),
        codegen=spec.General.Codegen,
        query=spec.Sample[0].Query,
    )
    assert query.result_format == "root-file"
    query.query_string_generator = None
    with pytest.raises(RuntimeError):
        query.generate_selection_string()
    with pytest.raises(ValueError):
        query = sx.generic_query(
            dataset_identifier=spec.Sample[0].RucioDID,
            codegen=spec.General.Codegen,
            query=5,
        )
    with pytest.raises(NameError):
        query = sx.generic_query(
            dataset_identifier=spec.Sample[0].RucioDID,
            codegen="nonsense",
            query=spec.Sample[0].Query,
        )
    with pytest.raises(RuntimeError):
        # no codegen specified by generic class
        query = sx.generic_query(
            dataset_identifier=spec.Sample[0].RucioDID, query=spec.Sample[0].Query
        )


def test_deliver_progress_options(transformed_result, network_patches, with_event_loop):
    from servicex import deliver, ProgressBarFormat
    from servicex.query import UprootRaw  # type: ignore

    spec = ServiceXSpec.model_validate(
        {
            "Sample": [
                {
                    "Name": "sampleA",
                    "RucioDID": "user.ivukotic:user.ivukotic.single_top_tW__nominal",
                    "Query": UprootRaw([{"treename": "nominal"}]),
                }
            ]
        }
    )

    async def fake_submit(signed_urls_only, expandable_progress):
        expandable_progress.add_task("zip", start=False, total=None)
        return transformed_result

    with patch(
        "servicex.query_core.Query.submit_and_download",
        side_effect=fake_submit,
    ):
        import servicex.query_core

        rv = deliver(
            spec,
            config_path="tests/example_config.yaml",
            progress_bar=ProgressBarFormat.compact,
        )
        servicex.query_core.Query.submit_and_download.assert_called_once()
        assert rv is not None
        assert rv["sampleA"].valid()
        rv = deliver(
            spec,
            config_path="tests/example_config.yaml",
            progress_bar=ProgressBarFormat.none,
        )
        assert rv is not None
        assert rv["sampleA"].valid()
        deliver(
            spec,
            config_path="tests/example_config.yaml",
            progress_bar=ProgressBarFormat.expanded,
        )
        assert rv is not None
        assert rv["sampleA"].valid()
        with pytest.raises(ValueError):
            deliver(
                spec,
                config_path="tests/example_config.yaml",
                progress_bar="garbage",
            )


def test_entrypoint_import():
    """This will check that we have at least the Python transformer defined in servicex.query"""
    from servicex.query import PythonFunction  # type: ignore # noqa: F401
