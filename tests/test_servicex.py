import asyncio
import os
from contextlib import contextmanager
from pathlib import Path
from typing import List, Optional

import asyncmock
import minio
import pandas as pd
import pytest
import servicex as fe
from confuse.core import Configuration
from servicex import (
    DatasetType,
    ServiceXException,
    ServiceXFatalTransformException,
    ServiceXUnknownDataRequestID,
    ServiceXUnknownRequestID,
)
from servicex.cache import Cache
from servicex.data_conversions import DataConverterAdaptor
from servicex.minio_adaptor import MinioAdaptorFactory
from servicex.servicex import StreamInfoPath, StreamInfoUrl
from servicex.servicex_config import ServiceXConfigAdaptor
from servicex.utils import ServiceXNoFilesInCache, log_adaptor

from .conftest import MockMinioAdaptor, MockServiceXAdaptor, build_cache_mock  # NOQA

# To enable later tests when we want something no one is going
# to specify anywhere in their code.
fe.servicex.g_allowed_formats.append("parquet-ftw")


def clean_fname(fname: str):
    "No matter the string given, make it an acceptable filename"
    return fname.replace("*", "_").replace(";", "_").replace(":", "_")


def test_default_ctor(mocker):
    """Test the default ctor. This requires that a .servicex file be present to work,
    so we are going to dummy it out.
    """
    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.settings = Configuration("servicex", "servicex")
    config.get_servicex_adaptor_config.return_value = (
        "http://no-way.dude",
        "no_spoon_there_is",
    )

    fe.ServiceXDataset("localds://dude", "uproot-ftw", config_adaptor=config)

    config.get_servicex_adaptor_config.assert_called_with(
        "uproot-ftw", backend_type=None
    )
    config.get_default_returned_datatype.assert_called_with(
        "uproot-ftw", backend_type=None
    )


def test_sx_name(mocker):
    """Makes ure the name comes back right."""
    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.settings = Configuration("servicex", "servicex")
    config.get_servicex_adaptor_config.return_value = (
        "http://no-way.dude",
        "no_spoon_there_is",
    )

    ds = fe.ServiceXDataset("localds://dude", "uproot-ftw", config_adaptor=config)
    assert ds.dataset_as_name == "localds://dude"


def test_default_ctor_no_type(mocker):
    """Test the default ctor. This requires that a .servicex file be present to work,
    so we are going to dummy it out.
    """
    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.settings = Configuration("servicex", "servicex")
    config.get_servicex_adaptor_config.return_value = (
        "http://no-way.dude",
        "no_spoon_there_is",
    )

    fe.ServiceXDataset("localds://dude", config_adaptor=config)

    config.get_servicex_adaptor_config.assert_called_with(None, backend_type=None)
    config.get_default_returned_datatype.assert_called_with(None, backend_type=None)


def test_default_ctor_cache(mocker):
    "Test that the default config is passed the right value"

    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.settings = Configuration("servicex", "servicex")
    config.get_servicex_adaptor_config.return_value = (
        "http://no-way.dude",
        "1232j322432j22token",
    )

    cache = mocker.MagicMock(spec=Cache)
    cache_create_call = mocker.patch("servicex.servicex.Cache", return_value=cache)

    fe.ServiceXDataset("localds://dude", config_adaptor=config)

    cache_create_call.assert_called_once()
    assert not cache_create_call.call_args[0][1]


def test_default_ctor_cache_no(mocker):
    "Test that the default config is passed the right value"

    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.settings = Configuration("servicex", "servicex")
    config.get_servicex_adaptor_config.return_value = (
        "http://no-way.dude",
        "j@1232j322432j22token.com",
    )

    cache = mocker.MagicMock(spec=Cache)
    cache_create_call = mocker.patch("servicex.servicex.Cache", return_value=cache)

    fe.ServiceXDataset("localds://dude", config_adaptor=config, ignore_cache=True)

    cache_create_call.assert_called_once()
    assert cache_create_call.call_args[0][1]


def test_ignore_cache_on_ds(mocker):
    "Test that the ignore_cache context manager works correctly on the ds level"

    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.settings = Configuration("servicex", "servicex")
    config.get_servicex_adaptor_config.return_value = (
        "http://no-way.dude",
        "1232j322432j22token",
    )

    got_called = False

    @contextmanager
    def do_context():
        nonlocal got_called
        got_called = True
        yield

    cache = mocker.MagicMock(spec=Cache)
    cache.ignore_cache = do_context
    mocker.patch("servicex.servicex.Cache", return_value=cache)

    ds = fe.ServiceXDataset("localds://dude", config_adaptor=config)
    with ds.ignore_cache():
        pass

    assert got_called


def test_get_datatypes_good(mocker):
    "Test that we return a good datatype"

    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.settings = Configuration("servicex", "servicex")
    config.get_servicex_adaptor_config.return_value = (
        "http://no-way.dude",
        "no_spoon_there_is",
    )
    config.get_default_returned_datatype.return_value = "root"

    r = fe.ServiceXDataset("localds://dude", "uproot-ftw", config_adaptor=config)

    assert r.first_supported_datatype(["root", "parquet"]) == "root"


def test_get_datatypes_background_type(mocker):
    "Test that we return a good datatype"

    # config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    # config.settings = Configuration("servicex", "servicex")
    # config.get_servicex_adaptor_config.return_value = (
    #     "http://no-way.dude",
    #     "no_spoon_there_is",
    # )

    r = fe.ServiceXDataset("localds://dude", "uproot", backend_type="xaod")

    assert r.first_supported_datatype(["root-file", "parquet"]) == "root-file"


def test_get_datatypes_single(mocker):
    "Test that we return a good datatype"

    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.settings = Configuration("servicex", "servicex")
    config.get_servicex_adaptor_config.return_value = (
        "http://no-way.dude",
        "no_spoon_there_is",
    )
    config.get_default_returned_datatype.return_value = "root"

    r = fe.ServiceXDataset("localds://dude", "uproot-ftw", config_adaptor=config)

    assert r.first_supported_datatype("root") == "root"


def test_get_datatypes_bad(mocker):
    "Test that we return a good datatype"

    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.settings = Configuration("servicex", "servicex")
    config.get_servicex_adaptor_config.return_value = (
        "http://no-way.dude",
        "no_spoon_there_is",
    )
    config.get_default_returned_datatype.return_value = "root"

    r = fe.ServiceXDataset("localds://dude", "uproot-ftw", config_adaptor=config)

    assert r.first_supported_datatype("forking") is None


@pytest.mark.asyncio
async def test_minio_back(mocker):
    "Get a root file with a single file"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )
    r = await ds.get_data_rootfiles_async("(valid qastle string)")

    mock_minio_adaptor.mock_download_file.assert_called_with(
        "123-456", "one_minio_entry", "/foo/bar.root"
    )

    assert len(r) == 1
    assert r[0] == Path("/foo/bar.root")


@pytest.mark.asyncio
async def test_skipped_file(mocker):
    """
    ServiceX should throw if a file is marked as "skipped".
    """
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_transform_status = mocker.Mock(return_value=(0, 1, 1))
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "123-456", mock_transform_status
    )
    mock_minio_adaptor = MockMinioAdaptor(
        mocker, files=["one_minio_entry", "two_minio_entry"]
    )
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    with pytest.raises(fe.ServiceXException) as e:
        ds = fe.ServiceXDataset(
            "http://one-ds",
            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
            minio_adaptor=mock_minio_adaptor,  # type: ignore
            cache_adaptor=mock_cache,
            data_convert_adaptor=data_adaptor,
            local_log=mock_logger,
        )
        ds.get_data_rootfiles("(valid qastle string)")

    assert "Failed to transform" in str(e.value)


def test_minio_cant_find_bucket(mocker):
    "Make sure the non-async version works"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    first = True

    def our_get_files(request_id: str) -> List[str]:
        nonlocal first
        if first:
            first = False
            raise minio.error.NoSuchBucket("bucket was not found")
        else:
            return ["one_minio_entry", "two_minio_entry"]

    mock_minio_adaptor = MockMinioAdaptor(
        mocker, files=["one_minio_entry", "two_minio_entry"]
    )
    mock_minio_adaptor.get_files = our_get_files

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        data_convert_adaptor=data_adaptor,
        cache_adaptor=mock_cache,
        local_log=mock_logger,
    )

    r = ds.get_data_rootfiles("(valid qastle string)")
    assert len(r) == 2
    assert r[0] == Path("/foo/bar.root")


def test_good_run_root_files_no_async(mocker):
    "Make sure the non-async version works"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(
        mocker, files=["one_minio_entry", "two_minio_entry"]
    )
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        data_convert_adaptor=data_adaptor,
        cache_adaptor=mock_cache,
        local_log=mock_logger,
    )

    r = ds.get_data_rootfiles("(valid qastle string)")
    assert len(r) == 2
    assert r[0] == Path("/foo/bar.root")


@pytest.mark.asyncio
async def test_good_run_root_files_pause(mocker, short_status_poll_time):
    "Get a root file with a single file"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_transform_status = mocker.Mock(side_effect=[(1, 0, 0), (0, 1, 0)])
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "123-456", mock_transform_status
    )
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
    )
    r = await ds.get_data_rootfiles_async("(valid qastle string)")
    assert len(r) == 1
    assert len(mock_servicex_adaptor.transform_status.mock_calls) == 2


@pytest.mark.asyncio
async def test_good_run_files_back_4_order_1(mocker):
    "Simple run with expected results"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(
        mocker,
        files=[
            "one_minio_entry",
            "two_minio_entry",
            "three_minio_entry",
            "four_minio_entry",
        ],
    )
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        data_convert_adaptor=data_adaptor,
        cache_adaptor=mock_cache,
        local_log=mock_logger,
    )
    r = await ds.get_data_rootfiles_async("(valid qastle string)")
    assert isinstance(r, list)
    assert len(r) == 4
    s_r = sorted([f.name for f in r])
    assert [f.name for f in r] == s_r


@pytest.mark.asyncio
async def test_good_run_files_back_4_order_2(mocker):
    "Simple run with expected results"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(
        mocker,
        files=[
            "four_minio_entry",
            "three_minio_entry",
            "two_minio_entry",
            "one_minio_entry",
        ],
    )
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
    )
    r = await ds.get_data_rootfiles_async("(valid qastle string)")
    assert isinstance(r, list)
    assert len(r) == 4
    s_r = sorted([f.name for f in r])
    assert [f.name for f in r] == s_r


@pytest.mark.asyncio
async def test_good_run_files_back_4_unordered(mocker):
    "Simple run; should return alphabetized list"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(
        mocker,
        files=[
            "one_minio_entry",
            "two_minio_entry",
            "three_minio_entry",
            "four_minio_entry",
        ],
    )
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
    )
    r = await ds.get_data_rootfiles_async("(valid qastle string)")
    assert isinstance(r, list)
    assert len(r) == 4
    assert r[0] == Path("/tmp/servicex-testing/123-456/four_minio_entry")
    assert r[1] == Path("/tmp/servicex-testing/123-456/one_minio_entry")
    assert r[2] == Path("/tmp/servicex-testing/123-456/three_minio_entry")
    assert r[3] == Path("/tmp/servicex-testing/123-456/two_minio_entry")


@pytest.mark.asyncio
async def test_good_download_files_parquet(mocker, short_status_poll_time):
    "Simple run with expected results"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_transform_status = mocker.Mock(side_effect=[(1, 0, 0), (0, 1, 0)])
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "123-456", mock_transform_status
    )
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
    )
    r = await ds.get_data_parquet_async("(valid qastle string)")
    assert isinstance(r, list)
    assert len(r) == 1
    assert r[0] == Path("/tmp/servicex-testing/123-456/one_minio_entry")
    assert len(mock_servicex_adaptor.transform_status.mock_calls) == 2


@pytest.mark.asyncio
async def test_good_run_single_ds_1file_pandas(mocker, good_pandas_file_data):
    "Simple run with expected results"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.get_default_returned_datatype.return_value = "parquet"

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=good_pandas_file_data,
        local_log=mock_logger,
        config_adaptor=config,
    )
    r = await ds.get_data_pandas_df_async("(valid qastle string)")
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 6

    good_pandas_file_data.combine_pandas.assert_called_once()
    good_pandas_file_data.convert_to_pandas.assert_called_once()
    assert len(good_pandas_file_data.combine_pandas.call_args[0][0]) == 1


@pytest.mark.asyncio
async def test_pandas_uses_default_return_type(mocker, good_awkward_file_data):
    "Make sure the awkward request against an xaod backend asks for a root file"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.get_default_returned_datatype.return_value = "parquet-ftw"

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        backend_name="uproot",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=good_awkward_file_data,
        local_log=mock_logger,
        config_adaptor=config,
    )
    await ds.get_data_pandas_df_async("(valid qastle string)")

    assert mock_servicex_adaptor.query_json["result-format"] == "parquet-ftw"


@pytest.mark.asyncio
async def test_good_run_single_ds_1file_awkward(mocker, good_awkward_file_data):
    "Simple run with expected results"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.get_default_returned_datatype.return_value = "root-file"

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=good_awkward_file_data,
        local_log=mock_logger,
        config_adaptor=config,
    )
    r = await ds.get_data_awkward_async("(valid qastle string)")
    assert isinstance(r, dict)
    assert len(r) == 1
    assert "JetPt" in r
    assert len(r["JetPt"]) == 6

    good_awkward_file_data.combine_awkward.assert_called_once()
    good_awkward_file_data.convert_to_awkward.assert_called_once()
    assert len(good_awkward_file_data.combine_awkward.call_args[0][0]) == 1


@pytest.mark.asyncio
async def test_awkward_uses_default_return_type(mocker, good_awkward_file_data):
    "Make sure the awkward request against an xaod backend asks for a root file"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.get_default_returned_datatype.return_value = "parquet-ftw"

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        backend_name="uproot",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=good_awkward_file_data,
        local_log=mock_logger,
        config_adaptor=config,
    )
    await ds.get_data_awkward_async("(valid qastle string)")

    assert mock_servicex_adaptor.query_json["result-format"] == "parquet-ftw"


@pytest.mark.asyncio
async def test_good_run_single_ds_2file_pandas(mocker, good_pandas_file_data):
    "Simple run with expected results"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(
        mocker, files=["one_minio_entry", "two_minio_entry"]
    )
    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.get_default_returned_datatype.return_value = "parquet"

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=good_pandas_file_data,
        local_log=mock_logger,
        config_adaptor=config,
    )
    await ds.get_data_pandas_df_async("(valid qastle string)")
    good_pandas_file_data.combine_pandas.assert_called_once()
    assert len(good_pandas_file_data.combine_pandas.call_args[0][0]) == 2


@pytest.mark.asyncio
async def test_good_run_single_ds_2file_awkward(mocker, good_awkward_file_data):
    "Simple run with expected results"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(
        mocker, files=["one_minio_entry", "two_minio_entry"]
    )
    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.get_default_returned_datatype.return_value = "root-file"

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=good_awkward_file_data,
        local_log=mock_logger,
        config_adaptor=config,
    )
    await ds.get_data_awkward_async("(valid qastle string)")
    assert len(good_awkward_file_data.combine_awkward.call_args[0][0]) == 2


@pytest.mark.asyncio
async def test_async_root_files_from_minio(mocker):
    "Get a root file pulling back minio info as it arrives"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )
    lst = await ds.get_data_rootfiles_uri_async(
        "(valid qastle string)", as_signed_url=True
    )

    assert len(lst) == 1
    r = lst[0]
    assert isinstance(r, StreamInfoUrl)
    assert r.bucket == "123-456"
    assert r.file == "one_minio_entry"
    assert r.url == "http://the.url.com"

    assert mock_servicex_adaptor.query_json["result-format"] == "root-file"
    assert mock_minio_adaptor.access_called_with == ("123-456", "one_minio_entry")


def test_root_files_from_minio(mocker):
    "Get a root file pulling back minio info as it arrives"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )
    lst = ds.get_data_rootfiles_uri("(valid qastle string)", as_signed_url=True)

    assert len(lst) == 1
    r = lst[0]
    assert isinstance(r, StreamInfoUrl)
    assert r.bucket == "123-456"
    assert r.file == "one_minio_entry"
    assert r.url == "http://the.url.com"

    assert mock_servicex_adaptor.query_json["result-format"] == "root-file"
    assert mock_minio_adaptor.access_called_with == ("123-456", "one_minio_entry")


@pytest.mark.asyncio
async def test_stream_root_files_from_minio(mocker):
    "Get a root file pulling back minio info as it arrives"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )
    lst = [
        f
        async for f in ds.get_data_rootfiles_uri_stream(
            "(valid qastle string)", as_signed_url=True
        )
    ]

    assert len(lst) == 1
    r = lst[0]
    assert isinstance(r, StreamInfoUrl)
    assert r.bucket == "123-456"
    assert r.file == "one_minio_entry"
    assert r.url == "http://the.url.com"

    assert mock_minio_adaptor.access_called_with == ("123-456", "one_minio_entry")
    assert mock_servicex_adaptor.query_json["result-format"] == "root-file"


@pytest.mark.asyncio
async def test_stream_root_files_from_s3(mocker):
    "Get a root file pulling back minio info as it arrives"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )
    lst = [
        f
        async for f in ds.get_data_rootfiles_uri_stream(
            "(valid qastle string)", as_signed_url=False
        )
    ]

    assert len(lst) == 1
    r = lst[0]
    assert isinstance(r, StreamInfoUrl)
    assert r.bucket == "123-456"
    assert r.file == "one_minio_entry"
    assert r.url == "s3://123-456/one_minio_entry"

    assert mock_servicex_adaptor.query_json["result-format"] == "root-file"


@pytest.mark.asyncio
async def test_stream_root_files(mocker):
    "Get a root file pulling back minio info as it arrives"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )
    lst = [f async for f in ds.get_data_rootfiles_stream("(valid qastle string)")]

    assert len(lst) == 1
    r = lst[0]
    assert isinstance(r, StreamInfoPath)
    assert r.file == "one_minio_entry"
    assert "foo" in r.path.parts
    assert "bar.root" in r.path.parts

    assert mock_servicex_adaptor.query_json["result-format"] == "root-file"


@pytest.mark.asyncio
async def test_stream_bad_request_id_run_root_files_from_minio(mocker):
    "Using the minio interface - the request_id is not known"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    transform_status = mocker.MagicMock(side_effect=ServiceXUnknownRequestID("boom"))
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "123-456", mock_transform_status=transform_status
    )
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )

    with pytest.raises(ServiceXUnknownDataRequestID) as e:
        lst = []
        async for f_info in ds.get_data_rootfiles_uri_stream("(valid qastle string)"):
            lst.append(f_info)

    assert "to know about" in str(e.value)


@pytest.mark.asyncio
async def test_stream_bad_transform_run_root_files_from_minio(mocker):
    "Using the async minio interface - fail to transform (like bad DID)"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    fatal_transform_status = {
        "request_id": "24e59fa2-e1d7-4831-8c7e-82b2efc7c658",
        "did": "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_0000",  # NOQA
        "columns": "Electrons.pt(), Electrons.eta(), Electrons.phi(), Electrons.e(), Muons.pt(), Muons.eta(), Muons.phi(), Muons.e()",  # NOQA
        "selection": None,
        "tree-name": None,
        "image": "sslhep/servicex_func_adl_xaod_transformer:130_reset_cwd",
        "chunk-size": 7000,
        "workers": 1,
        "result-destination": "object-store",
        "result-format": "arrow",
        "workflow-name": "straight_transform",
        "generated-code-cm": None,
        "status": "Fatal",
        "failure-info": "DID Not found mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_0000",  # NOQA
    }
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker,
        "123-456",
        mock_transform_status=mocker.MagicMock(
            side_effect=ServiceXFatalTransformException("DID was BAD")
        ),  # NOQA
        mock_transform_query_status=mocker.MagicMock(
            return_value=fatal_transform_status
        ),
    )
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )

    with pytest.raises(ServiceXFatalTransformException) as e:
        lst = []
        async for f_info in ds.get_data_rootfiles_uri_stream("(valid qastle string)"):
            lst.append(f_info)

    assert "Fatal Error" in str(e.value)
    # Make sure there is no cache entry for this fatal error. (see #189)
    assert mock_cache.remove_query.call_count == 1


@pytest.mark.asyncio
async def test_stream_bad_file_transform_run_root_files_from_minio(mocker):
    "Using the async minio interface, some files will fail to translate."
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_transform_status = mocker.Mock(return_value=(0, 1, 1))
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "123-456", mock_transform_status
    )
    mock_minio_adaptor = MockMinioAdaptor(
        mocker, files=["one_minio_entry", "two_minio_entry"]
    )
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )

    with pytest.raises(ServiceXException) as e:
        lst = []
        async for f_info in ds.get_data_rootfiles_uri_stream("(valid qastle string)"):
            lst.append(f_info)

    assert "Failed to transform all files" in str(e.value)


@pytest.mark.asyncio
async def test_stream_parquet_files_from_minio(mocker):
    "Get a parquet file pulling back minio info as it arrives"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )
    lst = [
        f_info
        async for f_info in ds.get_data_parquet_uri_stream("(valid qastle string)")
    ]

    assert len(lst) == 1
    assert lst[0].bucket == "123-456"
    assert lst[0].file == "one_minio_entry"

    assert mock_servicex_adaptor.query_json["result-format"] == "parquet"


@pytest.mark.asyncio
async def test_async_parquet_files_from_minio(mocker):
    "Get a parquet file pulling back minio info as it arrives"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )
    lst = await ds.get_data_parquet_uri_async("(valid qastle string)")

    assert len(lst) == 1
    assert lst[0].bucket == "123-456"
    assert lst[0].file == "one_minio_entry"

    assert mock_servicex_adaptor.query_json["result-format"] == "parquet"


def test_parquet_files_from_minio(mocker):
    "Get a parquet file pulling back minio info as it arrives"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )
    lst = ds.get_data_parquet_uri("(valid qastle string)")

    assert len(lst) == 1
    assert lst[0].bucket == "123-456"
    assert lst[0].file == "one_minio_entry"

    assert mock_servicex_adaptor.query_json["result-format"] == "parquet"


@pytest.mark.asyncio
async def test_stream_parquet_files(mocker):
    "Get a parquet file pulling back minio info as it arrives"
    file_path = "/foo/bar.root" if os.name != "nt" else r"c:\foo\bar.root"
    mock_cache = build_cache_mock(mocker, data_file_return=file_path)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )
    lst = [
        f_info async for f_info in ds.get_data_parquet_stream("(valid qastle string)")
    ]

    assert len(lst) == 1
    assert lst[0].file == "one_minio_entry"
    assert "foo" in lst[0].path.parts
    assert "bar.root" in lst[0].path.parts
    assert "file://" in lst[0].url

    assert mock_servicex_adaptor.query_json["result-format"] == "parquet"


@pytest.mark.asyncio
async def test_stream_awkward_data(mocker):
    "Get a parquet file pulling back minio info as it arrives"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = asyncmock.MagicMock(spec=DataConverterAdaptor)
    data_adaptor.convert_to_awkward.return_value = {"JetPt": 10}

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )
    lst = [
        f_info async for f_info in ds.get_data_awkward_stream("(valid qastle string)")
    ]

    assert len(lst) == 1
    assert lst[0].file == "one_minio_entry"
    assert isinstance(lst[0].data, dict)


@pytest.mark.asyncio
async def test_stream_pandas_data(mocker):
    "Get a parquet file pulling back minio info as it arrives"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = asyncmock.MagicMock(spec=DataConverterAdaptor)
    data_adaptor.convert_to_pandas.return_value = {"JetPt": 10}

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )
    lst = [
        f_info async for f_info in ds.get_data_pandas_stream("(valid qastle string)")
    ]

    assert len(lst) == 1
    assert lst[0].file == "one_minio_entry"
    assert isinstance(lst[0].data, dict)


@pytest.mark.asyncio
async def test_status_exception(mocker):
    "Make sure status error - like transform not found - is reported all the way to the top"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker,
        "123-456",
        mock_transform_status=mocker.MagicMock(
            side_effect=fe.ServiceXException("bad attempt")
        ),
    )
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=[])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)
    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.get_default_returned_datatype.return_value = "parquet-ftw"

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        config_adaptor=config,
    )
    with pytest.raises(fe.ServiceXException) as e:
        await ds.get_data_awkward_async("(valid qastle string)")
    assert "attempt" in str(e.value)


@pytest.mark.asyncio
async def test_image_spec(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=good_awkward_file_data,
        image="fork-it-over:latest",
    )
    await ds.get_data_rootfiles_async("(valid qastle string)")

    called = mock_servicex_adaptor.query_json
    assert called["image"] == "fork-it-over:latest"


@pytest.mark.asyncio
async def test_no_image_spec(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=good_awkward_file_data,
    )
    await ds.get_data_rootfiles_async("(valid qastle string)")

    called = mock_servicex_adaptor.query_json
    assert "image" not in called


@pytest.mark.asyncio
async def test_max_workers_spec(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        max_workers=50,
    )
    await ds.get_data_rootfiles_async("(valid qastle string)")

    called = mock_servicex_adaptor.query_json
    assert called["workers"] == "50"


@pytest.mark.asyncio
async def test_did_set(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        max_workers=50,
    )
    await ds.get_data_rootfiles_async("(valid qastle string)")

    called = mock_servicex_adaptor.query_json
    assert called["did"] == "localds://mc16_tev:13"
    assert "file-list" not in called


@pytest.mark.asyncio
async def test_file_list_set(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        ["http://file1.root", "http://flie2.root"],
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        max_workers=50,
    )
    await ds.get_data_rootfiles_async("(valid qastle string)")

    called = mock_servicex_adaptor.query_json
    assert called["file-list"] == ["http://file1.root", "http://flie2.root"]
    assert "did" not in called


@pytest.mark.asyncio
async def test_file_list_http(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "http://file1.root",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        max_workers=50,
    )
    await ds.get_data_rootfiles_async("(valid qastle string)")

    called = mock_servicex_adaptor.query_json
    assert called["file-list"] == ["http://file1.root"]
    assert "did" not in called


@pytest.mark.asyncio
async def test_file_list_root(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "root://file1.root",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        max_workers=50,
    )
    await ds.get_data_rootfiles_async("(valid qastle string)")

    called = mock_servicex_adaptor.query_json
    assert called["file-list"] == ["root://file1.root"]
    assert "did" not in called


@pytest.mark.asyncio
async def test_title_spec(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        max_workers=50,
    )
    await ds.get_data_rootfiles_async("(valid qastle string)", title="fork_spoon")

    called = mock_servicex_adaptor.query_json
    assert called["title"] == "fork_spoon"


@pytest.mark.asyncio
async def test_no_title_spec(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        max_workers=50,
    )
    await ds.get_data_rootfiles_async("(valid qastle string)")

    called = mock_servicex_adaptor.query_json
    assert "title" not in called


@pytest.mark.asyncio
async def test_codegen_override(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        max_workers=50,
        codegen="good_codegen",
    )
    await ds.get_data_rootfiles_async("(valid qastle string)")

    called = mock_servicex_adaptor.query_json
    assert called["codegen"] == "good_codegen"


@pytest.mark.asyncio
async def test_codegen_backend_type(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        max_workers=50,
        backend_type="uproot",
    )
    await ds.get_data_rootfiles_async("(valid qastle string)")

    called = mock_servicex_adaptor.query_json
    assert called["codegen"] == "uproot"


@pytest.mark.asyncio
async def test_codegen_default_by_backend(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        max_workers=50,
    )
    await ds.get_data_rootfiles_async("(valid qastle string)")

    called = mock_servicex_adaptor.query_json
    assert called["codegen"] == "atlasr21"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "n_ds, n_query", [(1, 4), (4, 1), (1, 100), (100, 1), (4, 4), (20, 20)]
)
async def test_nqueries_on_n_ds(n_ds: int, n_query: int, mocker):
    "Run some number of queries on some number of datasets"

    def create_ds_query(index: int):
        mock_cache = build_cache_mock(mocker)
        mock_logger = mocker.MagicMock(spec=log_adaptor)
        mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456-{0}")
        mock_minio_adaptor = MockMinioAdaptor(
            mocker, files=[f"one_minio_entry_{index}"]
        )
        data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

        ds = fe.ServiceXDataset(
            f"localds://mc16_tev:13_{index}",
            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
            minio_adaptor=mock_minio_adaptor,  # type:ignore
            cache_adaptor=mock_cache,
            data_convert_adaptor=data_adaptor,
            local_log=mock_logger,
        )
        return [
            ds.get_data_rootfiles_async(f"(valid qastle string {i})")
            for i in range(n_query)
        ]

    all_results = [item for i in range(n_ds) for item in create_ds_query(i)]
    all_wait = await asyncio.gather(*all_results)

    # They are different queries, so they should come down in different files.
    count = 0
    s = set()
    for r in all_wait:
        for f in r:
            s.add(str(f))
            count += 1

    assert len(s) == count


def test_callback_good(mocker):
    "Simple run with expected results, but with the non-async version"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    f_total = None
    f_processed = None
    f_downloaded = None
    f_failed = None

    def check_in(total: Optional[int], processed: int, downloaded: int, failed: int):
        nonlocal f_total, f_processed, f_downloaded, f_failed
        f_total = total
        f_processed = processed
        f_downloaded = downloaded
        f_failed = failed

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        status_callback_factory=lambda ds, title, downloading: check_in,
    )
    ds.get_data_rootfiles("(valid qastle string)")

    assert f_total == 1
    assert f_processed == 1
    assert f_downloaded == 1
    assert f_failed == 0


def test_callback_is_downloading(mocker):
    "Make sure this file download sets the file-download marker in the callback"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    def check_in(total: Optional[int], processed: int, downloaded: int, failed: int):
        pass

    ds_name = None
    ds_downloading = None

    def build_it(ds: DatasetType, title: Optional[str], downloading: bool):
        nonlocal ds_name, ds_downloading
        ds_name = ds
        ds_downloading = downloading
        return check_in

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        status_callback_factory=build_it,
    )
    ds.get_data_rootfiles("(valid qastle string)")

    assert ds_name == "http://one-ds"
    assert ds_downloading


@pytest.mark.asyncio
async def test_callback_is_not_downloading(mocker):
    "Stream download of Minio URLs should not set the download marker"
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    def check_in(total: Optional[int], processed: int, downloaded: int, failed: int):
        pass

    ds_name = None
    ds_downloading = None

    def build_it(ds: DatasetType, title: Optional[str], downloading: bool):
        nonlocal ds_name, ds_downloading
        ds_name = ds
        ds_downloading = downloading
        return check_in

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
        status_callback_factory=build_it,
    )
    _ = [f async for f in ds.get_data_rootfiles_uri_stream("(valid qastle string)")]

    assert ds_name == "http://one-ds"
    assert not ds_downloading


@pytest.mark.asyncio
async def test_callback_none(mocker):
    "Get a root file with a single file"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        status_callback_factory=None,
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
    )
    r = await ds.get_data_rootfiles_async("(valid qastle string)")

    mock_minio_adaptor.mock_download_file.assert_called_with(
        "123-456", "one_minio_entry", "/foo/bar.root"
    )

    assert len(r) == 1
    assert r[0] == Path("/foo/bar.root")


@pytest.mark.asyncio
async def test_cache_awkward_root_confusion(mocker, good_awkward_file_data, tmp_path):
    "Seen in the wild. Ask for root files, then ask for awkward files, and get the root files"

    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
    config.get_default_returned_datatype.return_value = "parquet-ftw"

    # Use the real cache here - we need to return the value we stash.
    c = tmp_path / "cache"
    out_file = c / "data" / "123-456" / "one_minio_entry"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.touch()
    cache = Cache(c)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=cache,
        data_convert_adaptor=good_awkward_file_data,
        local_log=mock_logger,
        config_adaptor=config,
    )
    q_string = "(valid qastle string)"
    r1 = await ds.get_data_rootfiles_async(q_string)
    r2 = await ds.get_data_awkward_async(q_string)

    assert isinstance(r1, list)
    assert isinstance(r2, dict)


@pytest.mark.asyncio
async def test_cache_query_even_with_status_update_failure(
    mocker, short_status_poll_time
):
    """
    1. Start a query.
    1. Get something back files
    1. Second status fails
    1. Make sure the query is marked in the cache (so that a lookup can occur next time)
    """

    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    transform_status = mocker.MagicMock(
        side_effect=[(1, 1, 0), ServiceXException("boom")]
    )
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "123-456", mock_transform_status=transform_status
    )
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        data_convert_adaptor=data_adaptor,
        cache_adaptor=mock_cache,
        local_log=mock_logger,
    )
    with pytest.raises(fe.ServiceXException):
        # Will fail with one file downloaded.
        await ds.get_data_rootfiles_async("(valid qastle string)")

    mock_cache.set_query.assert_called_once()
    mock_cache.remove_query.assert_not_called()


@pytest.mark.asyncio
async def test_servicex_gone_when_redownload_request(mocker, short_status_poll_time):
    """
    1. Our transform query is in the cache.
    2. The files are not yet all in the cache.
    3. We call to get the status, and there is a "not known" error.
    4. The query in the cache should have been removed.
    5. The query is called again.
    """
    mock_cache = build_cache_mock(mocker, query_cache_return="123-456")
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    transform_status = mocker.MagicMock(
        side_effect=[ServiceXUnknownRequestID("boom"), (0, 1, 0)]
    )
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "123-456", mock_transform_status=transform_status
    )
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        data_convert_adaptor=data_adaptor,
        cache_adaptor=mock_cache,
        local_log=mock_logger,
    )

    await ds.get_data_rootfiles_async("(valid qastle string)")

    assert mock_cache.remove_query.call_count == 1


@pytest.mark.asyncio
async def test_servicex_empty_minio_container(mocker, short_status_poll_time):
    """
    1. Our transform query is in the cache.
    2. The files are not yet all in the cache.
    3. We call to get the status, and there is a "not known" error.
    4. The query in the cache should have been removed.
    5. The query is called again.
    """
    mock_cache = build_cache_mock(mocker, query_cache_return="123-456")
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    transform_status = mocker.MagicMock(
        side_effect=[ServiceXNoFilesInCache("boom"), (0, 1, 0)]
    )
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "123-456", mock_transform_status=transform_status
    )
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        data_convert_adaptor=data_adaptor,
        cache_adaptor=mock_cache,
        local_log=mock_logger,
    )

    await ds.get_data_rootfiles_async("(valid qastle string)")

    assert mock_cache.remove_query.call_count == 1


@pytest.mark.asyncio
async def test_servicex_gone_when_redownload_request_urls(
    mocker, short_status_poll_time
):
    """
    1. Our transform query is in the cache.
    2. We call to get the URL's to feed the user
    3. Minio is empty
    4. Transform must be resubmitted.
    """
    mock_cache = build_cache_mock(mocker, query_cache_return="123-456")
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(
        mocker,
        files=["one_minio_entry"],
        exception_on_access=minio.error.NoSuchBucket("nope"),
    )
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        data_convert_adaptor=data_adaptor,
        cache_adaptor=mock_cache,
        local_log=mock_logger,
    )

    [f async for f in ds.get_data_rootfiles_uri_stream("(valid qastle string)")]


@pytest.mark.asyncio
async def test_servicex_transformer_failure_reload(mocker, short_status_poll_time):
    """
    1. Start a transform
    2. A file is marked as failing
    3. The query is not cached (so it can be run again next time)
    """
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    transform_status = mocker.MagicMock(return_value=(0, 0, 1))
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "123-456", mock_transform_status=transform_status
    )
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        data_convert_adaptor=data_adaptor,
        cache_adaptor=mock_cache,
        local_log=mock_logger,
    )

    with pytest.raises(fe.ServiceXException):
        # Will fail with one skipped file.
        await ds.get_data_rootfiles_async("(valid qastle string)")

    mock_cache.set_query.assert_called_once()
    mock_cache.remove_query.assert_called_once()


@pytest.mark.asyncio
async def test_servicex_transformer_failure_errors_dumped(
    mocker, short_status_poll_time
):
    """
    1. Start a transform
    2. A file is marked as failing
    3. Make sure that the dump errors is called.
    """
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    transform_status = mocker.MagicMock(return_value=(0, 0, 1))
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "123-456", mock_transform_status=transform_status
    )
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        data_convert_adaptor=data_adaptor,
        cache_adaptor=mock_cache,
        local_log=mock_logger,
    )

    with pytest.raises(fe.ServiceXException):
        # Will fail with one skipped file.
        await ds.get_data_rootfiles_async("(valid qastle string)")

    assert mock_servicex_adaptor.dump_query_errors_count == 1


@pytest.mark.asyncio
async def test_good_run_root_bad_DID(mocker):
    "Get a root file with a single file"
    mock_cache = build_cache_mock(mocker, data_file_return="/foo/bar.root")

    fatal_transform_status = {
        "request_id": "24e59fa2-e1d7-4831-8c7e-82b2efc7c658",
        "did": "mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_0000",  # NOQA
        "columns": "Electrons.pt(), Electrons.eta(), Electrons.phi(), Electrons.e(), Muons.pt(), Muons.eta(), Muons.phi(), Muons.e()",  # NOQA
        "selection": None,
        "tree-name": None,
        "image": "sslhep/servicex_func_adl_xaod_transformer:130_reset_cwd",
        "chunk-size": 7000,
        "workers": 1,
        "result-destination": "object-store",
        "result-format": "arrow",
        "workflow-name": "straight_transform",
        "generated-code-cm": None,
        "status": "Fatal",
        "failure-info": "DID Not found mc15_13TeV:mc15_13TeV.361106.PowhegPythia8EvtGen_AZNLOCTEQ6L1_Zee.merge.DAOD_STDM3.e3601_s2576_s2132_r6630_r6264_p2363_tid05630052_0000",  # NOQA
    }
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker,
        "123-456",
        mock_transform_status=mocker.MagicMock(
            side_effect=ServiceXFatalTransformException("DID was BAD")
        ),  # NOQA
        mock_transform_query_status=mocker.MagicMock(
            return_value=fatal_transform_status
        ),
    )

    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        data_convert_adaptor=data_adaptor,
        cache_adaptor=mock_cache,
        local_log=mock_logger,
    )

    with pytest.raises(ServiceXFatalTransformException) as e:
        await ds.get_data_rootfiles_async("(valid qastle string)")

    assert "DID Not found mc15" in str(e.value)
    # Make sure the query is not cached (see #189)
    assert mock_cache.remove_query.call_count == 1


@pytest.mark.asyncio
async def test_servicex_in_progress_lock_cleared(mocker, short_status_poll_time):
    """
    1. Start a transform
    2. A file is marked as failing
    3. The query is not cached (so it can be run again next time)
    """
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    transform_status = mocker.MagicMock(return_value=(0, 0, 1))
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "123-456", mock_transform_status=transform_status
    )
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
    )

    with pytest.raises(fe.ServiceXException):
        # Will fail with one skipped file.
        await ds.get_data_rootfiles_async("(valid qastle string)")

    import servicex.servicex_utils as sxu

    assert len(sxu._in_progress_items) == 0


@pytest.mark.asyncio
async def test_download_cached_nonet(mocker, tmp_path: Path):
    """
    Check that we do not use the network if we have already cached a file.
        - Cache is populated
        - the status calls are not made more than for the first time
        - the calls to minio are only made the first time (the list_objects, for example)
    """
    f1 = tmp_path / "file1.root"
    f1.touch()
    mock_cache = build_cache_mock(
        mocker, query_cache_return="123-455", files=[("f1", f1)]
    )
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_bomb = mocker.Mock(side_effect=RuntimeError("should not be called"))
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker,
        "XXX-XXX",
        mock_transform_status=mock_bomb,
        mock_query=mock_bomb,
        mock_transform_query_status=mock_bomb,
    )
    mock_minio_adaptor = MockMinioAdaptor(
        mocker, files=["one_minio_entry", "two_minio_entry"]
    )
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
    )
    await ds.get_data_rootfiles_async("(valid qastle string")


@pytest.mark.asyncio
async def test_download_write_to_inmem_cache(mocker):
    """
    Check that we do not use the network if we have already cached a file.
        - Cache is populated
        - the status calls are not made more than for the first time
        - the calls to minio are only made the first time (the list_objects, for example)
    """
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_transform_status = mocker.Mock(side_effect=[(0, 2, 0)])
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "123-456", mock_transform_status
    )
    mock_minio_adaptor = MockMinioAdaptor(
        mocker, files=["one_minio_entry", "two_minio_entry"]
    )
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        data_convert_adaptor=data_adaptor,
        cache_adaptor=mock_cache,
        local_log=mock_logger,
    )
    await ds.get_data_rootfiles_async("(valid qastle string")

    # Check the the number of times we called for a transform is good.
    mock_cache.set_inmem.assert_called_once()


@pytest.mark.asyncio
async def test_download_cached_awkward(mocker, good_awkward_file_data):
    "Run two right after each other - they should return the same data in memory"
    fork_it = ["data", "is", "there"]
    mock_cache = build_cache_mock(mocker, in_memory=fork_it)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_transform_status = mocker.Mock(side_effect=[(0, 2, 0)])
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "123-456", mock_transform_status
    )
    mock_minio_adaptor = MockMinioAdaptor(
        mocker, files=["one_minio_entry", "two_minio_entry"]
    )

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=good_awkward_file_data,
        local_log=mock_logger,
    )
    a1 = await ds.get_data_rootfiles_async("(valid qastle string")

    assert a1 is fork_it


@pytest.mark.asyncio
async def test_simultaneous_query_not_requeued(mocker, good_awkward_file_data):
    "Run two at once - they should not both generate queries as they are identical"

    async def do_query():
        mock_cache = build_cache_mock(mocker, make_in_memory_work=True)
        mock_logger = mocker.MagicMock(spec=log_adaptor)
        mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
        mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
        config = mocker.MagicMock(spec=ServiceXConfigAdaptor)
        config.get_default_returned_datatype.return_value = "parquet-ftw"

        ds = fe.ServiceXDataset(
            "localds://dude-is-funny",
            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
            minio_adaptor=mock_minio_adaptor,  # type: ignore
            data_convert_adaptor=good_awkward_file_data,
            cache_adaptor=mock_cache,
            local_log=mock_logger,
            config_adaptor=config,
        )
        return await ds.get_data_awkward_async("(valid qastle string")

    a1, a2 = await asyncio.gather(*[do_query(), do_query()])  # type: ignore
    assert a1 is a2


@pytest.mark.asyncio
async def test_good_minio_factory_from_best(mocker):
    "Get a root file with a single file"
    mock_cache = build_cache_mock(
        mocker,
        data_file_return="/foo/bar.root",
        query_status_lookup_return={"request_id": "bogus"},
    )
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_logger = mocker.MagicMock(spec=log_adaptor)

    mock_minio_factory = mocker.MagicMock(spec=MinioAdaptorFactory)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_minio_factory.from_best.return_value = mock_minio_adaptor
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_factory,
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
    )
    await ds.get_data_rootfiles_async("(valid qastle string)")

    mock_minio_adaptor.mock_download_file.assert_called_once()
    mock_minio_factory.from_best.assert_called_once()
    assert mock_minio_factory.from_best.call_args[0][0] == {"request_id": "bogus"}


@pytest.mark.asyncio
async def test_user_deleted_query_status_files(mocker, tmp_path: Path):
    """
    1. User has made this query before, and everything was cached correctly
    2. User deletes the query status file only
    3. System should re-query the status and replace the file.
    """
    f1 = tmp_path / "file1.root"
    f1.touch()
    mock_cache = build_cache_mock(
        mocker, query_cache_return="123-455", files=[("f1", f1)]
    )
    mock_cache.query_status_exists.return_value = False

    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_bomb = mocker.Mock(side_effect=RuntimeError("should not be called"))
    mock_servicex_adaptor = MockServiceXAdaptor(
        mocker, "XXX-XXX", mock_transform_status=mock_bomb, mock_query=mock_bomb
    )

    mock_minio_adaptor = MockMinioAdaptor(
        mocker, files=["one_minio_entry", "two_minio_entry"]
    )
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "http://one-ds",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        data_convert_adaptor=data_adaptor,
        local_log=mock_logger,
    )
    await ds.get_data_rootfiles_async("(valid qastle string")

    mock_cache.set_query_status.assert_called_once()


@pytest.mark.asyncio
async def test_user_deleted_query_status_stream(mocker):
    """
    1. User has made this query before, and everything was cached correctly
    2. User deletes the query status file only
    3. System should re-query the status and replace the file.
    4. Then stream url's as expected.
    """
    mock_cache = build_cache_mock(
        mocker, data_file_return="/foo/bar.root", query_cache_return="123-456"
    )
    mock_cache.query_status_exists.return_value = False

    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=["one_minio_entry"])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    data_adaptor = mocker.MagicMock(spec=DataConverterAdaptor)

    ds = fe.ServiceXDataset(
        "localds://mc16_tev:13",
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        data_convert_adaptor=data_adaptor,
    )
    [f async for f in ds.get_data_rootfiles_uri_stream("(valid qastle string)")]

    assert mock_cache.set_query_status.call_count == 2
