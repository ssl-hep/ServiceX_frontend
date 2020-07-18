import asyncio
import os
from pathlib import Path
import shutil
import tempfile
from typing import List, Optional

import pandas as pd
import pytest

import servicex as fe
from servicex.utils import ServiceXException, ServiceXUnknownRequestID, log_adaptor

from .conftest import (  # NOQA
    MockMinioAdaptor,
    MockServiceXAdaptor,
    build_cache_mock,
)


def clean_fname(fname: str):
    'No matter the string given, make it an acceptable filename'
    return fname.replace('*', '_') \
                .replace(';', '_') \
                .replace(':', '_')


def test_default_ctor():
    fe.ServiceXDataset('localds://dude')


@pytest.mark.asyncio
async def test_good_run_root_files(mocker):
    'Get a root file with a single file'
    mock_cache = build_cache_mock(mocker)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    filename_func = mocker.Mock(return_value="/foo/bar.root")

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            file_name_func=filename_func,
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    r = await ds.get_data_rootfiles_async('(valid qastle string)')

    mock_minio_adaptor.mock_download_file.assert_called_with(
        "123-456",
        "one_minio_entry",
        "/foo/bar.root")

    assert len(r) == 1
    assert r[0] == '/foo/bar.root'


@pytest.mark.asyncio
async def test_skipped_file(mocker):
    '''
    ServiceX should throw if a file is marked as "skipped".
    '''
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_transform_status = mocker.Mock(return_value=(0, 1, 1))
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456", mock_transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry'])

    with pytest.raises(fe.ServiceXException) as e:
        ds = fe.ServiceXDataset('http://one-ds',
                                servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                                minio_adaptor=mock_minio_adaptor,  # type: ignore
                                cache_adaptor=mock_cache,
                                local_log=mock_logger)
        ds.get_data_rootfiles('(valid qastle string)')

    assert "Failed to transform" in str(e.value)


def test_good_run_root_files_no_async(mocker):
    'Make sure the non-async version works'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry'])
    filename_func = mocker.Mock(return_value="/foo/bar.root")

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            file_name_func=filename_func,
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)

    r = ds.get_data_rootfiles('(valid qastle string)')
    assert len(r) == 2
    assert r[0] == '/foo/bar.root'


@pytest.mark.asyncio
async def test_good_run_root_files_pause(mocker, short_status_poll_time):
    'Get a root file with a single file'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_transform_status = mocker.Mock(side_effect=[(1, 0, 0), (0, 1, 0)])
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456", mock_transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert len(r) == 1
    assert len(mock_servicex_adaptor.transform_status.mock_calls) == 2


@pytest.mark.asyncio
async def test_good_run_files_back_4_order_1(mocker):
    'Simple run with expected results'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry',
                                                         'three_minio_entry',
                                                         'four_minio_entry'])

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert isinstance(r, list)
    assert len(r) == 4
    s_r = sorted([f.name for f in r])
    assert [f.name for f in r] == s_r


@pytest.mark.asyncio
async def test_good_run_files_back_4_order_2(mocker):
    'Simple run with expected results'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['four_minio_entry', 'three_minio_entry',
                                                         'two_minio_entry',
                                                         'one_minio_entry'])

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert isinstance(r, list)
    assert len(r) == 4
    s_r = sorted([f.name for f in r])
    assert [f.name for f in r] == s_r


@pytest.mark.asyncio
async def test_good_run_files_back_4_unordered(mocker):
    'Simple run; should return alphabetized list'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry',
                                                         'three_minio_entry',
                                                         'four_minio_entry'])
    mocker.patch('servicex.utils.default_file_cache_name', Path('/tmp/servicex-testing'))

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert isinstance(r, list)
    assert len(r) == 4
    assert r[0] == Path('/tmp/servicex-testing/123-456/four_minio_entry')
    assert r[1] == Path('/tmp/servicex-testing/123-456/one_minio_entry')
    assert r[2] == Path('/tmp/servicex-testing/123-456/three_minio_entry')
    assert r[3] == Path('/tmp/servicex-testing/123-456/two_minio_entry')


@pytest.mark.asyncio
async def test_good_download_files_parquet(mocker, short_status_poll_time):
    'Simple run with expected results'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_transform_status = mocker.Mock(side_effect=[(1, 0, 0), (0, 1, 0)])
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456", mock_transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])
    mocker.patch('servicex.utils.default_file_cache_name', Path('/tmp/servicex-testing'))

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    r = await ds.get_data_parquet_async('(valid qastle string)')
    assert isinstance(r, list)
    assert len(r) == 1
    assert r[0] == Path('/tmp/servicex-testing/123-456/one_minio_entry')
    assert len(mock_servicex_adaptor.transform_status.mock_calls) == 2


@pytest.mark.asyncio
async def test_good_run_single_ds_1file_pandas(mocker, good_pandas_file_data):
    'Simple run with expected results'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    r = await ds.get_data_pandas_df_async('(valid qastle string)')
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 6


@pytest.mark.asyncio
async def test_good_run_single_ds_1file_awkward(mocker, good_awkward_file_data):
    'Simple run with expected results'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    r = await ds.get_data_awkward_async('(valid qastle string)')
    assert isinstance(r, dict)
    assert len(r) == 1
    assert b'JetPt' in r
    assert len(r[b'JetPt']) == 6


@pytest.mark.asyncio
async def test_good_run_single_ds_2file_pandas(mocker, good_pandas_file_data):
    'Simple run with expected results'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry'])

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    r = await ds.get_data_pandas_df_async('(valid qastle string)')
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 6 * 2


@pytest.mark.asyncio
async def test_good_run_single_ds_2file_awkward(mocker, good_awkward_file_data):
    'Simple run with expected results'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry'])

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    r = await ds.get_data_awkward_async('(valid qastle string)')
    assert isinstance(r, dict)
    assert len(r) == 1
    assert b'JetPt' in r
    assert len(r[b'JetPt']) == 6 * 2


@pytest.mark.asyncio
async def test_status_exception(mocker):
    'Make sure status error - like transform not found - is reported all the way to the top'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = \
        MockServiceXAdaptor(
            mocker, '123-456',
            mock_transform_status=mocker.MagicMock(side_effect=fe.ServiceXException('bad attempt'))
        )
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=[])

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    with pytest.raises(fe.ServiceXException) as e:
        await ds.get_data_awkward_async('(valid qastle string)')
    assert "attempt" in str(e.value)


@pytest.mark.asyncio
async def test_image_spec(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger,
                            image='fork-it-over:latest')
    await ds.get_data_rootfiles_async('(valid qastle string)')

    called = mock_servicex_adaptor.query_json
    assert called['image'] == 'fork-it-over:latest'


@pytest.mark.asyncio
async def test_max_workers_spec(mocker, good_awkward_file_data):
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger,
                            max_workers=50)
    await ds.get_data_rootfiles_async('(valid qastle string)')

    called = mock_servicex_adaptor.query_json
    assert called['workers'] == '50'


@pytest.mark.asyncio
@pytest.mark.parametrize("n_ds, n_query", [(1, 4), (4, 1), (1, 100), (100, 1), (4, 4), (20, 20)])
async def test_nqueries_on_n_ds(n_ds: int, n_query: int, mocker):
    'Run some number of queries on some number of datasets'
    def create_ds_query(index: int):
        mock_cache = build_cache_mock(mocker)
        mock_logger = mocker.MagicMock(spec=log_adaptor)
        mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456-{0}")
        mock_minio_adaptor = MockMinioAdaptor(mocker, files=[f'one_minio_entry_{index}'])

        ds = fe.ServiceXDataset(f'localds://mc16_tev:13_{index}',
                                servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                                minio_adaptor=mock_minio_adaptor,  # type:ignore
                                cache_adaptor=mock_cache,
                                local_log=mock_logger)
        return [ds.get_data_rootfiles_async(f'(valid qastle string {i})') for i in range(n_query)]

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


@pytest.mark.asyncio
async def test_download_to_temp_dir(mocker):
    'Download to a specified storage directory'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    tmp = os.path.join(tempfile.gettempdir(), 'my_test_dir')
    if os.path.exists(tmp):
        shutil.rmtree(tmp)
    os.mkdir(tmp)

    ds = fe.ServiceXDataset('localds://dude-is-funny',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger,
                            storage_directory=tmp)
    r = await ds.get_data_rootfiles_async('(valid qastle string')

    assert isinstance(r, List)
    assert len(r) == 1
    assert str(r[0]).startswith(tmp)


@pytest.mark.asyncio
async def test_download_to_lambda_dir(mocker):
    'Download to files using a file name function callback'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    tmp = os.path.join(tempfile.gettempdir(), 'my_test_dir')
    if os.path.exists(tmp):
        shutil.rmtree(tmp)
    os.mkdir(tmp)

    ds = fe.ServiceXDataset(
        'localds://dude-is-funny',
        servicex_adaptor=mock_servicex_adaptor,  # type: ignore
        minio_adaptor=mock_minio_adaptor,  # type: ignore
        cache_adaptor=mock_cache,
        local_log=mock_logger,
        file_name_func=lambda rid, obj_name: Path(f'{tmp}\\{clean_fname(obj_name)}'))
    r = await ds.get_data_rootfiles_async('(valid qastle string')

    assert isinstance(r, List)
    assert len(r) == 1
    assert str(r[0]).startswith(tmp)


@pytest.mark.asyncio
async def test_download_bad_params_filerename(mocker):
    'Specify both a storage directory and a filename rename func - illegal'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    tmp = os.path.join(tempfile.gettempdir(), 'my_test_dir')
    if os.path.exists(tmp):
        shutil.rmtree(tmp)
    os.mkdir(tmp)
    with pytest.raises(Exception) as e:
        fe.ServiceXDataset(
            'http://one-ds',
            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
            minio_adaptor=mock_minio_adaptor,  # type: ignore
            cache_adaptor=mock_cache,
            local_log=mock_logger,
            storage_directory=tmp,
            file_name_func=lambda rid, obj_name: Path(f'{tmp}\\{clean_fname(obj_name)}'))
    assert "only specify" in str(e.value)


def test_callback_good(mocker):
    'Simple run with expected results, but with the non-async version'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

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

    ds = fe.ServiceXDataset('http://one-ds',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger,
                            status_callback_factory=lambda ds: check_in)
    ds.get_data_rootfiles('(valid qastle string)')

    assert f_total == 1
    assert f_processed == 1
    assert f_downloaded == 1
    assert f_failed == 0


@pytest.mark.asyncio
async def test_callback_none(mocker):
    'Get a root file with a single file'
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])
    filename_func = mocker.Mock(return_value="/foo/bar.root")

    ds = fe.ServiceXDataset('localds://mc16_tev:13',
                            status_callback_factory=None,
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            file_name_func=filename_func,
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    r = await ds.get_data_rootfiles_async('(valid qastle string)')

    mock_minio_adaptor.mock_download_file.assert_called_with(
        "123-456",
        "one_minio_entry",
        "/foo/bar.root")

    assert len(r) == 1
    assert r[0] == '/foo/bar.root'


@pytest.mark.asyncio
async def test_cache_query_even_with_status_update_failure(mocker, short_status_poll_time):
    '''
    1. Start a query.
    1. Get something back files
    1. Second status fails
    1. Make sure the query is marked in the cache (so that a lookup can occur next time)
    '''

    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    transform_status = mocker.MagicMock(side_effect=[(1, 1, 0), ServiceXException('boom')])
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456",
                                                mock_transform_status=transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceXDataset('http://one-ds',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    with pytest.raises(fe.ServiceXException):
        # Will fail with one file downloaded.
        await ds.get_data_rootfiles_async('(valid qastle string)')

    mock_cache.set_query.assert_called_once()
    mock_cache.remove_query.assert_not_called()


@pytest.mark.asyncio
async def test_servicex_gone_when_redownload_request(mocker, short_status_poll_time):
    '''
    1. Our transform query is in the cache.
    2. The files are not yet all in the cache.
    3. We call to get the status, and there is a "not known" error.
    4. The query in the cache should have been removed.

    This will force the system to re-start the query next time it is called.
    '''
    mock_cache = build_cache_mock(mocker, query_cache_return='123-456')
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    transform_status = mocker.MagicMock(side_effect=ServiceXUnknownRequestID('boom'))
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456",
                                                mock_transform_status=transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceXDataset('http://one-ds',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)

    with pytest.raises(ServiceXException) as e:
        # Will fail with one file downloaded.
        await ds.get_data_rootfiles_async('(valid qastle string)')

    assert 'resubmit' in str(e.value)

    mock_cache.set_query.assert_not_called()
    mock_cache.remove_query.assert_called_once()


@pytest.mark.asyncio
async def test_servicex_transformer_failure_reload(mocker, short_status_poll_time):
    '''
    1. Start a transform
    2. A file is marked as failing
    3. The query is not cached (so it can be run again next time)
    '''
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    transform_status = mocker.MagicMock(return_value=(0, 0, 1))
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456",
                                                mock_transform_status=transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceXDataset('http://one-ds',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)

    with pytest.raises(fe.ServiceXException):
        # Will fail with one skipped file.
        await ds.get_data_rootfiles_async('(valid qastle string)')

    mock_cache.set_query.assert_called_once()
    mock_cache.remove_query.assert_called_once()


@pytest.mark.asyncio
async def test_servicex_in_progress_lock_cleared(mocker, short_status_poll_time):
    '''
    1. Start a transform
    2. A file is marked as failing
    3. The query is not cached (so it can be run again next time)
    '''
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    transform_status = mocker.MagicMock(return_value=(0, 0, 1))
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456",
                                                mock_transform_status=transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceXDataset('http://one-ds',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)

    with pytest.raises(fe.ServiceXException):
        # Will fail with one skipped file.
        await ds.get_data_rootfiles_async('(valid qastle string)')

    import servicex.servicex_utils as sxu
    assert len(sxu._in_progress_items) == 0


@pytest.mark.asyncio
async def test_download_cached_nonet(mocker):
    '''
    Check that we do not use the network if we have already cached a file.
        - Cache is populated
        - the status calls are not made more than for the first time
        - the calls to minio are only made the first time (the list_objects, for example)
    '''
    mock_cache = build_cache_mock(mocker, query_cache_return='123-455',
                                  files=[('f1', 'file1.root')])
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_transform_status = mocker.Mock(side_effect=[(0, 2, 0)])
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456", mock_transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry'])

    ds = fe.ServiceXDataset('http://one-ds',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    await ds.get_data_rootfiles_async('(valid qastle string')

    # Check the the number of times we called for a transform is good.
    mock_transform_status.submit_query.assert_not_called()
    mock_transform_status.transform_status.assert_not_called()


@pytest.mark.asyncio
async def test_download_write_to_inmem_cache(mocker):
    '''
    Check that we do not use the network if we have already cached a file.
        - Cache is populated
        - the status calls are not made more than for the first time
        - the calls to minio are only made the first time (the list_objects, for example)
    '''
    mock_cache = build_cache_mock(mocker)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_transform_status = mocker.Mock(side_effect=[(0, 2, 0)])
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456", mock_transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry'])

    ds = fe.ServiceXDataset('http://one-ds',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    await ds.get_data_rootfiles_async('(valid qastle string')

    # Check the the number of times we called for a transform is good.
    mock_cache.set_inmem.assert_called_once()


@pytest.mark.asyncio
async def test_download_cached_awkward(mocker, good_awkward_file_data):
    'Run two right after each other - they should return the same data in memory'
    fork_it = ['data', 'is', 'there']
    mock_cache = build_cache_mock(mocker, in_memory=fork_it)
    mock_logger = mocker.MagicMock(spec=log_adaptor)
    mock_transform_status = mocker.Mock(side_effect=[(0, 2, 0)])
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456", mock_transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry'])

    ds = fe.ServiceXDataset('http://one-ds',
                            servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                            minio_adaptor=mock_minio_adaptor,  # type: ignore
                            cache_adaptor=mock_cache,
                            local_log=mock_logger)
    a1 = await ds.get_data_rootfiles_async('(valid qastle string')

    assert a1 is fork_it


@pytest.mark.asyncio
async def test_simultaneous_query_not_requeued(mocker, good_awkward_file_data):
    'Run two at once - they should not both generate queires as they are identical'

    async def do_query():
        mock_cache = build_cache_mock(mocker, make_in_memory_work=True)
        mock_logger = mocker.MagicMock(spec=log_adaptor)
        mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
        mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])
        ds = fe.ServiceXDataset('localds://dude-is-funny',
                                servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                                minio_adaptor=mock_minio_adaptor,  # type: ignore
                                cache_adaptor=mock_cache,
                                local_log=mock_logger)
        return await ds.get_data_awkward_async('(valid qastle string')

    a1, a2 = await asyncio.gather(*[do_query(), do_query()])  # type: ignore
    assert a1 is a2
