import asyncio
import os
import shutil
import tempfile
from typing import List, Optional
from pathlib import Path
from unittest import mock

import pandas as pd
import pytest

import servicex as fe

from .utils_for_testing import (  # NOQA
    ClientSessionMocker,
    delete_default_downloaded_files,
    files_in_minio,
    good_pandas_file_data,
    good_transform_request,
    good_awkward_file_data,
    no_files_in_minio,
    short_status_poll_time,
    bad_transform_request,
    servicex_state_machine,
    servicex_adaptor,
    MockServiceXAdaptor,
    MockMinioAdaptor,
    build_cache_mock
) # NOQA


# def clean_fname(fname: str):
#     'No matter the string given, make it an acceptable filename'
#     return fname.replace('*', '_') \
#                 .replace(';', '_') \
#                 .replace(':', '_')


@pytest.mark.asyncio
async def test_good_run_root_files(mocker):
    'Get a root file with a single file'
    mock_cache = build_cache_mock(mocker)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])
    filename_func = mocker.Mock(return_value="/foo/bar.root")

    ds = fe.ServiceX('localds://mc16_tev:13',
                     servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                     minio_adaptor=mock_minio_adaptor,  # type: ignore
                     file_name_func=filename_func,
                     cache_adaptor=mock_cache)
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
    mock_transform_status = mocker.Mock(return_value=(0, 1, 1))
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456", mock_transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry'])

    with pytest.raises(fe.ServiceXException) as e:
        ds = fe.ServiceX('http://one-ds',
                         servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                         minio_adaptor=mock_minio_adaptor,  # type: ignore
                         cache_adaptor=mock_cache)
        ds.get_data_rootfiles('(valid qastle string)')

    assert "failed to transform" in str(e.value)


def test_good_run_root_files_no_async(mocker):
    'Make sure the non-async version works'
    mock_cache = build_cache_mock(mocker)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry'])
    filename_func = mocker.Mock(return_value="/foo/bar.root")

    ds = fe.ServiceX('localds://mc16_tev:13', servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                     minio_adaptor=mock_minio_adaptor,  # type: ignore
                     file_name_func=filename_func,
                     cache_adaptor=mock_cache)

    r = ds.get_data_rootfiles('(valid qastle string)')
    assert len(r) == 2
    assert r[0] == '/foo/bar.root'


@pytest.mark.asyncio
async def test_good_run_root_files_pause(mocker, short_status_poll_time):
    'Get a root file with a single file'
    mock_cache = build_cache_mock(mocker)
    mock_transform_status = mocker.Mock(side_effect=[(1, 0, 0), (0, 1, 0)])
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456", mock_transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceX('localds://mc16_tev:13',
                     servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                     minio_adaptor=mock_minio_adaptor,  # type: ignore
                     cache_adaptor=mock_cache)
    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert len(r) == 1
    assert len(mock_servicex_adaptor.transform_status.mock_calls) == 2


@pytest.mark.asyncio
async def test_good_run_files_back_4_order_1(mocker):
    'Simple run with expected results'
    mock_cache = build_cache_mock(mocker)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry', 'three_minio_entry',
                                                         'four_minio_entry'])

    ds = fe.ServiceX('localds://mc16_tev:13',
                     servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                     minio_adaptor=mock_minio_adaptor,  # type: ignore
                     cache_adaptor=mock_cache)
    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert isinstance(r, list)
    assert len(r) == 4
    s_r = sorted([f.name for f in r])
    assert [f.name for f in r] == s_r


@pytest.mark.asyncio
async def test_good_run_files_back_4_order_2(mocker):
    'Simple run with expected results'
    mock_cache = build_cache_mock(mocker)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['four_minio_entry', 'three_minio_entry', 'two_minio_entry',
                                                         'one_minio_entry'])

    ds = fe.ServiceX('localds://mc16_tev:13',
                     servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                     minio_adaptor=mock_minio_adaptor,  # type: ignore
                     cache_adaptor=mock_cache)
    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert isinstance(r, list)
    assert len(r) == 4
    s_r = sorted([f.name for f in r])
    assert [f.name for f in r] == s_r


@pytest.mark.asyncio
async def test_good_run_files_back_4_unordered(mocker):
    'Simple run; should return alphabetized list'
    mock_cache = build_cache_mock(mocker)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry', 'three_minio_entry',
                                                         'four_minio_entry'])
    mocker.patch('servicex.utils.default_file_cache_name', Path('/tmp/servicex-testing'))

    ds = fe.ServiceX('localds://mc16_tev:13',
                     servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                     minio_adaptor=mock_minio_adaptor,  # type: ignore
                     cache_adaptor=mock_cache)
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
    mock_transform_status = mocker.Mock(side_effect=[(1, 0, 0), (0, 1, 0)])
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456", mock_transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])
    mocker.patch('servicex.utils.default_file_cache_name', Path('/tmp/servicex-testing'))

    ds = fe.ServiceX('localds://mc16_tev:13',
                     servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                     minio_adaptor=mock_minio_adaptor,  # type: ignore
                     cache_adaptor=mock_cache)
    r = await ds.get_data_parquet_async('(valid qastle string)')
    assert isinstance(r, list)
    assert len(r) == 1
    assert r[0] == Path('/tmp/servicex-testing/123-456/one_minio_entry')
    assert len(mock_servicex_adaptor.transform_status.mock_calls) == 2


@pytest.mark.asyncio
async def test_good_run_single_ds_1file_pandas(mocker, good_pandas_file_data):
    'Simple run with expected results'
    mock_cache = build_cache_mock(mocker)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceX('localds://mc16_tev:13',
                     servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                     minio_adaptor=mock_minio_adaptor,  # type: ignore
                     cache_adaptor=mock_cache)
    r = await ds.get_data_pandas_df_async('(valid qastle string)')
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 6


@pytest.mark.asyncio
async def test_good_run_single_ds_1file_awkward(mocker, good_awkward_file_data):
    'Simple run with expected results'
    mock_cache = build_cache_mock(mocker)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceX('localds://mc16_tev:13',
                     servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                     minio_adaptor=mock_minio_adaptor,  # type: ignore
                     cache_adaptor=mock_cache)
    r = await ds.get_data_awkward_async('(valid qastle string)')
    assert isinstance(r, dict)
    assert len(r) == 1
    assert b'JetPt' in r
    assert len(r[b'JetPt']) == 6


@pytest.mark.asyncio
async def test_good_run_single_ds_2file_pandas(mocker, good_pandas_file_data):
    'Simple run with expected results'
    mock_cache = build_cache_mock(mocker)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry'])

    ds = fe.ServiceX('localds://mc16_tev:13',
                     servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                     minio_adaptor=mock_minio_adaptor,  # type: ignore
                     cache_adaptor=mock_cache)
    r = await ds.get_data_pandas_df_async('(valid qastle string)')
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 6 * 2


@pytest.mark.asyncio
async def test_good_run_single_ds_2file_awkward(mocker, good_awkward_file_data):
    'Simple run with expected results'
    mock_cache = build_cache_mock(mocker)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry'])

    ds = fe.ServiceX('localds://mc16_tev:13',
                     servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                     minio_adaptor=mock_minio_adaptor,  # type: ignore
                     cache_adaptor=mock_cache)
    r = await ds.get_data_awkward_async('(valid qastle string)')
    assert isinstance(r, dict)
    assert len(r) == 1
    assert b'JetPt' in r
    assert len(r[b'JetPt']) == 6 * 2


@pytest.mark.asyncio
async def test_status_exception(mocker):
    'Make sure status error - like transform not found - is reported all the way to the top'
    mock_cache = build_cache_mock(mocker)
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, '123-456',
                                                mock_transform_status=mocker.MagicMock(side_effect=fe.ServiceXException('bad attempt')))
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=[])

    ds = fe.ServiceX('localds://mc16_tev:13',
                     servicex_adaptor=mock_servicex_adaptor,  # type: ignore
                     minio_adaptor=mock_minio_adaptor,  # type: ignore
                     cache_adaptor=mock_cache)
    with pytest.raises(fe.ServiceXException) as e:
        await ds.get_data_awkward_async('(valid qastle string)')
    assert "attempt" in str(e.value)


@pytest.mark.skip
@pytest.mark.asyncio
async def test_image_spec(mocker, good_awkward_file_data):
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceX('localds://mc16_tev:13', servicex_adaptor=mock_servicex_adaptor,
                     minio_adaptor=mock_minio_adaptor, image='fork-it-over:latest')
    await ds.get_data_awkward_async('(valid qastle string)')

    called = good_transform_request.call_args[0][2]
    assert called['image'] == 'fork-it-over:latest'


@pytest.mark.skip
@pytest.mark.asyncio
async def test_max_workers_spec(mocker, good_awkward_file_data):
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    ds = fe.ServiceX('localds://mc16_tev:13', servicex_adaptor=mock_servicex_adaptor,
                     minio_adaptor=mock_minio_adaptor, max_workers=50)
    await ds.get_data_awkward_async('(valid qastle string)')

    called = ds[0][2]
    assert called['workers'] == '50'


@pytest.mark.skip
@pytest.mark.asyncio
async def test_servicex_rejects_transform_request(mocker, bad_transform_request):
    'Simple run bomb during transform query'
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    with pytest.raises(fe.ServiceXException) as e:
        ds = fe.ServiceX('localds://mc16_tev:13', servicex_adaptor=mock_servicex_adaptor,
                     minio_adaptor=mock_minio_adaptor, max_workers=50)
        await ds.get_data_awkward_async('(valid qastle string)')

    assert str(e).find('400') >= 0


@pytest.mark.skip
@pytest.mark.asyncio
@pytest.mark.parametrize("n_ds, n_query", [(1, 4), (4, 1), (1, 100), (100, 1), (4, 4), (20, 20)])
async def test_nqueries_on_n_ds(n_ds: int, n_query: int, good_transform_request, files_in_minio):
    'Run some number of queries on some number of datasets'
    def create_ds_query(index: int):
        ds = fe.ServiceX(f'localds://mc16_tev:13_{index}')
        return [ds.get_data_rootfiles_async(f'(valid qastle string {i})') for i in range(n_query)]

    all_results = [item for i in range(n_ds) for item in create_ds_query(i)]
    all_wait = await asyncio.gather(*all_results)

    # They are different queries, so they should come down in different files.
    count = 0
    s = set()
    for r in all_wait:
        for f in r:
            assert f.exists()
            s.add(str(f))
            count += 1

    assert len(s) == count


@pytest.mark.asyncio
async def test_download_to_temp_dir(mocker):
    'Download to a specified storage directory'
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    tmp = os.path.join(tempfile.gettempdir(), 'my_test_dir')
    if os.path.exists(tmp):
        shutil.rmtree(tmp)
    os.mkdir(tmp)

    ds = fe.ServiceX('localds://dude-is-funny', servicex_adaptor=mock_servicex_adaptor,
                     minio_adaptor=mock_minio_adaptor, storage_directory=tmp)
    r = await ds.get_data_rootfiles_async('(valid qastle string')

    assert isinstance(r, List)
    assert len(r) == 1
    assert str(r[0]).startswith(tmp)


@pytest.mark.skip
@pytest.mark.asyncio
async def test_download_to_lambda_dir(mocker):
    'Download to files using a file name function callback'
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    tmp = os.path.join(tempfile.gettempdir(), 'my_test_dir')
    if os.path.exists(tmp):
        shutil.rmtree(tmp)
    os.mkdir(tmp)

    ds = fe.ServiceX('localds://dude-is-funny', servicex_adaptor=mock_servicex_adaptor,
                     minio_adaptor=mock_minio_adaptor,
                     file_name_func=lambda rid, obj_name: Path(f'{tmp}\\{clean_fname(obj_name)}'))
    r = await ds.get_data_rootfiles_async('(valid qastle string')

    assert isinstance(r, List)
    assert len(r) == 1
    assert str(r[0]).startswith(tmp)


@pytest.mark.asyncio
async def test_download_bad_params_filerename(mocker):
    'Specify both a storage directory and a filename rename func - illegal'
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    tmp = os.path.join(tempfile.gettempdir(), 'my_test_dir')
    if os.path.exists(tmp):
        shutil.rmtree(tmp)
    os.mkdir(tmp)
    with pytest.raises(Exception) as e:
        fe.ServiceX('http://one-ds', servicex_adaptor=mock_servicex_adaptor, minio_adaptor=mock_minio_adaptor,
                    storage_directory=tmp,
                    file_name_func=lambda rid, obj_name: Path(f'{tmp}\\{clean_fname(obj_name)}'))
    assert "only specify" in str(e.value)


def test_callback_good(mocker):
    'Simple run with expected results, but with the non-async version'
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

    ds = fe.ServiceX('http://one-ds', servicex_adaptor=mock_servicex_adaptor, minio_adaptor=mock_minio_adaptor,
                     status_callback_factory=lambda ds: check_in)
    ds.get_data_rootfiles('(valid qastle string)')

    assert f_total == 1
    assert f_processed == 1
    assert f_downloaded == 1
    assert f_failed == 0


@pytest.mark.skip
def test_failed_iteration(mocker):
    '''
    ServiceX fails one of its files:
    There are times service x returns a few number of files total for one query, but then resumes having a good number'
    '''
    mock_transform_status = mocker.Mock(side_effect=[(0, 1, 1)])
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456", mock_transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    f_total = []
    f_processed = []
    f_downloaded = []
    f_failed = []

    def check_in(total: Optional[int], processed: int, downloaded: int, failed: int):
        nonlocal f_total, f_processed, f_downloaded, f_failed
        f_total.append(total)
        f_processed.append(processed)
        f_downloaded.append(downloaded)
        f_failed.append(failed)

    with pytest.raises(fe.ServiceXException) as e:
        ds = fe.ServiceX('http://one-ds', servicex_adaptor=mock_servicex_adaptor, minio_adaptor=mock_minio_adaptor,
                         status_callback_factory=lambda ds: check_in)
        ds.get_data_rootfiles('(valid qastle string)')

    assert len(f_total) == 2
    assert all(i == 2 for i in f_total)
    assert all(i == 1 for i in f_failed)
    assert all(i <= 1 for i in f_downloaded)
    assert "failed to transform" in str(e.value)


@pytest.mark.skip
@pytest.mark.asyncio
async def test_resume_download_missing_files(servicex_state_machine, short_status_poll_time):
    '''
    We get a status error message, and then we can re-download them.

    1. Request the transform
    1. Get the status - but that fails the second time
    1. This causes the download to bomb.
    1. Re-request the download, and then discover it is done.
    '''

    servicex_state_machine['reset']()
    servicex_state_machine['add_status_step'](processed=1, remaining=1, failed=0)
    servicex_state_machine['add_status_fail'](fe.ServiceXException('Lost the internet'))

    ds = fe.ServiceX('http://one-ds')
    with pytest.raises(fe.ServiceXException):
        # Will fail with one file downloaded.
        await ds.get_data_rootfiles_async('(valid qastle string)')

    servicex_state_machine['reset'](keep_request_id=True)
    servicex_state_machine['add_status_step'](processed=2, remaining=0, failed=0)

    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert len(r) == 2
    servicex_state_machine['patch_submit_query'].assert_called_once()


@pytest.mark.skip
@pytest.mark.asyncio
async def test_servicex_gone_when_redownload_request(servicex_state_machine, short_status_poll_time):
    '''
    We call to get a transform, get one of 2 files, then get an error.
    We try again, and this time servicex has been restarted, so it knows nothing about our request
    We have to re-request the transform and start from scratch.
    '''

    servicex_state_machine['reset']()
    servicex_state_machine['add_status_step'](processed=1, remaining=1, failed=0)
    servicex_state_machine['add_status_fail'](fe.ServiceXException('Lost the internet'))

    ds = fe.ServiceX('http://one-ds')

    with pytest.raises(Exception):
        # Will fail with one file downloaded.
        await ds.get_data_rootfiles_async('(valid qastle string)')

    # Reset to work with a new query
    servicex_state_machine['reset']()
    servicex_state_machine['add_status_step'](processed=2, remaining=0, failed=0)

    # New instance of ServiceX now, and it is ready to do everything.
    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert len(r) == 2
    servicex_state_machine['patch_submit_query'].call_count == 2, 'Request for a transform should have been called twice'


@pytest.mark.skip
@pytest.mark.asyncio
async def test_servicex_transformer_failure_reload(servicex_state_machine, short_status_poll_time):
    '''
    We call to get a transform, and the 1 file fails (gets marked as skip).
    We then call again, and it works, and we get back the files we want.
    '''

    servicex_state_machine['reset']()
    servicex_state_machine['add_status_step'](processed=1, remaining=0, failed=1)
    servicex_state_machine['add_status_fail'](fe.ServiceXException('Lost the internet'))

    ds = fe.ServiceX('http://one-ds')

    with pytest.raises(Exception):
        # Will fail with one skipped file.
        await ds.get_data_rootfiles_async('(valid qastle string)')

    # Setup for a good query.
    servicex_state_machine['reset']()
    servicex_state_machine['add_status_step'](processed=2, remaining=0, failed=0)

    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert len(r) == 2
    servicex_state_machine['patch_submit_query'].call_count == 2, 'Request for a transform should have been called twice'


@pytest.mark.asyncio
async def test_download_cached_nonet(mocker):
    '''
    Check that we do not use the network if we have already cached a file.
        - the transform is requested only initally
        - the status calls are not made more than for the first time
        - the calls to minio are only made the first time (the list_objects, for example)
    '''
    mock_transform_status = mocker.Mock(side_effect=[(0, 2, 0)])
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456", mock_transform_status)
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry'])

    async def do_query():
        ds = fe.ServiceX('localds://dude-is-funny', servicex_adaptor=mock_servicex_adaptor,
                         minio_adaptor=mock_minio_adaptor)
        return await ds.get_data_rootfiles_async('(valid qastle string')

    # Call 1
    await do_query()

    # Call 2
    await do_query()

    # Check the the number of times we called for a transform is good.
    assert len(mock_servicex_adaptor.query.mock_calls) == 1

    # Check that we made only one status call.
    assert len(mock_servicex_adaptor.transform_status.mock_calls) == 1

    # Check that we only called to see how many objects there were in minio once.
    # patch_info['list_files'].assert_called_once()


@pytest.mark.asyncio
async def test_download_cached_awkward(mocker, good_awkward_file_data):
    'Run two right after each other - they should return the same data in memory'
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry', 'two_minio_entry'])

    async def do_query():
        ds = fe.ServiceX('localds://dude-is-funny', servicex_adaptor=mock_servicex_adaptor,
                         minio_adaptor=mock_minio_adaptor)
        return await ds.get_data_awkward_async('(valid qastle string')

    a1 = await do_query()
    a2 = await do_query()
    assert a1 is a2


@pytest.mark.asyncio
async def test_download_cache_qastle_norm(mocker, good_awkward_file_data):
    'Run two right after each other - they should return the same data in memory'
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    async def do_query(q: str):
        ds = fe.ServiceX('localds://dude-is-funny', servicex_adaptor=mock_servicex_adaptor,
                         minio_adaptor=mock_minio_adaptor)
        return await ds.get_data_awkward_async(q)

    a1 = await do_query('(lambda (list a0 a1) (+ a0 a1))')
    a2 = await do_query('(lambda (list b0 b1) (+ b0 b1))')
    assert a1 is a2


@pytest.mark.asyncio
async def test_simultaneous_query_not_requeued(mocker, good_awkward_file_data):
    'Run two at once - they should not both generate queires as they are identical'
    mock_servicex_adaptor = MockServiceXAdaptor(mocker, "123-456")
    mock_minio_adaptor = MockMinioAdaptor(mocker, files=['one_minio_entry'])

    async def do_query():
        ds = fe.ServiceX('localds://dude-is-funny', servicex_adaptor=mock_servicex_adaptor,
                         minio_adaptor=mock_minio_adaptor)
        return await ds.get_data_awkward_async('(valid qastle string')

    a1, a2 = await asyncio.gather(*[do_query(), do_query()])  # type: ignore
    assert a1 is a2
