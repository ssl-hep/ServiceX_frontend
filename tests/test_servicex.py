import asyncio
from json import dumps
import os
import queue
import re
import shutil
import tempfile
from typing import List, Optional, Any, Tuple
from unittest import mock
from unittest.mock import MagicMock
from pathlib import Path

from minio.error import ResponseError
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
    bad_transform_status,
    short_status_poll_time,
    bad_transform_request
)  # NOQA


# @pytest.fixture(scope="module")
# def reduce_wait_time():
#     import servicex.servicex as ssx
#     old_value = ssx.servicex_status_poll_time
#     ssx.servicex_status_poll_time = 0.01
#     yield None
#     ssx.servicex_status_poll_time = old_value


# def good_copy(a, b, c):
#     'Mock the fget_object from minio by copying out our test file'
#     shutil.copy('tests/sample_servicex_output.root', c)


# def make_minio_file(fname):
#     r = mock.MagicMock()
#     r.object_name = fname
#     return r


# @pytest.fixture()
# def files_back_1(mocker):
#     p_list = mocker.patch('minio.api.Minio.list_objects', return_value=[make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')])
#     p_fget = mocker.patch('minio.api.Minio.fget_object', side_effect=good_copy)
#     return p_fget, p_list


# @pytest.fixture()
# def files_back_1_fail_initally(mocker):
#     'Throw a response error - so the bucket does not not get created right away'
#     response = MagicMock()
#     response.data = '<xml></xml>'
#     mocker.patch('minio.api.Minio.list_objects', side_effect=[ResponseError(response, 'POST', 'Dude'), [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]])
#     mocker.patch('minio.api.Minio.fget_object', side_effect=good_copy)
#     return None


# @pytest.fixture()
# def files_back_2(mocker):
#     p_list = mocker.patch('minio.api.Minio.list_objects', return_value=[make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio'),
#                                                                         make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000002.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')])
#     p_fget = mocker.patch('minio.api.Minio.fget_object', side_effect=good_copy)
#     return p_fget, p_list


# @pytest.fixture()
# def files_back_4_order_1(mocker):
#     mocker.patch('minio.api.Minio.list_objects',
#                  return_value=[
#                      make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio'),
#                      make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000002.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio'),
#                      make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000003.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio'),
#                      make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000004.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')
#                  ])
#     mocker.patch('minio.api.Minio.fget_object', side_effect=good_copy)
#     return None


# @pytest.fixture()
# def files_back_4_order_2(mocker):
#     mocker.patch('minio.api.Minio.list_objects',
#                  return_value=[
#                      make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000003.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio'),
#                      make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000004.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio'),
#                      make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000002.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio'),
#                      make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')
#                  ])
#     mocker.patch('minio.api.Minio.fget_object', side_effect=good_copy)
#     return None


# @pytest.fixture()
# def files_back_2_one_at_a_time(mocker):
#     q = queue.Queue()
#     d = {}
#     f1 = make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')
#     f2 = make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000002.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')

#     def return_files(req_id):
#         if len(d) == 0:
#             q.put("get-file-list 0")
#             d['flag'] = True
#             return [f1]
#         else:
#             q.put("get-file-list 1")
#             return [f1, f2]

#     def copy_files(a, b, c):
#         q.put(f'copy-a-file {c}')
#         good_copy(a, b, c)

#     mocker.patch('minio.api.Minio.list_objects', side_effect=return_files)
#     mocker.patch('minio.api.Minio.fget_object', side_effect=copy_files)
#     return q


# class list_objects_callable(mock.MagicMock):
#     def __call__(self, *args, **kwargs):
#         assert len(args) == 1
#         req_id = args[0]
#         num_files_s = req_id
#         dash = req_id.find('_')
#         if dash > 0:
#             num_files_s = req_id[:dash]
#         num_files = int(num_files_s)

#         return [make_minio_file(f'root:::dcache-atlas-{req_id}-{i}') for i in range(0, num_files)]


# @pytest.fixture()
# def indexed_files_back(mocker):
#     'Use the request id formatting to figure out how many files to deliver back'
#     mocker.patch('minio.api.Minio.list_objects', new_callable=list_objects_callable)
#     mocker.patch('minio.api.Minio.fget_object', side_effect=good_copy)


# @pytest.fixture()
# def transform_status_fails_once_then_unknown(mocker):
#     '''
#     Return good request. Have a single file come back.
#     Then pretend we know nothing about the transform any longer.
#     This requires a bit of a state machine to keep straight.
#     '''
#     request_id_1 = "1234-4433-111-34-22-444"
#     request_id_2 = "1234-4433-111-34-22-555"

#     request_id = request_id_1

#     # For this we always accept the transform request.
#     def call_post():
#         nonlocal request_id
#         return ClientSessionMocker(dumps({"request_id": request_id}), 200)
#     post_patch = mocker.patch('aiohttp.ClientSession.post', side_effect=lambda _, json: call_post())

#     called_times = 0

#     def get_status(a):
#         nonlocal called_times, request_id_1, request_id_2, request_id
#         req_id = a.split("/")[-2]
#         try:
#             if req_id == request_id_1:
#                 if called_times == 0:
#                     # One file through
#                     return ClientSessionMocker(dumps({"files-remaining": "1", "files-processed": "1"}), 200)
#                 else:
#                     # ServiceX was restarted - we have no idea!!!
#                     request_id = request_id_2
#                     return ClientSessionMocker(dumps({"message": "Internal Server Error"}), 500)
#             elif req_id == request_id_2:
#                 return ClientSessionMocker(dumps({"files-remaining": "0", "files-processed": "2"}), 200)
#         finally:
#             called_times += 1

#     mocker.patch('aiohttp.ClientSession.get', side_effect=get_status)

#     def get_list_objects(req_id: str):
#         nonlocal request_id, request_id_1, request_id_2, called_times
#         if request_id == request_id_1:
#             return [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]
#         elif request_id == request_id_2:
#             return [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio'),
#                     make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000002.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]

#     mocker.patch('minio.api.Minio.list_objects', side_effect=get_list_objects)
#     mocker.patch('minio.api.Minio.fget_object', side_effect=good_copy)

#     return post_patch


# @pytest.fixture()
# def transform_fails_once_then_second_good(mocker):
#     '''
#     1. Return a good request
#     2. Return one good file and a second one is bad (skipped)
#     3. Return a good request (second request)
#     4. Return two good files.
#     '''
#     request_id_1 = "1234-4433-111-34-22-444"
#     request_id_2 = "1234-4433-111-34-22-555"

#     request_id = None

#     # For this we always accept the transform request.
#     def call_post():
#         nonlocal request_id, request_id_1, request_id_2
#         if request_id is None:
#             request_id = request_id_1
#         else:
#             request_id = request_id_2
#         return ClientSessionMocker(dumps({"request_id": request_id}), 200)

#     post_patch = mocker.patch('aiohttp.ClientSession.post', side_effect=lambda _, json: call_post())

#     called_times = 0

#     def get_status(a):
#         nonlocal request_id_1, request_id_2
#         req_id = a.split("/")[-2]
#         if req_id == request_id_1:
#             return ClientSessionMocker(dumps({"files-remaining": "0", "files-skipped": 1, "files-processed": "1"}), 200)
#         elif req_id == request_id_2:
#             return ClientSessionMocker(dumps({"files-remaining": "0", "files-processed": "2"}), 200)

#     mocker.patch('aiohttp.ClientSession.get', side_effect=get_status)

#     def get_list_objects(req_id: str):
#         nonlocal request_id, request_id_1, request_id_2, called_times
#         if request_id == request_id_1:
#             return [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]
#         elif request_id == request_id_2:
#             return [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio'),
#                     make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000002.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]

#     mocker.patch('minio.api.Minio.list_objects', side_effect=get_list_objects)
#     mocker.patch('minio.api.Minio.fget_object', side_effect=good_copy)

#     return post_patch


# @pytest.fixture()
# def transform_status_fails_once_then_good(mocker):
#     '''
#     1. Return good request
#     2. Return a single file
#     3. Return a bad status message
#     4. Return a good status message

#     We have to manage the list_objects in parallel since its behavior is linked here.
#     '''
#     # For this we always accept the transform request.
#     def call_post():
#         return ClientSessionMocker(dumps({"request_id": "1234-4433-111-34-22-444"}), 200)
#     posted_patch = mocker.patch('aiohttp.ClientSession.post', side_effect=lambda _, json: call_post())

#     called_times = 0

#     def get_status(a):
#         nonlocal called_times
#         try:
#             if called_times == 0:
#                 # One file through
#                 return ClientSessionMocker(dumps({"files-remaining": "1", "files-processed": "1"}), 200)
#             elif called_times == 1:
#                 # We return a bad status message
#                 return ClientSessionMocker(dumps({"message": "No such request id"}), 400)
#             elif called_times == 2:
#                 # Ok - this is after they have re-called. So now we are in good shape!
#                 return ClientSessionMocker(dumps({"files-remaining": "0", "files-processed": "2"}), 200)
#         finally:
#             called_times += 1

#     mocker.patch('aiohttp.ClientSession.get', side_effect=get_status)

#     def get_list_objects(a):
#         nonlocal called_times
#         # Note that called_times has been incremented so called_times == 1, is like state 0 in get_status
#         if called_times == 1:
#             return [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]
#         elif called_times == 2:
#             raise Exception("Should not be calling list_objects after an error from status")
#         elif called_times == 3:
#             return [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio'),
#                     make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000002.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]

#     mocker.patch('minio.api.Minio.list_objects', side_effect=get_list_objects)
#     mocker.patch('minio.api.Minio.fget_object', side_effect=good_copy)

#     return posted_patch

# # TODO: Move this into the common area so we don't repeat this code
# #       Will have to solve failures in the test_utils fellow, however.
# @pytest.fixture()
# def good_transform_request(mocker):
#     '''
#     Setup to run a good transform request that returns a single file.
#     '''
#     called_json_data = {}
#     times_called = 0

#     def call_post(data_dict_to_save: dict, json=None):
#         nonlocal times_called
#         times_called += 1
#         data_dict_to_save.update(json)
#         data_dict_to_save.update(dict(called=times_called))
#         return ClientSessionMocker(dumps({"request_id": "1234-4433-111-34-22-444"}), 200)
#     mocker.patch('aiohttp.ClientSession.post', side_effect=lambda _, json: call_post(called_json_data, json=json))

#     r2 = ClientSessionMocker(dumps({"files-remaining": "0", "files-processed": "1"}), 200)
#     status_mock = mocker.patch('aiohttp.ClientSession.get', return_value=r2)

#     return called_json_data, status_mock


# @pytest.fixture()
# def good_transform_bad_status(mocker):
#     '''
#     Setup to run a good transform request that returns a single file.
#     '''
#     called_json_data = {}

#     def call_post(data_dict_to_save: dict, json=None):
#         data_dict_to_save.update(json)
#         return ClientSessionMocker(dumps({"request_id": "1234-4433-111-34-22-444"}), 200)
#     mocker.patch('aiohttp.ClientSession.post', side_effect=lambda _, json: call_post(called_json_data, json=json))

#     r2 = ClientSessionMocker(dumps({"message": "not doing this over and over"}), 400)
#     mocker.patch('aiohttp.ClientSession.get', return_value=r2)

#     return called_json_data


# @pytest.fixture()
# def bad_transform(mocker):
#     '''
#     Setup to run a good transform request that returns a single file.
#     '''
#     called_json_data = {}

#     def call_post(data_dict_to_save: dict, json=None):
#         data_dict_to_save.update(json)
#         return ClientSessionMocker(dumps({"request_id": "1234-4433-111-34-22-444"}), 200)
#     mocker.patch('aiohttp.ClientSession.post', side_effect=lambda _, json: call_post(called_json_data, json=json))

#     r2 = ClientSessionMocker(dumps({"files-remaining": "0", "files-processed": "1", "files-skipped": 1}), 200)
#     mocker.patch('aiohttp.ClientSession.get', return_value=r2)

#     return called_json_data


# @pytest.fixture()
# def bad_transform_request(mocker):
#     '''
#     Fail when we return!
#     '''
#     r1 = ClientSessionMocker(dumps({"message": "Things Just Went Badly"}), 400)
#     mocker.patch('aiohttp.ClientSession.post', return_value=r1)

#     return None


# @pytest.fixture()
# def good_transform_request_delayed_finish(mocker):
#     '''
#     Setup to run a good transform request that returns a single file.
#     '''
#     r1 = ClientSessionMocker(dumps({"request_id": "1234-4433-111-34-22-444"}), 200)
#     mocker.patch('aiohttp.ClientSession.post', return_value=r1)

#     f1 = ClientSessionMocker(dumps({"files-remaining": "1", "files-processed": "1"}), 200)
#     f2 = ClientSessionMocker(dumps({"files-remaining": "0", "files-processed": "2"}), 200)
#     mocker.patch('aiohttp.ClientSession.get', side_effect=[f1, f2])

#     return None


# @pytest.fixture()
# def good_transform_jittery_file_totals_3(mocker):
#     '''
#     Setup to run a good transform request that returns a single file.
#     '''
#     r1 = ClientSessionMocker(dumps({"request_id": "1234-4433-111-34-22-444"}), 200)
#     mocker.patch('aiohttp.ClientSession.post', return_value=r1)

#     f1 = ClientSessionMocker(dumps({"files-remaining": "3", "files-processed": "0"}), 200)
#     f2 = ClientSessionMocker(dumps({"files-remaining": "2", "files-processed": "0"}), 200)
#     f3 = ClientSessionMocker(dumps({"files-remaining": "1", "files-processed": "2"}), 200)
#     f4 = ClientSessionMocker(dumps({"files-remaining": "0", "files-processed": "3"}), 200)
#     mocker.patch('aiohttp.ClientSession.get', side_effect=[f1, f2, f3, f4])

#     return None


# @pytest.fixture()
# def good_requests_indexed(mocker):
#     '''
#     Parse the dataset to figure out what is to be done
#     '''
#     def call_post(_, json=None):
#         dataset = json['did']
#         info = re.search('^[a-z]+_([0-9]+)_([0-9]+)$', dataset)
#         query_number = info[1]
#         n_files = info[2]
#         return ClientSessionMocker(dumps({"request_id": f'{n_files}_{query_number}'}), 200)
#     mocker.patch('aiohttp.ClientSession.post', side_effect=call_post)

#     # Now get back the returns, which are just going to always be the same.
#     g1 = ClientSessionMocker(dumps({"files-remaining": "0", "files-processed": "4"}), 200)
#     mocker.patch('aiohttp.ClientSession.get', return_value=g1)


def clean_fname(fname: str):
    'No matter the string given, make it an acceptable filename'
    return fname.replace('*', '_') \
                .replace(';', '_') \
                .replace(':', '_')


def test_create_with_dataset():
    'Default should be possible'
    ds = fe.ServiceX('localds://mc16_tev:13')
    assert ds.dataset == 'localds://mc16_tev:13'
    assert ds.endpoint == 'http://localhost:5000/servicex'


@pytest.mark.asyncio
async def test_good_run_root_files(good_transform_request, files_in_minio):
    'Get a root file with a single file'
    ds = fe.ServiceX('localds://mc16_tev:13')
    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert isinstance(r, list)
    assert len(r) == 1
    assert r[0].exists()
    assert good_transform_request.call_args[0][2]['result-format'] == 'root-file'


@pytest.mark.asyncio
async def test_skipped_file(good_transform_request, files_in_minio):
    '''
    ServiceX should throw if a file is marked as "skipped".
    '''
    files_in_minio(2, as_failed=1)

    with pytest.raises(fe.ServiceX_Exception) as e:
        ds = fe.ServiceX('http://one-ds')
        ds.get_data_rootfiles('(valid qastle string)')

    assert "failed to transform" in str(e.value)


def test_good_run_root_files_no_async(good_transform_request, files_in_minio):
    'Make sure the non-async version works'
    ds = fe.ServiceX('localds://mc16_tev:13')
    r = ds.get_data_rootfiles('(valid qastle string)')
    assert isinstance(r, list)
    assert len(r) == 1
    assert r[0].exists()


@pytest.mark.asyncio
async def test_good_run_root_files_pause(good_transform_request, files_in_minio, short_status_poll_time):
    'Get a root file with a single file'
    files_in_minio(1, status_calls_before_complete=1)
    ds = fe.ServiceX('localds://mc16_tev:13')
    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert isinstance(r, list)
    assert len(r) == 1
    assert r[0].exists()
    assert good_transform_request.call_args[0][2]['result-format'] == 'root-file'


@pytest.mark.asyncio
async def test_good_run_files_back_4_order_1(good_transform_request, files_in_minio):
    'Simple run with expected results'
    files_in_minio(4, reverse=False)
    ds = fe.ServiceX('localds://mc16_tev:13')
    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert isinstance(r, list)
    assert len(r) == 4
    s_r = sorted([f.name for f in r])
    assert [f.name for f in r] == s_r


@pytest.mark.asyncio
async def test_good_run_files_back_4_order_2(good_transform_request, files_in_minio):
    'Simple run with expected results'
    files_in_minio(4, reverse=True)
    ds = fe.ServiceX('localds://mc16_tev:13')
    r = await ds.get_data_rootfiles_async('(valid qastle string)')
    assert isinstance(r, list)
    assert len(r) == 4
    s_r = sorted([f.name for f in r])
    assert [f.name for f in r] == s_r


@pytest.mark.asyncio
async def test_good_download_files_parquet(good_transform_request, files_in_minio):
    'Simple run with expected results'
    ds = fe.ServiceX('localds://mc16_tev:13')
    r = await ds.get_data_parquet_async('(valid qastle string)')
    assert isinstance(r, list)
    assert len(r) == 1
    assert r[0].exists()
    assert good_transform_request.call_args[0][2]['result-format'] == 'parquet'


@pytest.mark.asyncio
async def test_good_run_single_ds_1file_pandas(good_transform_request, files_in_minio, good_pandas_file_data):
    'Simple run with expected results'
    ds = fe.ServiceX('localds://mc16_tev:13')
    r = await ds.get_data_pandas_df_async('(valid qastle string)')
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 6


@pytest.mark.asyncio
async def test_good_run_single_ds_1file_awkward(good_transform_request, files_in_minio, good_awkward_file_data):
    'Simple run with expected results'
    ds = fe.ServiceX('localds://mc16_tev:13')
    r = await ds.get_data_awkward_async('(valid qastle string)')
    assert isinstance(r, dict)
    assert len(r) == 1
    assert b'JetPt' in r
    assert len(r[b'JetPt']) == 6


@pytest.mark.asyncio
async def test_good_run_single_ds_2file_pandas(good_transform_request, files_in_minio, good_pandas_file_data):
    'Simple run with expected results'
    files_in_minio(2)
    ds = fe.ServiceX('localds://mc16_tev:13')
    r = await ds.get_data_pandas_df_async('(valid qastle string)')
    assert isinstance(r, pd.DataFrame)
    assert len(r) == 6 * 2


@pytest.mark.asyncio
async def test_good_run_single_ds_2file_awkward(good_transform_request, files_in_minio, good_awkward_file_data):
    'Simple run with expected results'
    files_in_minio(2)
    ds = fe.ServiceX('localds://mc16_tev:13')
    r = await ds.get_data_awkward_async('(valid qastle string)')
    assert isinstance(r, dict)
    assert len(r) == 1
    assert b'JetPt' in r
    assert len(r[b'JetPt']) == 6 * 2


@pytest.mark.asyncio
async def test_status_exception(good_transform_request, bad_transform_status):
    'Make sure status error - like transform not found - is reported all the way to the top'
    ds = fe.ServiceX('localds://mc16_tev:13')
    with pytest.raises(fe.ServiceX_Exception) as e:
        await ds.get_data_awkward_async('(valid qastle string)')
    assert "attempt" in str(e.value)


@pytest.mark.asyncio
async def test_image_spec(good_transform_request, files_in_minio, good_awkward_file_data):
    ds = fe.ServiceX('localds://mc16_tev:13', image='fork-it-over:latest')
    await ds.get_data_awkward_async('(valid qastle string)')

    called = good_transform_request.call_args[0][2]
    assert called['image'] == 'fork-it-over:latest'


@pytest.mark.asyncio
async def test_max_workers_spec(good_transform_request, files_in_minio, good_awkward_file_data):
    ds = fe.ServiceX('localds://mc16_tev:13', max_workers=50)
    await ds.get_data_awkward_async('(valid qastle string)')

    called = good_transform_request.call_args[0][2]
    assert called['workers'] == '50'


@pytest.mark.asyncio
async def test_servicex_rejects_transform_request(bad_transform_request):
    'Simple run bomb during transform query'
    with pytest.raises(fe.ServiceX_Exception) as e:
        ds = fe.ServiceX('localds://mc16_tev:13', max_workers=50)
        await ds.get_data_awkward_async('(valid qastle string)')

    assert str(e).find('400') >= 0


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
async def test_download_to_temp_dir(good_transform_request, files_in_minio):
    'Download to a specified storage directory'
    tmp = os.path.join(tempfile.gettempdir(), 'my_test_dir')
    if os.path.exists(tmp):
        shutil.rmtree(tmp)
    os.mkdir(tmp)

    ds = fe.ServiceX('localds://dude-is-funny', storage_directory=tmp)
    r = await ds.get_data_rootfiles_async('(valid qastle string')

    assert isinstance(r, List)
    assert len(r) == 1
    assert r[0].exists()
    assert str(r[0]).startswith(tmp)


@pytest.mark.asyncio
async def test_download_to_lambda_dir(good_transform_request, files_in_minio):
    'Download to files using a file name function callback'
    tmp = os.path.join(tempfile.gettempdir(), 'my_test_dir')
    if os.path.exists(tmp):
        shutil.rmtree(tmp)
    os.mkdir(tmp)

    ds = fe.ServiceX('localds://dude-is-funny', file_name_func=lambda rid, obj_name: Path(f'{tmp}\\{clean_fname(obj_name)}'))
    r = await ds.get_data_rootfiles_async('(valid qastle string')

    assert isinstance(r, List)
    assert len(r) == 1
    assert r[0].exists()
    assert str(r[0]).startswith(tmp)


@pytest.mark.asyncio
async def test_download_bad_params_filerename(good_transform_request, files_in_minio):
    'Specify both a storage directory and a filename rename func - illegal'
    tmp = os.path.join(tempfile.gettempdir(), 'my_test_dir')
    if os.path.exists(tmp):
        shutil.rmtree(tmp)
    os.mkdir(tmp)
    with pytest.raises(Exception) as e:
        fe.ServiceX('http://one-ds', storage_directory=tmp, file_name_func=lambda rid, obj_name: Path(f'{tmp}\\{clean_fname(obj_name)}'))
    assert "only specify" in str(e.value)


def test_callback_good(good_transform_request, files_in_minio):
    'Simple run with expected results, but with the non-async version'
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

    ds = fe.ServiceX('http://one-ds', status_callback=check_in)
    ds.get_data_rootfiles('(valid qastle string)')

    assert f_total == 1
    assert f_processed == 1
    assert f_downloaded == 1
    assert f_failed == 0


def test_failed_iteration(good_transform_request, files_in_minio):
    '''
    ServiceX fails one of its files:
    There are times service x returns a few number of files total for one query, but then resumes having a good number'
    '''
    files_in_minio(2, as_failed=1)

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

    with pytest.raises(fe.ServiceX_Exception) as e:
        ds = fe.ServiceX('http://one-ds', status_callback=check_in)
        ds.get_data_rootfiles('(valid qastle string)')

    assert len(f_total) == 1
    assert all(i == 2 for i in f_total)
    assert all(i == 1 for i in f_failed)
    assert "failed to transform" in str(e.value)


@pytest.mark.skip
@pytest.mark.asyncio
async def test_resume_download_missing_files(transform_status_fails_once_then_good, reduce_wait_time):
    '''
    We get a status error message, and then we can re-download them.

    1. Request the transform
    1. Get the status - but that fails the second time
    1. This causes the download to bomb.
    1. Re-request the download, and then discover it is done.
    '''

    with pytest.raises(Exception):
        # Will fail with one file downloaded.
        await fe.get_data_async('(valid qastle string)', 'one_ds', data_type='root-file')

    r = await fe.get_data_async('(valid qastle string)', 'one_ds', data_type='root-file')
    assert len(r) == 2
    transform_status_fails_once_then_good.assert_called_once()


@pytest.mark.skip
@pytest.mark.asyncio
async def test_servicex_gone_when_redownload_request(transform_status_fails_once_then_unknown, reduce_wait_time):
    '''
    We call to get a transform, get one of 2 files, then get an error.
    We try again, and this time servicex has been restarted, so it knows nothing about our request
    We have to re-request the transform and start from scratch.
    '''

    with pytest.raises(Exception):
        # Will fail with one file downloaded.
        await fe.get_data_async('(valid qastle string)', 'one_ds', data_type='root-file')

    # New instance of ServiceX now, and it is ready to do everything.
    r = await fe.get_data_async('(valid qastle string)', 'one_ds', data_type='root-file')
    assert len(r) == 2
    assert transform_status_fails_once_then_unknown.call_count == 2, 'Request for a transform should have been called twice'


@pytest.mark.skip
@pytest.mark.asyncio
async def test_servicex_transformer_failure_reload(transform_fails_once_then_second_good, reduce_wait_time):
    '''
    We call to get a transform, and the 1 file fails (gets marked as skip).
    We then call again, and it works, and we get back the files we want.
    '''

    with pytest.raises(Exception):
        # Will fail with one file downloaded.
        await fe.get_data_async('(valid qastle string)', 'one_ds', data_type='root-file')

    r = await fe.get_data_async('(valid qastle string)', 'one_ds', data_type='root-file')
    assert len(r) == 2
    assert transform_fails_once_then_second_good.call_count == 2, 'Request for a transform should have been called twice'


@pytest.mark.asyncio
@pytest.mark.parametrize("n_files", [1, 2])
async def test_download_cached_nonet(good_transform_request, files_in_minio, n_files: int):
    '''
    Check that we do not use the network if we have already cached a file.
        - the transform is requested only initally
        - the status calls are not made more than for the first time
        - the calls to minio are only made the first time (the list_objects, for example)
    '''
    files_in_minio(n_files)

    async def do_query():
        ds = fe.ServiceX('localds://dude-is-funny')
        return await ds.get_data_rootfiles_async('(valid qastle string')

    # Call 1
    await do_query()

    # Call 2
    await do_query()

    # Check the the number of times we called for a transform is good.
    good_transform_request.assert_called_once()

    # Check that we made only one status call.
    patch_info = files_in_minio(n_files)
    patch_info['get_transform_status'].assert_called_once()

    # Check that we only called to see how many objects there were in minio once.
    patch_info['list_files'].assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("n_files", [1, 2])
async def test_download_cached_awkward(good_transform_request, files_in_minio, good_awkward_file_data, n_files: int):
    'Run two right after each other - they should return the same data in memory'
    async def do_query():
        ds = fe.ServiceX('localds://dude-is-funny')
        return await ds.get_data_awkward_async('(valid qastle string')

    a1 = await do_query()
    a2 = await do_query()
    assert a1 is a2


@pytest.mark.asyncio
async def test_simultaneous_query_not_requeued(good_transform_request, files_in_minio, good_awkward_file_data):
    'Run two at once - they should not both generate queires as they are identical'
    async def do_query():
        ds = fe.ServiceX('localds://dude-is-funny')
        return await ds.get_data_awkward_async('(valid qastle string')

    a1, a2 = await asyncio.gather(*[do_query(), do_query()])  # type: ignore
    assert a1 is a2




# TODO:
# Other tests
#  Loose connection for a while after we submit the request
#  Don't have request to submit the request
#  Fail during download due to bad (temporary) connection.
