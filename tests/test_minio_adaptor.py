from pathlib import Path

import minio
from minio.error import ResponseError
import pytest

from servicex import (
    MinioAdaptor,
    ServiceXException,
)
from servicex.minio_adaptor import MinioAdaptorFactory, find_new_bucket_files


def make_minio_file(fname):
    from unittest import mock
    r = mock.MagicMock()
    r.object_name = fname
    return r


@pytest.fixture
def good_minio_client(mocker):

    minio_client = mocker.MagicMock(spec=minio.Minio)
    minio_client.list_objects.return_value = \
        [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas'
                         ':dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052'
                         '._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]

    mocker.patch('servicex.minio_adaptor.Minio', return_value=minio_client)

    p_rename = mocker.patch('servicex.minio_adaptor.Path.rename', mocker.MagicMock())

    mocker.patch('servicex.minio_adaptor.Path.mkdir', mocker.MagicMock())

    return p_rename, minio_client


@pytest.fixture
def bad_minio_client(mocker):
    minio_client = mocker.MagicMock(spec=minio.Minio)
    minio_client.list_objects.return_value = \
        [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas:dq2'
                         ':atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.05630052._000001'
                         '.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1.part.minio')]
    minio_client.fget_object.side_effect = Exception('this copy really failed')

    mocker.patch('servicex.minio_adaptor.Minio', return_value=minio_client)

    p_rename = mocker.patch('servicex.minio_adaptor.Path.rename', mocker.MagicMock())
    mocker.patch('servicex.minio_adaptor.Path.mkdir', mocker.MagicMock())

    return p_rename, minio_client


@pytest.fixture
def bad_then_good_minio_listing(mocker):
    response1 = mocker.MagicMock()
    response1.data = '<xml></xml>'
    response2 = [make_minio_file('root:::dcache-atlas-xrootd-wan.desy.de:1094::pnfs:desy.de:atlas'
                                 ':dq2:atlaslocalgroupdisk:rucio:mc15_13TeV:8a:f1:DAOD_STDM3.'
                                 '05630052._000001.pool.root.198fbd841d0a28cb0d9dfa6340c890273-1'
                                 '.part.minio')]

    minio_client = mocker.MagicMock(spec=minio.Minio)
    minio_client.list_objects.side_effect = [ResponseError(response1, 'POST', 'Due'), response2]

    mocker.patch('servicex.minio_adaptor.Minio', return_value=minio_client)

    p_rename = mocker.patch('servicex.minio_adaptor.Path.rename', mocker.MagicMock())
    mocker.patch('servicex.minio_adaptor.Path.mkdir', mocker.MagicMock())

    return p_rename, minio_client


@pytest.mark.asyncio
async def test_download_good(good_minio_client):
    ma = MinioAdaptor('localhost:9000')

    final_path = Path('/tmp/output-file.dude')  # type: Path
    await ma.download_file('111-22-333-444', 'dude-where-is-my-lunch', final_path)

    p_rename, p_minio = good_minio_client

    # Make sure copy was called.
    p_minio.fget_object.assert_called_once()
    expected_path = Path('/tmp/output-file.dude.temp')
    p_minio.fget_object.assert_called_with('111-22-333-444', 'dude-where-is-my-lunch',
                                           str(expected_path))

    # Make sure the rename was done.
    p_rename.assert_called_with(final_path)


@pytest.mark.asyncio
async def test_download_bad(bad_minio_client):
    ma = MinioAdaptor('localhost:9000')

    final_path = Path('/tmp/output-file.dude')
    with pytest.raises(ServiceXException) as e:
        await ma.download_file('111-22-333-444', 'dude-where-is-my-lunch', final_path)

    assert "Failed to copy" in str(e.value)

    p_rename, p_minio = bad_minio_client
    p_minio.fget_object.assert_called_once()

    # Make sure the rename was done.
    p_rename.assert_not_called()


@pytest.mark.asyncio
async def test_download_already_there(mocker, good_minio_client):
    p_exists = mocker.patch('servicex.minio_adaptor.Path.exists', return_value=True)

    ma = MinioAdaptor('localhost:9000')

    final_path = Path('/tmp/output-file.dude')  # type: Path
    await ma.download_file('111-22-333-444', 'dude-where-is-my-lunch', final_path)

    p_rename, p_minio = good_minio_client

    # Make sure copy was called.
    p_minio.fget_object.assert_not_called()

    # Make sure the rename was done.
    p_rename.assert_not_called()

    # Make sure the exists worked.
    p_exists.assert_called_once()


def test_list_objects(good_minio_client):
    ma = MinioAdaptor('localhost:9000')

    f = ma.get_files('111-222-333-444')
    assert len(f) == 1


def test_list_objects_with_null(bad_then_good_minio_listing):
    '''Sometimes for reasons we do not understand we get back a response error from
    list_objects minio method'''
    ma = MinioAdaptor('localhost:9000')
    f = ma.get_files('111-222-333-444')
    assert len(f) == 1


@pytest.mark.asyncio
async def test_find_new_bucket_0_files(mocker):
    from .conftest import MockMinioAdaptor, as_async_seq
    ma = MockMinioAdaptor(mocker)
    r = [f async for f in find_new_bucket_files(ma,  # type: ignore
                                                '123-456', as_async_seq([(1, 0, 0), (0, 1, 0)]))]

    assert len(r) == 0


@pytest.mark.asyncio
async def test_find_new_bucket_1_files(mocker):
    from .conftest import MockMinioAdaptor, as_async_seq
    ma = MockMinioAdaptor(mocker, files=['one_two_three'])
    r = [f async for f in find_new_bucket_files(ma,  # type: ignore
                                                '123-456', as_async_seq([(1, 0, 0), (0, 1, 0)]))]

    assert len(r) == 1


def test_factory_no_inputs():
    f = MinioAdaptorFactory()
    with pytest.raises(Exception):
        # Should fail b.c. no way to figure out what to create!
        f.from_best()


def test_factor_always():
    a = MinioAdaptor('localhost:9000')
    f = MinioAdaptorFactory(always_return=a)
    assert f.from_best() is a


def test_factory_set_endpoint():
    from confuse import Configuration
    c = Configuration('bogus', 'bogus')
    c.clear()
    c['api_endpoint']['minio_endpoint'] = 'the-good-host.org:9000'
    c['api_endpoint']['minio_username'] = 'amazing'
    c['api_endpoint']['minio_password'] = 'forkingshirtballs'

    c['api_endpoint']['default_minio_username'] = 'badnews'
    c['api_endpoint']['default_minio_password'] = 'bears'

    m = MinioAdaptorFactory(c).from_best()
    assert m._endpoint == 'the-good-host.org:9000'
    assert m._access_key == "amazing"
    assert m._secretkey == "forkingshirtballs"


def test_factory_use_api_usernamepassword():
    from confuse import Configuration
    c = Configuration('bogus', 'bogus')
    c.clear()

    c['api_endpoint']['endpoint'] = 'http://my-left-foot.com:5000'
    c['api_endpoint']['username'] = 'thegoodplace'
    c['api_endpoint']['password'] = 'forkingshirtballs!'

    c['api_endpoint']['minio_endpoint'] = 'the-good-host.org:9000'

    m = MinioAdaptorFactory(c).from_best()
    assert m._endpoint == 'the-good-host.org:9000'
    assert m._access_key == "thegoodplace"
    assert m._secretkey == "forkingshirtballs!"


def test_factory_use_default_username_password():
    from confuse import Configuration
    c = Configuration('bogus', 'bogus')
    c.clear()

    c['api_endpoint']['minio_endpoint'] = 'the-good-host.org:9000'
    c['api_endpoint']['default_minio_username'] = 'thegoodplace'
    c['api_endpoint']['default_minio_password'] = 'forkingshirtballs!'

    m = MinioAdaptorFactory(c).from_best()
    assert m._endpoint == 'the-good-host.org:9000'
    assert m._access_key == "thegoodplace"
    assert m._secretkey == "forkingshirtballs!"


def test_factory_from_request():
    info = {
        'minio-access-key': 'miniouser',
        'minio-endpoint': 'minio.servicex.com:9000',
        'minio-secret-key': 'leftfoot1',
    }
    m = MinioAdaptorFactory().from_best(info)
    assert m._endpoint == 'minio.servicex.com:9000'
    assert m._access_key == "miniouser"
    assert m._secretkey == "leftfoot1"


def test_factory_request_missing():
    info = {}
    from confuse import Configuration
    c = Configuration('bogus', 'bogus')
    c.clear()

    c['api_endpoint']['minio_endpoint'] = 'the-good-host.org:9000'
    c['api_endpoint']['default_minio_username'] = 'thegoodplace'
    c['api_endpoint']['default_minio_password'] = 'forkingshirtballs!'

    m = MinioAdaptorFactory(c).from_best(info)
    assert m._endpoint == 'the-good-host.org:9000'
    assert m._access_key == "thegoodplace"
    assert m._secretkey == "forkingshirtballs!"
