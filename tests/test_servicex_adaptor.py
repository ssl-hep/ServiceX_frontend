from json import dumps
from typing import Optional

import aiohttp
from aiohttp.client_exceptions import ContentTypeError
import pytest

from servicex import (
    ServiceXAdaptor,
    ServiceXException,
    ServiceXFailedFileTransform,
    ServiceXUnknownRequestID,
)
from servicex.servicex_adaptor import (
    servicex_adaptor_factory,
    transform_status_stream,
    trap_servicex_failures,
)

from .utils_for_testing import ClientSessionMocker, as_async_seq, short_status_poll_time  # NOQA


@pytest.fixture
def servicex_status_request(mocker):
    '''
    Fixture that emulates the async python library get call when used with a
    status.

      - Does not check the incoming http address
      - Does not check the Returns a standard triple status from servicex
      - Does not check the headers
      - Call this to set:
            servicex_status_request(1, 2, 3)
            Sets remaining to 1, failed to 2, and processed to 3.
    '''
    files_remaining = None
    files_failed = None
    files_processed = 0

    def get_status(a, headers=None):
        r = {}

        def store(name: str, values: Optional[int]):
            nonlocal r
            if values is not None:
                r[name] = values
        store('files-remaining', files_remaining)
        store('files-skipped', files_failed)
        store('files-processed', files_processed)

        return ClientSessionMocker(dumps(r), 200)

    mocker.patch('aiohttp.ClientSession.get', side_effect=get_status)

    def set_it_up(remain: Optional[int], failed: Optional[int], processed: Optional[int]):
        nonlocal files_remaining, files_failed, files_processed
        files_remaining = remain
        files_failed = failed
        files_processed = processed

    return set_it_up


@pytest.fixture
def good_submit(mocker):
    client = mocker.MagicMock()
    r = ClientSessionMocker(dumps({'request_id': "111-222-333-444"}), 200)
    client.post = mocker.MagicMock(return_value=r)
    return client


@pytest.fixture
def good_submit_with_auth(mocker):
    client = mocker.MagicMock()
    r = ClientSessionMocker([
        dumps({'access_token': "jwt:access"}),
        dumps({'request_id': "111-222-333-444"})], [200, 200])
    client.post = mocker.MagicMock(return_value=r)
    return client


@pytest.fixture
def bad_submit(mocker):
    client = mocker.MagicMock()
    r = ClientSessionMocker(dumps({'message': "bad text"}), 400)
    client.post = lambda d, json, headers: r
    return client


@pytest.fixture
def bad_submit_html(mocker):
    '''
    Instead of returning json, it returns text/html.
    '''
    client = mocker.MagicMock(spec=aiohttp.ClientSession)

    class bad_return:
        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def __aenter__(self):
            return self

        async def json(self):
            raise Exception("ContentTypeError")

        def status(self):
            return 500

        async def text(self):
            return "html error content bogus world"

    client.post.return_value = bad_return()
    return client


@pytest.fixture
def servicex_status_unknown(mocker):
    r = ClientSessionMocker(dumps({'message': "unknown status"}), 500)
    mocker.patch('aiohttp.ClientSession.get', return_value=r)


@pytest.mark.asyncio
async def test_status_no_auth(servicex_status_request):
    servicex_status_request(None, 0, 10)
    sa = ServiceXAdaptor(endpoint='http://localhost:5000/sx')
    async with aiohttp.ClientSession() as client:
        r = await sa.get_transform_status(client, '123-123-123-444')
        assert len(r) == 3
        assert r[0] is None
        assert r[1] == 10
        assert r[2] == 0


@pytest.mark.asyncio
async def test_status_with_auth(mocker):
    client = mocker.MagicMock()
    client.post = mocker.Mock(return_value=ClientSessionMocker([
        dumps({'access_token': "jwt:access"})], [200]))

    client.get = mocker.Mock(return_value=ClientSessionMocker([
        dumps({'files-remaining': 1,
               'files-skipped': 0,
               'files-processed': 0})], [200]))

    sa = ServiceXAdaptor(endpoint='http://localhost:5000/sx',
                         refresh_token='jwt:refresh')

    await sa.get_transform_status(client, '123-123-123-444')
    print(client.post.mock_calls)
    client.post.assert_called_with("http://localhost:5000/sx/token/refresh", json=None,
                                   headers={'Authorization': 'Bearer jwt:refresh'})

    client.get.assert_called_with("http://localhost:5000/sx/servicex/transformation/123-123-123-444/status",
                                  headers={'Authorization': 'Bearer jwt:access'})


@pytest.mark.asyncio
async def test_status_unknown_request(servicex_status_unknown):

    sa = ServiceXAdaptor('http://localhost:5000/sx')
    with pytest.raises(ServiceXUnknownRequestID) as e:
        async with aiohttp.ClientSession() as client:
            await sa.get_transform_status(client, '123-123-123-444')

    assert 'transform status' in str(e.value)


def version_mock(mocker, spec):
    import sys
    if sys.version_info[1] < 8:
        from asyncmock import AsyncMock  # type: ignore
        return AsyncMock(spec=spec)
    else:
        return mocker.MagicMock(spec=spec)


@pytest.mark.asyncio
async def test_status_stream_simple_sequence(mocker):
    adaptor = version_mock(mocker, spec=ServiceXAdaptor)
    adaptor.get_transform_status.configure_mock(return_value=(0, 1, 1))

    async with aiohttp.ClientSession() as client:
        v = [a async for a in transform_status_stream(adaptor, client, '123-455')]

    assert len(v) == 1
    assert v[0] == (0, 1, 1)


@pytest.mark.asyncio
async def test_status_stream_simple_2sequence(short_status_poll_time, mocker):
    adaptor = version_mock(mocker, spec=ServiceXAdaptor)
    adaptor.get_transform_status.configure_mock(side_effect=[(1, 1, 1), (0, 1, 1)])

    async with aiohttp.ClientSession() as client:
        v = [a async for a in transform_status_stream(adaptor, client, '123-455')]

    assert len(v) == 2
    assert v[0] == (1, 1, 1)
    assert v[1] == (0, 1, 1)


@pytest.mark.asyncio
async def test_watch_no_fail(short_status_poll_time, mocker):
    v = [a async for a in trap_servicex_failures(as_async_seq([(1, 0, 0), (0, 1, 0)]))]

    assert len(v) == 2
    assert v[0] == (1, 0, 0)
    assert v[1] == (0, 1, 0)


@pytest.mark.asyncio
async def test_watch_fail(short_status_poll_time, mocker):
    v = []
    with pytest.raises(ServiceXFailedFileTransform) as e:
        async for a in trap_servicex_failures(as_async_seq([(1, 0, 0), (0, 0, 1)])):
            v.append(a)

    # Should force a failure as soon as it is detected.
    assert len(v) == 1
    assert 'failed to transform' in str(e.value)


@pytest.mark.asyncio
async def test_watch_fail_start(short_status_poll_time, mocker):
    v = []
    with pytest.raises(ServiceXFailedFileTransform) as e:
        async for a in trap_servicex_failures(as_async_seq([(2, 0, 0), (1, 0, 1), (0, 1, 1)])):
            v.append(a)

    assert len(v) == 1
    assert 'failed to transform' in str(e.value)


@pytest.mark.asyncio
async def test_submit_good_no_auth(good_submit):
    sa = ServiceXAdaptor(endpoint='http://localhost:5000/sx')

    rid = await sa.submit_query(good_submit, {'hi': 'there'})

    good_submit.post.assert_called_once()
    args, kwargs = good_submit.post.call_args

    assert len(args) == 1
    assert args[0] == 'http://localhost:5000/sx/servicex/transformation'

    assert len(kwargs) == 2
    assert 'headers' in kwargs
    assert len(kwargs['headers']) == 0

    assert 'json' in kwargs
    assert kwargs['json'] == {'hi': 'there'}

    assert rid is not None
    assert isinstance(rid, str)
    assert rid == '111-222-333-444'


@pytest.mark.asyncio
async def test_submit_good_with_auth(mocker):
    client = mocker.MagicMock()
    client.post = mocker.Mock(return_value=ClientSessionMocker([
        dumps({'access_token': "jwt:access"}),
        dumps({'request_id': "111-222-333-444"})], [200, 200]))

    sa = ServiceXAdaptor(endpoint='http://localhost:5000/sx',
                         refresh_token='jwt:refresh')

    await sa.submit_query(client, {'hi': 'there'})
    r = client.post.mock_calls

    assert len(r) == 2

    # Verify the login POST
    _, args, kwargs = r[0]
    assert args[0] == 'http://localhost:5000/sx/token/refresh'
    assert kwargs['headers']['Authorization'] == 'Bearer jwt:refresh'

    # Verify the Submit POST
    _, args, kwargs = r[1]
    assert args[0] == 'http://localhost:5000/sx/servicex/transformation'
    assert kwargs['headers']['Authorization'] == 'Bearer jwt:access'


@pytest.mark.asyncio
async def test_submit_good_with_auth_existing_token(mocker):
    client = mocker.MagicMock()
    client.post = mocker.Mock(return_value=ClientSessionMocker([
        dumps({'access_token': "jwt:access"}),
        dumps({'request_id': "111-222-333-444"}),
        dumps({'request_id': "222-333-444-555"})], [200, 200, 200]))

    sa = ServiceXAdaptor(endpoint='http://localhost:5000/sx',
                         refresh_token='jwt:refresh')

    mocker.patch('google.auth.jwt.decode', return_value={'exp': float('inf')})  # Never expires
    rid1 = await sa.submit_query(client, {'hi': 'there'})
    assert rid1 == '111-222-333-444'

    rid2 = await sa.submit_query(client, {'hi': 'there'})
    assert rid2 == '222-333-444-555'

    r = client.post.mock_calls

    assert len(r) == 3

    # Verify the access token POST
    _, args, kwargs = r[0]
    assert args[0] == 'http://localhost:5000/sx/token/refresh'
    assert kwargs['headers']['Authorization'] == 'Bearer jwt:refresh'

    # Verify the Submit POST
    _, args, kwargs = r[1]
    assert args[0] == 'http://localhost:5000/sx/servicex/transformation'
    assert kwargs['headers']['Authorization'] == 'Bearer jwt:access'

    # Verify the second Submit POST
    _, args, kwargs = r[2]
    assert args[0] == 'http://localhost:5000/sx/servicex/transformation'
    assert kwargs['headers']['Authorization'] == 'Bearer jwt:access'


@pytest.mark.asyncio
async def test_submit_good_with_auth_expired_token(mocker):
    client = mocker.MagicMock()
    client.post = mocker.Mock(return_value=ClientSessionMocker([
        dumps({'access_token': "jwt:access"}),
        dumps({'request_id': "111-222-333-444"}),
        dumps({'access_token': "jwt:access2"}),
        dumps({'request_id': "222-333-444-555"})], [200, 200, 200, 200]))

    sa = ServiceXAdaptor(endpoint='http://localhost:5000/sx',
                         refresh_token='jwt:refresh')

    mocker.patch('google.auth.jwt.decode', return_value={'exp': 0})  # Always expired

    rid1 = await sa.submit_query(client, {'hi': 'there'})
    assert rid1 == '111-222-333-444'

    rid2 = await sa.submit_query(client, {'hi': 'there'})
    assert rid2 == '222-333-444-555'

    r = client.post.mock_calls

    assert len(r) == 4

    # Verify the login POST
    _, args, kwargs = r[0]
    assert args[0] == 'http://localhost:5000/sx/token/refresh'
    assert kwargs['headers']['Authorization'] == 'Bearer jwt:refresh'

    # Verify the Submit POST
    _, args, kwargs = r[1]
    assert args[0] == 'http://localhost:5000/sx/servicex/transformation'
    assert kwargs['headers']['Authorization'] == 'Bearer jwt:access'

    # Verify the second login POST
    _, args, kwargs = r[2]
    assert args[0] == 'http://localhost:5000/sx/token/refresh'
    assert kwargs['headers']['Authorization'] == 'Bearer jwt:refresh'

    # Verify the second Submit POST
    _, args, kwargs = r[3]
    assert args[0] == 'http://localhost:5000/sx/servicex/transformation'
    assert kwargs['headers']['Authorization'] == 'Bearer jwt:access2'


@pytest.mark.asyncio
async def test_submit_bad(bad_submit):
    sa = ServiceXAdaptor(endpoint='http://localhost:5000/sx')

    with pytest.raises(ServiceXException) as e:
        await sa.submit_query(bad_submit, {'hi': 'there'})

    assert "bad text" in str(e.value)


@pytest.mark.asyncio
async def test_submit_bad_html(bad_submit_html):
    sa = ServiceXAdaptor(endpoint='http://localhost:5000/sx')

    with pytest.raises(ServiceXException) as e:
        await sa.submit_query(bad_submit_html, {'hi': 'there'})

    assert "html" in str(e.value)


@pytest.mark.asyncio
async def test_submit_good_with_bad_token(mocker):
    client = mocker.MagicMock()
    client.post = mocker.Mock(return_value=ClientSessionMocker(
        dumps({'message': 'Wrong credentials'}), 401))

    sa = ServiceXAdaptor(endpoint='http://localhost:5000/sx',
                         refresh_token="XXXXX")

    with pytest.raises(ServiceXException) as e:
        await sa.submit_query(client, {'hi': 'there'})

    assert "ServiceX access token request rejected" in str(e.value)


def test_servicex_adaptor_settings():
    from confuse import Configuration
    c = Configuration('bogus', 'bogus')
    c.clear()
    c['api_endpoint']['endpoint'] = 'http://my-left-foot.com:5000'
    c['api_endpoint']['token'] = 'forkingshirtballs.thegoodplace.bortles'

    sx = servicex_adaptor_factory(c)
    assert sx._endpoint == 'http://my-left-foot.com:5000'
    assert sx._refresh_token == 'forkingshirtballs.thegoodplace.bortles'
