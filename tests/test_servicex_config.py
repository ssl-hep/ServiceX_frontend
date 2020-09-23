
import pytest

from servicex.ConfigSettings import ConfigSettings
from servicex.servicex_config import ServiceXConfigAdaptor
from servicex.utils import ServiceXException


def test_default_ctor():
    x = ServiceXConfigAdaptor()
    assert isinstance(x.settings, ConfigSettings)


def test_passed_in_settings():
    c = ConfigSettings('servicex', 'servicex')
    x = ServiceXConfigAdaptor(c)
    assert x.settings is c


def test_returned_datatype_nothing():
    c = ConfigSettings('servicex', 'servicex')
    c.clear()
    x = ServiceXConfigAdaptor(c)
    with pytest.raises(ServiceXException):
        x.get_default_returned_datatype(None)


def test_returned_datatype_default():
    c = ConfigSettings('servicex', 'servicex')
    c.clear()
    c['default_return_data'] = 'root'
    x = ServiceXConfigAdaptor(c)
    assert x.get_default_returned_datatype(None) == 'root'


def test_returned_datatype_from_default_dict():
    c = ConfigSettings('servicex', 'servicex')
    c.clear()
    c['backend_types'] = [{'type': 'forkit', 'return_data': 'spoon'}]
    x = ServiceXConfigAdaptor(c)
    assert x.get_default_returned_datatype('forkit') == 'spoon'


def test_returned_datatype_from_endpoint():
    c = ConfigSettings('servicex', 'servicex')
    c.clear()
    c['backend_types'] = [{'type': 'forkit', 'return_data': 'spoon'}]
    c['api_endpoints'] = [{'type': 'forkit', 'return_data': 'spoons'}]
    x = ServiceXConfigAdaptor(c)
    assert x.get_default_returned_datatype('forkit') == 'spoons'


def test_defalt_config_has_default_return_datatype():
    'Test default settings - default_returned_datatype'
    c = ConfigSettings('servicex', 'servicex')
    assert c['default_return_data'].exists()


def test_defalt_config_has_backend_types():
    c = ConfigSettings('servicex', 'servicex')
    assert c['backend_types'].exists()
    count = 0
    for info in c['backend_types']:
        count += 1
        assert info['type'].exists()
        assert info['return_data'].exists()
    assert count > 0


def test_sx_adaptor_settings():
    from confuse import Configuration
    c = Configuration('bogus', 'bogus')
    c.clear()
    c['api_endpoints'] = [
        {
            'type': 'my-type',
            'endpoint': 'http://my-left-foot.com:5000',
            'email': 'thegoodplace@example.com',
            'password': 'forkingshirtballs',
        }
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, email, password = x.get_servicex_adaptor_config('my-type')

    assert endpoint == 'http://my-left-foot.com:5000'
    assert email == 'thegoodplace@example.com'
    assert password == 'forkingshirtballs'


def test_sx_adaptor_settings_no_backend_name_requested():
    'Request None for a backend name'
    from confuse import Configuration
    c = Configuration('bogus', 'bogus')
    c.clear()
    c['api_endpoints'] = [
        {
            'type': 'my-type',
            'endpoint': 'http://my-left-foot.com:5000',
            'email': 'thegoodplace@example.com',
            'password': 'forkingshirtballs',
        }
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, email, password = x.get_servicex_adaptor_config()

    assert endpoint == 'http://my-left-foot.com:5000'
    assert email == 'thegoodplace@example.com'
    assert password == 'forkingshirtballs'


def test_sx_adaptor_settings_no_backend_name_requested_or_listed():
    'Request None for a backend name'
    from confuse import Configuration
    c = Configuration('bogus', 'bogus')
    c.clear()
    c['api_endpoints'] = [
        {
            'endpoint': 'http://my-left-foot.com:5000',
            'email': 'thegoodplace@example.com',
            'password': 'forkingshirtballs',
        }
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, email, password = x.get_servicex_adaptor_config()

    assert endpoint == 'http://my-left-foot.com:5000'
    assert email == 'thegoodplace@example.com'
    assert password == 'forkingshirtballs'


def test_sx_adaptor_settings_backend_name_requested_with_unlabeled_type():
    'Request None for a backend name'
    from confuse import Configuration
    c = Configuration('bogus', 'bogus')
    c.clear()
    c['api_endpoints'] = [
        {
            'endpoint': 'http://my-left-foot.com:5000',
            'email': 'thegoodplace@example.com',
            'password': 'forkingshirtballs',
        }
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, email, password = x.get_servicex_adaptor_config('xaod')

    assert endpoint == 'http://my-left-foot.com:5000'
    assert email == 'thegoodplace@example.com'
    assert password == 'forkingshirtballs'


def test_sx_adaptor_settings_backend_name_requested_after_labeled_type():
    'Request None for a backend name'
    from confuse import Configuration
    c = Configuration('bogus', 'bogus')
    c.clear()
    c['api_endpoints'] = [
        {
            'endpoint': 'http://my-left-foot.com:5000',
            'email': 'thegoodplace@example.com',
            'password': 'forkingshirtballs',
        },
        {
            'type': 'xaod',
            'endpoint': 'http://my-left-foot.com:5001',
            'email': 'thegoodplace1@example.com',
            'password': 'forkingshirtballs1',
        }
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, email, password = x.get_servicex_adaptor_config('xaod')

    assert endpoint == 'http://my-left-foot.com:5001'
    assert email == 'thegoodplace1@example.com'
    assert password == 'forkingshirtballs1'


def test_sx_adaptor_settings_backend_name_unlabeled_type():
    'Request None for a backend name'
    from confuse import Configuration
    c = Configuration('bogus', 'bogus')
    c.clear()
    c['api_endpoints'] = [
        {
            'type': 'xaod',
            'endpoint': 'http://my-left-foot.com:5000',
            'email': 'thegoodplace@example.com',
            'password': 'forkingshirtballs',
        },
        {
            'endpoint': 'http://my-left-foot.com:5001',
            'email': 'thegoodplace1@example.com',
            'password': 'forkingshirtballs1',
        }
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, email, password = x.get_servicex_adaptor_config()

    assert endpoint == 'http://my-left-foot.com:5001'
    assert email == 'thegoodplace1@example.com'
    assert password == 'forkingshirtballs1'


def test_sx_adaptor_settings_wrong_type():
    from confuse import Configuration
    c = Configuration('bogus', 'bogus')
    c.clear()
    c['api_endpoints'] = [
        {
            'type': 'my-type',
            'endpoint': 'http://my-left-foot.com:5000',
            'email': 'thegoodplace@example.com',
            'password': 'forkingshirtballs',
        }
    ]

    x = ServiceXConfigAdaptor(c)
    with pytest.raises(ServiceXException) as e:
        x.get_servicex_adaptor_config('your-type')

    assert 'Unable to find type' in str(e.value)
    assert 'my-type' in str(e.value)


def test_sx_adaptor_settings_env():
    from confuse import Configuration
    c = Configuration('bogus', 'bogus')
    c.clear()
    c['api_endpoints'] = [
        {
            'type': '${SXTYPE}',
            'endpoint': '${ENDPOINT}:5000',
            'email': '${SXUSER}',
            'password': '${SXPASS}',
        }
    ]

    from os import environ
    environ['ENDPOINT'] = 'http://tachi.com'
    environ['SXUSER'] = 'Holden'
    environ['SXPASS'] = 'protomolecule'
    environ['SXTYPE'] = 'mcrn'

    x = ServiceXConfigAdaptor(c)
    endpoint, email, password = x.get_servicex_adaptor_config('mcrn')

    assert endpoint == 'http://tachi.com:5000'
    assert email == 'Holden'
    assert password == 'protomolecule'

# def test_default_config_endpoint():
#     c = ConfigSettings('servicex', 'servicex')
#     x = ServiceXConfigAdaptor(c)
#     end_point, email, password = x.get_servicex_adaptor_config()
#     assert end_point == 'http://localhost:5000'
#     assert email is None
#     assert password is None
