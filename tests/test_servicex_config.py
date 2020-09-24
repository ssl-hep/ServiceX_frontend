
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
