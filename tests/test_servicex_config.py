import pytest

from servicex.ConfigSettings import ConfigSettings
from servicex.servicex_config import ServiceXConfigAdaptor
from servicex import ServiceXException


def test_default_ctor():
    x = ServiceXConfigAdaptor()
    assert isinstance(x.settings, ConfigSettings)


def test_passed_in_settings():
    c = ConfigSettings("servicex", "servicex")
    x = ServiceXConfigAdaptor(c)
    assert x.settings is c


def test_returned_datatype_nothing():
    c = ConfigSettings("servicex", "servicex")
    c.clear()
    x = ServiceXConfigAdaptor(c)
    with pytest.raises(ServiceXException):
        x.get_default_returned_datatype(None)


def test_returned_datatype_default():
    c = ConfigSettings("servicex", "servicex")
    c.clear()
    c["default_return_data"] = "root"
    c["api_endpoints"] = [{"endpoint": "http://bork", "type": "forkit"}]
    x = ServiceXConfigAdaptor(c)
    assert x.get_default_returned_datatype(None) == "root"


def test_returned_datatype_default_missing():
    c = ConfigSettings("servicex", "servicex")
    c.clear()
    c["api_endpoints"] = [{"endpoint": "http://bork", "type": "forkit"}]
    x = ServiceXConfigAdaptor(c)

    with pytest.raises(ServiceXException) as e:
        x.get_default_returned_datatype(None)

    assert "default_return_data" in str(e)


def test_returned_datatype_from_default_dict():
    c = ConfigSettings("servicex", "servicex")
    c.clear()
    c["api_endpoints"] = [{"endpoint": "http://bork", "type": "forkit"}]
    c["backend_types"] = [{"type": "forkit", "return_data": "spoon"}]
    x = ServiceXConfigAdaptor(c)
    assert x.get_default_returned_datatype("forkit") == "spoon"


def test_returned_datatype_from_endpoint():
    c = ConfigSettings("servicex", "servicex")
    c.clear()
    c["backend_types"] = [{"type": "forkit", "return_data": "spoon"}]
    c["api_endpoints"] = [{"type": "forkit", "return_data": "spoons"}]
    x = ServiceXConfigAdaptor(c)
    assert x.get_default_returned_datatype("forkit") == "spoons"


def test_backend_info():
    c = ConfigSettings("servicex", "servicex")
    c.clear()
    c["backend_types"] = [{"type": "forkit", "silly": "spoon"}]
    c["api_endpoints"] = [{"type": "forkit", "return_data": "spoons"}]

    x = ServiceXConfigAdaptor(c)
    assert x.get_backend_info("forkit", "silly") == "spoon"


def test_backend_info_by_name():
    c = ConfigSettings("servicex", "servicex")
    c.clear()
    c["backend_types"] = [{"type": "forkit", "silly": "spoon"}]
    c["api_endpoints"] = [{"name": "dude", "type": "forkit", "return_data": "spoons"}]

    x = ServiceXConfigAdaptor(c)
    assert x.get_backend_info("dude", "silly") == "spoon"


def test_backend_info_specified_type():
    c = ConfigSettings("servicex", "servicex")
    c.clear()
    c["backend_types"] = [{"type": "forkit", "silly": "spoon"}]
    c["api_endpoints"] = [{"name": "dude", "type": "leftfoot", "return_data": "spoons"}]

    x = ServiceXConfigAdaptor(c)
    assert x.get_backend_info("dude", "silly", backend_type="forkit") == "spoon"


def test_default_config_has_default_return_datatype():
    "Test default settings - default_returned_datatype"
    c = ConfigSettings("servicex", "servicex")
    assert c["default_return_data"].exists()


def test_default_config_has_backend_types():
    c = ConfigSettings("servicex", "servicex")
    assert c["backend_types"].exists()
    count = 0
    for info in c["backend_types"]:
        count += 1
        assert info["type"].exists()
        assert info["return_data"].exists()
    assert count > 0


def test_default_config_has_codegen():
    c = ConfigSettings("servicex", "servicex")
    assert c["backend_types"].exists()
    count = 0
    for info in c["backend_types"]:
        count += 1
        assert info["codegen"].exists()
    assert count > 0


def test_sx_adaptor_settings(caplog):
    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["api_endpoints"] = [
        {
            "type": "my-type",
            "endpoint": "http://my-left-foot.com:5000",
            "token": "forkingshirtballs.thegoodplace.bortles",
        }
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, token = x.get_servicex_adaptor_config("my-type")

    assert endpoint == "http://my-left-foot.com:5000"
    assert token == "forkingshirtballs.thegoodplace.bortles"

    assert len(caplog.record_tuples) == 1  # Depreciation warning


def test_sx_adaptor_settings_name(caplog):
    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["api_endpoints"] = [
        {
            "type": "my-type",
            "name": "my-fork",
            "endpoint": "http://my-left-foot.com:5000",
            "token": "forkingshirtballs.thegoodplace.bortles",
        }
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, token = x.get_servicex_adaptor_config("my-fork")

    assert endpoint == "http://my-left-foot.com:5000"
    assert token == "forkingshirtballs.thegoodplace.bortles"

    assert len(caplog.record_tuples) == 0


def test_sx_adaptor_settings_name_not_type(caplog):
    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["api_endpoints"] = [
        {
            "type": "my-type1",
            "name": "my-fork1",
            "endpoint": "http://my-left-foot1.com:5000",
            "token": "forkingshirtballs.thegoodplace.bortles",
        },
        {
            "type": "my-type2",
            "name": "my-type1",
            "endpoint": "http://my-left-foot2.com:5000",
            "token": "forkingshirtballs.thegoodplace.bortles",
        },
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, token = x.get_servicex_adaptor_config("my-type1")

    assert endpoint == "http://my-left-foot2.com:5000"
    assert token == "forkingshirtballs.thegoodplace.bortles"

    assert len(caplog.record_tuples) == 0


def test_sx_adaptor_settings_name_wrong(caplog):
    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["api_endpoints"] = [
        {
            "type": "my-type",
            "name": "my-fork",
            "endpoint": "http://my-left-foot.com:5000",
            "token": "forkingshirtballs.thegoodplace.bortles",
        }
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, token = x.get_servicex_adaptor_config("my-type")

    assert (
        len(caplog.record_tuples) == 1
    )  # Depreciation warning for using match by type


def test_sx_adaptor_settings_no_backend_name_requested(caplog):
    "Request None for a backend name"
    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["api_endpoints"] = [
        {
            "type": "my-type",
            "endpoint": "http://my-left-foot.com:5000",
            "token": "forkingshirtballs.thegoodplace.bortles",
        }
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, token = x.get_servicex_adaptor_config()

    assert endpoint == "http://my-left-foot.com:5000"
    assert token == "forkingshirtballs.thegoodplace.bortles"

    assert (
        caplog.record_tuples[0][2] == "No backend name/type requested, "
        "using http://my-left-foot.com:5000 - please be "
        "explicit "
        "in the ServiceXDataset constructor"
    )


def test_sx_adaptor_settings_no_backend_name_requested_or_listed(caplog):
    "Request None for a backend name"
    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["api_endpoints"] = [
        {
            "endpoint": "http://my-left-foot.com:5000",
            "token": "forkingshirtballs.thegoodplace.bortles",
        }
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, token = x.get_servicex_adaptor_config()

    assert endpoint == "http://my-left-foot.com:5000"
    assert token == "forkingshirtballs.thegoodplace.bortles"

    assert (
        caplog.record_tuples[0][2] == "No backend name/type requested, "
        "using http://my-left-foot.com:5000 - please be "
        "explicit "
        "in the ServiceXDataset constructor"
    )


def test_sx_adaptor_settings_backend_name_requested_with_unlabeled_type(caplog):
    "Request None for a backend name"
    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["api_endpoints"] = [
        {
            "endpoint": "http://my-left-foot.com:5000",
            "token": "forkingshirtballs.thegoodplace.bortles",
        }
    ]
    x = ServiceXConfigAdaptor(c)
    with pytest.raises(ServiceXException) as e:
        _ = x.get_servicex_adaptor_config("xaod")

    assert "Unable to find" in str(e)


def test_sx_adaptor_settings_backend_name_requested_after_labeled_type(caplog):
    "Request None for a backend name"
    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["api_endpoints"] = [
        {
            "endpoint": "http://my-left-foot.com:5000",
            "token": "forkingshirtballs.thegoodplace.bortles",
        },
        {
            "type": "xaod",
            "endpoint": "http://my-left-foot.com:5001",
            "token": "forkingshirtballs.thegoodplace.bortles1",
        },
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, token = x.get_servicex_adaptor_config("xaod")

    assert endpoint == "http://my-left-foot.com:5001"
    assert token == "forkingshirtballs.thegoodplace.bortles1"

    assert len(caplog.record_tuples) == 1  # Depreciation warning for using type


def test_sx_adaptor_settings_backend_name_use_first():
    "Request None for a backend name"
    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["api_endpoints"] = [
        {
            "type": "xaod",
            "endpoint": "http://my-left-foot.com:5000",
            "token": "forkingshirtballs.thegoodplace.bortles",
        },
        {
            "endpoint": "http://my-left-foot.com:5001",
            "token": "forkingshirtballs.thegoodplace.bortles1",
        },
    ]
    x = ServiceXConfigAdaptor(c)
    endpoint, token = x.get_servicex_adaptor_config()

    assert endpoint == "http://my-left-foot.com:5000"
    assert token == "forkingshirtballs.thegoodplace.bortles"


def test_sx_adaptor_settings_wrong_type():
    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["api_endpoints"] = [
        {
            "type": "my-type",
            "endpoint": "http://my-left-foot.com:5000",
            "token": "forkingshirtballs.thegoodplace.bortles",
        }
    ]

    x = ServiceXConfigAdaptor(c)
    with pytest.raises(ServiceXException) as e:
        x.get_servicex_adaptor_config("your-type")

    assert "Unable to find name/type" in str(e.value)
    assert "my-type" in str(e.value)


def test_sx_adaptor_settings_env():
    from confuse import Configuration

    c = Configuration("bogus", "bogus")
    c.clear()
    c["api_endpoints"] = [
        {
            "type": "${SXTYPE}",
            "endpoint": "${ENDPOINT}:5000",
            "token": "${SXTOKEN}",
        }
    ]

    from os import environ

    environ["ENDPOINT"] = "http://tachi.com"
    environ["SXTYPE"] = "mcrn"
    environ["SXTOKEN"] = "protomolecule"

    x = ServiceXConfigAdaptor(c)
    endpoint, token = x.get_servicex_adaptor_config("mcrn")

    assert endpoint == "http://tachi.com:5000"
    assert token == "protomolecule"


def test_default_config_endpoint():
    c = ConfigSettings("servicex", "servicex")
    c.clear()
    c._add_default_source()
    x = ServiceXConfigAdaptor(c)

    end_point, token = x.get_servicex_adaptor_config()
    assert end_point == "http://localhost:5000"
    assert token is None


def test_sx_adaptor_nothing():
    c = ConfigSettings("servicex", "servicex")
    c.clear()
    x = ServiceXConfigAdaptor(c)

    with pytest.raises(ServiceXException):
        x.get_servicex_adaptor_config()
