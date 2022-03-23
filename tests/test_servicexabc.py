from servicex.servicexabc import ServiceXABC
import pytest


def test_load():
    with pytest.raises(TypeError):
        ServiceXABC("localds://bogus")  # type: ignore
