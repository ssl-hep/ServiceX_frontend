from servicex.servicex_client import GuardList
import pytest


def test_guardlist():
    gl1 = GuardList([1])
    assert str(gl1) == '[1]'
    assert gl1[0] == 1
    gl2 = GuardList(ValueError())
    assert str(gl2) == 'Invalid GuardList: ValueError()'
    with pytest.raises(ValueError):
        gl2[0]
    with pytest.raises(ValueError):
        len(gl2)
