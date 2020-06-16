from servicex.data_conversions import _convert_root_to_pandas, _convert_root_to_awkward
import pytest
import pandas as pd

from .utils_for_testing import good_root_file_path  # NOQA


@pytest.mark.asyncio
async def test_root_to_pandas(good_root_file_path):
    df = await _convert_root_to_pandas(good_root_file_path)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 283458


@pytest.mark.asyncio
async def test_root_to_awkward(good_root_file_path):
    df = await _convert_root_to_awkward(good_root_file_path)
    assert isinstance(df, dict)
    assert len(df) == 1
    assert b'JetPt' in df
    assert len(df[b'JetPt']) == 283458
