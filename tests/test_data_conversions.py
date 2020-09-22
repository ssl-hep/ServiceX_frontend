from servicex.data_conversions import DataConverterAdaptor
import pytest
import pandas as pd


@pytest.mark.asyncio
async def test_root_to_pandas(good_root_file_path):
    df = await DataConverterAdaptor().convert_to_pandas(good_root_file_path, 'root')
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 283458


@pytest.mark.asyncio
async def test_root_to_awkward(good_root_file_path):
    df = await DataConverterAdaptor().convert_to_awkward(good_root_file_path, 'root')
    assert len(df['JetPt']) == 283458
